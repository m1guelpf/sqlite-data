#if canImport(CloudKit)
  import Clocks
  import CloudKit
  import CustomDump
  import DependenciesTestSupport
  import Foundation
  import SQLiteDataTestSupport
  import InlineSnapshotTesting
  import OrderedCollections
  import SQLiteData
  import SnapshotTestingCustomDump
  import Testing

  @Table struct Category: Identifiable {
    let id: Int
    let name: String
  }

  @Table struct TodoItem: Identifiable {
    let id: Int
    let title: String
    let categoryID: Category.ID
    let parentTodoID: TodoItem.ID?
  }

  @Table struct UUIDUser: Identifiable {
    let id: UUID
    let name: String
    let parentID: UUID?
  }

  @Table struct UserActivity: Identifiable {
    let id: Int
    let userID: LocalUser.ID
    let action: String
  }

  @Suite(
    .snapshots(record: .missing),
    .dependencies {
      $0.currentTime.now = 0
      $0.continuousClock = TestClock<Duration>()
      $0.dataManager = InMemoryDataManager()
    },
    .attachMetadatabase(false)
  )
  class SelfReferentialTableTests: @unchecked Sendable {
    let userDatabase: UserDatabase
    private let _syncEngine: any Sendable
    private let _container: any Sendable

    @Dependency(\.continuousClock) var clock
    @Dependency(\.currentTime.now) var now
    @Dependency(\.dataManager) var dataManager

    var container: MockCloudContainer {
      _container as! MockCloudContainer
    }

    var syncEngine: SyncEngine {
      _syncEngine as! SyncEngine
    }

    init() async throws {
      let testContainerIdentifier = "iCloud.co.pointfree.Testing.\(UUID())"

      self.userDatabase = UserDatabase(
        database: try SQLiteDataTests.database(
          containerIdentifier: testContainerIdentifier,
          attachMetadatabase: false
        )
      )
      let privateDatabase = MockCloudDatabase(databaseScope: .private)
      let sharedDatabase = MockCloudDatabase(databaseScope: .shared)
      let container = MockCloudContainer(
        accountStatus: .available,
        containerIdentifier: testContainerIdentifier,
        privateCloudDatabase: privateDatabase,
        sharedCloudDatabase: sharedDatabase
      )
      _container = container
      privateDatabase.set(container: container)
      sharedDatabase.set(container: container)
      _syncEngine = try await SyncEngine(
        container: container,
        userDatabase: self.userDatabase,
        tables: LocalUser.self, Reminder.self, RemindersList.self,
        startImmediately: true
      )
      let currentUserRecordID = CKRecord.ID(recordName: "currentUser")
      await syncEngine.handleEvent(
        .accountChange(changeType: .signIn(currentUser: currentUserRecordID)),
        syncEngine: syncEngine.private
      )
      await syncEngine.handleEvent(
        .accountChange(changeType: .signIn(currentUser: currentUserRecordID)),
        syncEngine: syncEngine.shared
      )
      try await syncEngine.processPendingDatabaseChanges(scope: .private)
    }

    deinit {
      if #available(iOS 17, macOS 14, tvOS 17, watchOS 10, *) {
        guard syncEngine.isRunning
        else { return }

        syncEngine.shared.assertFetchChangesScopes([])
        syncEngine.shared.state.assertPendingDatabaseChanges([])
        syncEngine.shared.state.assertPendingRecordZoneChanges([])
        syncEngine.shared.assertAcceptedShareMetadata([])
        syncEngine.private.assertFetchChangesScopes([])
        syncEngine.private.state.assertPendingDatabaseChanges([])
        syncEngine.private.state.assertPendingRecordZoneChanges([])
        syncEngine.private.assertAcceptedShareMetadata([])

        try! syncEngine.metadatabase.read { db in
          try #expect(UnsyncedRecordID.count().fetchOne(db) == 0)
        }
      }
    }

    @Test func basicSelfReferentialHierarchy() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root User", parentID: nil)
            LocalUser(id: 2, name: "Child User", parentID: 1)
            LocalUser(id: 3, name: "Grandchild User", parentID: 2)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        assertQuery(LocalUser.order(by: \.id), database: userDatabase.database) {
          """
          ┌────────────────────────────┐
          │ LocalUser(                 │
          │   id: 1,                   │
          │   name: "Root User",       │
          │   parentID: nil            │
          │ )                          │
          ├────────────────────────────┤
          │ LocalUser(                 │
          │   id: 2,                   │
          │   name: "Child User",      │
          │   parentID: 1              │
          │ )                          │
          ├────────────────────────────┤
          │ LocalUser(                 │
          │   id: 3,                   │
          │   name: "Grandchild User", │
          │   parentID: 2              │
          │ )                          │
          └────────────────────────────┘
          """
        }

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: [
                [0]: CKRecord(
                  recordID: CKRecord.ID(1:localUsers/zone/__defaultOwner__),
                  recordType: "localUsers",
                  parent: nil,
                  share: nil,
                  id: 1,
                  name: "Root User"
                ),
                [1]: CKRecord(
                  recordID: CKRecord.ID(2:localUsers/zone/__defaultOwner__),
                  recordType: "localUsers",
                  parent: CKReference(recordID: CKRecord.ID(1:localUsers/zone/__defaultOwner__)),
                  share: nil,
                  id: 2,
                  name: "Child User",
                  parentID: 1
                ),
                [2]: CKRecord(
                  recordID: CKRecord.ID(3:localUsers/zone/__defaultOwner__),
                  recordType: "localUsers",
                  parent: CKReference(recordID: CKRecord.ID(2:localUsers/zone/__defaultOwner__)),
                  share: nil,
                  id: 3,
                  name: "Grandchild User",
                  parentID: 2
                )
              ]
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }

      @Test func receiveMultipleChildrenBeforeParentInSameBatch() async throws {
        let parentRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 1)
        )
        parentRecord.setValue(1, forKey: "id", at: now)
        parentRecord.setValue("Parent User", forKey: "name", at: now)

        let childRecords = (2...11).map { index in
          let childRecord = CKRecord(
            recordType: LocalUser.tableName,
            recordID: LocalUser.recordID(for: index)
          )
          childRecord.setValue(index, forKey: "id", at: now)
          childRecord.setValue("Child User \(index)", forKey: "name", at: now)
          childRecord.setValue(1, forKey: "parentID", at: now)
          childRecord.parent = CKRecord.Reference(
            record: parentRecord,
            action: .none
          )
          return childRecord
        }

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: childRecords + [parentRecord]
        ).notify()

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 11)

          let parent = try LocalUser.find(1).fetchOne(db)
          let child2 = try LocalUser.find(2).fetchOne(db)
          let child11 = try LocalUser.find(11).fetchOne(db)

          #expect(parent?.parentID == nil)
          #expect(child2?.parentID == 1)
          #expect(child11?.parentID == 1)
        }
      }

      @Test func receiveChildBeforeParentInSelfReferentialTable() async throws {
        let rootUserRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 1)
        )
        rootUserRecord.setValue(1, forKey: "id", at: now)
        rootUserRecord.setValue("Root User", forKey: "name", at: now)

        let childUserRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 2)
        )
        childUserRecord.setValue(2, forKey: "id", at: now)
        childUserRecord.setValue("Child User", forKey: "name", at: now)
        childUserRecord.setValue(1, forKey: "parentID", at: now)
        childUserRecord.parent = CKRecord.Reference(
          record: rootUserRecord,
          action: .none
        )

        let grandchildUserRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 3)
        )
        grandchildUserRecord.setValue(3, forKey: "id", at: now)
        grandchildUserRecord.setValue("Grandchild User", forKey: "name", at: now)
        grandchildUserRecord.setValue(2, forKey: "parentID", at: now)
        grandchildUserRecord.parent = CKRecord.Reference(
          record: childUserRecord,
          action: .none
        )

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [childUserRecord, grandchildUserRecord, rootUserRecord]
        ).notify()

        assertQuery(LocalUser.order(by: \.id), database: userDatabase.database) {
          """
          ┌────────────────────────────┐
          │ LocalUser(                 │
          │   id: 1,                   │
          │   name: "Root User",       │
          │   parentID: nil            │
          │ )                          │
          ├────────────────────────────┤
          │ LocalUser(                 │
          │   id: 2,                   │
          │   name: "Child User",      │
          │   parentID: 1              │
          │ )                          │
          ├────────────────────────────┤
          │ LocalUser(                 │
          │   id: 3,                   │
          │   name: "Grandchild User", │
          │   parentID: 2              │
          │ )                          │
          └────────────────────────────┘
          """
        }
      }

      @Test func deepHierarchy() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Level 1", parentID: nil)
            LocalUser(id: 2, name: "Level 2", parentID: 1)
            LocalUser(id: 3, name: "Level 3", parentID: 2)
            LocalUser(id: 4, name: "Level 4", parentID: 3)
            LocalUser(id: 5, name: "Level 5", parentID: 4)
            LocalUser(id: 6, name: "Level 6", parentID: 5)
            LocalUser(id: 7, name: "Level 7", parentID: 6)
            LocalUser(id: 8, name: "Level 8", parentID: 7)
            LocalUser(id: 9, name: "Level 9", parentID: 8)
            LocalUser(id: 10, name: "Level 10", parentID: 9)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 10)
        }

        let level1 = try syncEngine.private.database.record(for: LocalUser.recordID(for: 1))
        let level2 = try syncEngine.private.database.record(for: LocalUser.recordID(for: 2))
        let level5 = try syncEngine.private.database.record(for: LocalUser.recordID(for: 5))
        let level10 = try syncEngine.private.database.record(for: LocalUser.recordID(for: 10))

        #expect(level1.parent == nil)
        #expect(level2.parent?.recordID == LocalUser.recordID(for: 1))
        #expect(level5.parent?.recordID == LocalUser.recordID(for: 4))
        #expect(level10.parent?.recordID == LocalUser.recordID(for: 9))
      }

      @Test func cascadingDeleteInSelfReferentialTable() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root", parentID: nil)
            LocalUser(id: 2, name: "Child 1", parentID: 1)
            LocalUser(id: 3, name: "Grandchild", parentID: 2)
            LocalUser(id: 4, name: "Child 2", parentID: 1)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 4)
        }

        try await userDatabase.userWrite { db in
          try LocalUser.find(1).delete().execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        assertQuery(LocalUser.all, database: userDatabase.database) {
          """
          (No results)
          """
        }

        assertInlineSnapshot(of: container, as: .customDump) {
          """
          MockCloudContainer(
            privateCloudDatabase: MockCloudDatabase(
              databaseScope: .private,
              storage: []
            ),
            sharedCloudDatabase: MockCloudDatabase(
              databaseScope: .shared,
              storage: []
            )
          )
          """
        }
      }

      @Test func moveUserToNewParent() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Parent A", parentID: nil)
            LocalUser(id: 2, name: "Child of A", parentID: 1)
            LocalUser(id: 3, name: "Parent B", parentID: nil)
            LocalUser(id: 4, name: "Child of B", parentID: 3)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).update { $0.parentID = 3 }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let user = try LocalUser.find(2).fetchOne(db)
          #expect(user?.parentID == 3)
        }

        let user2Record = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 2)
        )
        #expect(user2Record.parent?.recordID == LocalUser.recordID(for: 3))
      }

      @Test func simultaneousUpdatesInHierarchy() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root", parentID: nil)
            LocalUser(id: 2, name: "Branch A", parentID: 1)
            LocalUser(id: 3, name: "Leaf A", parentID: 2)
            LocalUser(id: 4, name: "Branch B", parentID: 1)
            LocalUser(id: 5, name: "Leaf B", parentID: 4)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).update { $0.name = "Updated Branch A" }.execute(db)
          try LocalUser.find(4).update { $0.name = "Updated Branch B" }.execute(db)
          try LocalUser.find(5).update { $0.name = "Updated Leaf B" }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let branchA = try LocalUser.find(2).fetchOne(db)
          let branchB = try LocalUser.find(4).fetchOne(db)
          let leafB = try LocalUser.find(5).fetchOne(db)

          #expect(branchA?.name == "Updated Branch A")
          #expect(branchB?.name == "Updated Branch B")
          #expect(leafB?.name == "Updated Leaf B")
        }
      }

      @Test func conflictLocalMoveWhileRemoteDeletesParent() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Parent A", parentID: nil)
            LocalUser(id: 2, name: "Child", parentID: 1)
            LocalUser(id: 3, name: "Parent B", parentID: nil)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).update { $0.parentID = 3 }.execute(db)
        }

        try await syncEngine.modifyRecords(
          scope: .private,
          deleting: [LocalUser.recordID(for: 3)]
        ).notify()

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let child = try LocalUser.find(2).fetchOne(db)
          #expect(child == nil)

          let parentA = try LocalUser.find(1).fetchOne(db)
          #expect(parentA?.name == "Parent A")
        }
      }

      @Test func cascadingDeleteMiddleNode() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root", parentID: nil)
            LocalUser(id: 2, name: "Middle", parentID: 1)
            LocalUser(id: 3, name: "Leaf", parentID: 2)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).delete().execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 1)
          let root = try LocalUser.find(1).fetchOne(db)
          #expect(root?.name == "Root")
        }

        let zone = syncEngine.private.database.state.storage[syncEngine.defaultZone.zoneID]
        let localUserRecords = zone?.records.values.filter { $0.recordType == "localUsers" } ?? []
        #expect(localUserRecords.count == 1)
        #expect(localUserRecords.first?.recordID == LocalUser.recordID(for: 1))
      }

      @Test func mixedTableUpdates() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "User 1", parentID: nil)
            LocalUser(id: 2, name: "User 2", parentID: 1)
          }
        }

        try await userDatabase.userWrite { db in
          try db.seed {
            RemindersList(id: 1, title: "List 1")
            Reminder(id: 1, title: "Reminder 1", remindersListID: 1)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).update { $0.name = "Updated User 2" }.execute(db)
          try Reminder.find(1).update { $0.title = "Updated Reminder 1" }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let user = try LocalUser.find(2).fetchOne(db)
          let reminder = try Reminder.find(1).fetchOne(db)

          #expect(user?.name == "Updated User 2")
          #expect(reminder?.title == "Updated Reminder 1")
        }

        let userRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 2)
        )
        let reminderRecord = try syncEngine.private.database.record(
          for: Reminder.recordID(for: 1)
        )

        #expect(userRecord.recordType == "localUsers")
        #expect(reminderRecord.recordType == "reminders")
      }

      @Test func conflictResolutionInHierarchies() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Parent A", parentID: nil)
            LocalUser(id: 2, name: "Child", parentID: 1)
            LocalUser(id: 3, name: "Parent B", parentID: nil)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        let remoteRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 2)
        )
        remoteRecord.setValue(2, forKey: "id", at: now + 1)
        remoteRecord.setValue("Remote Name", forKey: "name", at: now + 1)
        remoteRecord.setValue(3, forKey: "parentID", at: now + 1)
        remoteRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 3),
          action: .none
        )

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [remoteRecord]
        ).notify()

        try await userDatabase.userWrite { db in
          try LocalUser.find(2).update {
            $0.name = "Local Name"
            $0.parentID = 1
          }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let child = try LocalUser.find(2).fetchOne(db)
          #expect(child?.name == "Local Name")
          #expect(child?.parentID == 1)
        }

        let cloudKitRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 2)
        )
        #expect(cloudKitRecord.parent?.recordID == LocalUser.recordID(for: 1))
      }

      @Test func receivingCompleteHierarchyFromCloudKit() async throws {
        let user1Record = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 1)
        )
        user1Record.setValue(1, forKey: "id", at: now)
        user1Record.setValue("Level 1", forKey: "name", at: now)

        let user2Record = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 2)
        )
        user2Record.setValue(2, forKey: "id", at: now)
        user2Record.setValue("Level 2", forKey: "name", at: now)
        user2Record.setValue(1, forKey: "parentID", at: now)
        user2Record.parent = CKRecord.Reference(
          record: user1Record,
          action: .none
        )

        let user3Record = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 3)
        )
        user3Record.setValue(3, forKey: "id", at: now)
        user3Record.setValue("Level 3", forKey: "name", at: now)
        user3Record.setValue(2, forKey: "parentID", at: now)
        user3Record.parent = CKRecord.Reference(
          record: user2Record,
          action: .none
        )

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [user1Record, user2Record, user3Record]
        ).notify()

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 3)

          let level1 = try LocalUser.find(1).fetchOne(db)
          let level2 = try LocalUser.find(2).fetchOne(db)
          let level3 = try LocalUser.find(3).fetchOne(db)

          #expect(level1?.name == "Level 1")
          #expect(level1?.parentID == nil)
          #expect(level2?.name == "Level 2")
          #expect(level2?.parentID == 1)
          #expect(level3?.name == "Level 3")
          #expect(level3?.parentID == 2)
        }
      }

      @Test func receivingRemoteMoveOperation() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Parent A", parentID: nil)
            LocalUser(id: 2, name: "Child", parentID: 1)
            LocalUser(id: 3, name: "Parent B", parentID: nil)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        let movedChildRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 2)
        )
        movedChildRecord.setValue(3, forKey: "parentID", at: now + 1)
        movedChildRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 3),
          action: .none
        )

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [movedChildRecord]
        ).notify()

        try await userDatabase.read { db in
          let child = try LocalUser.find(2).fetchOne(db)
          #expect(child?.parentID == 3)
        }

        let cloudKitRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 2)
        )
        #expect(cloudKitRecord.parent?.recordID == LocalUser.recordID(for: 3))
      }

      @Test func cloudKitReferenceViolationOnParentDelete_SelfReferential() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Parent", parentID: nil)
            LocalUser(id: 2, name: "Child", parentID: 1)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        let (_, deleteResults) = try syncEngine.private.database.modifyRecords(
          deleting: [LocalUser.recordID(for: 1)]
        )

        let deleteResult = deleteResults[LocalUser.recordID(for: 1)]
        switch deleteResult {
        case .failure(let error as CKError):
          #expect(error.code == .referenceViolation)
        case .success, .failure, .none:
          Issue.record("Expected reference violation error for self-referential table")
        }
      }

      @Test func cloudKitReferenceViolationOnParentDelete_CrossTable() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            RemindersList(id: 1, title: "My List")
            Reminder(id: 1, title: "My Reminder", remindersListID: 1)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        let (_, deleteResults) = try syncEngine.private.database.modifyRecords(
          deleting: [RemindersList.recordID(for: 1)]
        )

        let deleteResult = deleteResults[RemindersList.recordID(for: 1)]
        switch deleteResult {
        case .failure(let error as CKError):
          #expect(error.code == .referenceViolation)
        case .success, .failure, .none:
          Issue.record("Expected reference violation error for cross-table reference")
        }
      }

      @Test func receivingRemoteCascadingDelete() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root", parentID: nil)
            LocalUser(id: 2, name: "Parent", parentID: 1)
            LocalUser(id: 3, name: "Child", parentID: 2)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        let zone = syncEngine.private.database.state.storage[syncEngine.defaultZone.zoneID]
        let recordsBefore = zone?.records.values.filter { $0.recordType == "localUsers" } ?? []
        #expect(recordsBefore.count == 3)

        try await syncEngine.modifyRecords(
          scope: .private,
          deleting: [LocalUser.recordID(for: 3), LocalUser.recordID(for: 2)]
        ).notify()

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 1)
          let root = try LocalUser.find(1).fetchOne(db)
          #expect(root?.name == "Root")

          let parent = try LocalUser.find(2).fetchOne(db)
          let child = try LocalUser.find(3).fetchOne(db)
          #expect(parent == nil)
          #expect(child == nil)
        }

        let zoneAfter = syncEngine.private.database.state.storage[syncEngine.defaultZone.zoneID]
        let recordsAfter = zoneAfter?.records.values.filter { $0.recordType == "localUsers" } ?? []
        #expect(recordsAfter.count == 1)
        #expect(recordsAfter.first?.recordID == LocalUser.recordID(for: 1))
      }

      @Test func deferredConstraintsShouldNotBlockRetry() async throws {

        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Existing Parent", parentID: nil)
          }
        }
        try await syncEngine.processPendingRecordZoneChanges(scope: .private)


        let validChildRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 2)
        )
        validChildRecord.setValue(2, forKey: "id", at: now)
        validChildRecord.setValue("Valid Child", forKey: "name", at: now)
        validChildRecord.setValue(1, forKey: "parentID", at: now)
        validChildRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 1),
          action: .none
        )

        let orphanChildRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 3)
        )
        orphanChildRecord.setValue(3, forKey: "id", at: now)
        orphanChildRecord.setValue("Orphan Child", forKey: "name", at: now)
        orphanChildRecord.setValue(999, forKey: "parentID", at: now)
        orphanChildRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 999),
          action: .none
        )

        let unrelatedRecord = CKRecord(
          recordType: RemindersList.tableName,
          recordID: RemindersList.recordID(for: 1)
        )
        unrelatedRecord.setValue(1, forKey: "id", at: now)
        unrelatedRecord.setValue("Shopping", forKey: "title", at: now)

        container.privateCloudDatabase.state.withValue { state in
          let zoneID = validChildRecord.recordID.zoneID
          state.storage[zoneID]?.records[validChildRecord.recordID] = validChildRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[orphanChildRecord.recordID] = orphanChildRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[unrelatedRecord.recordID] = unrelatedRecord.copy() as? CKRecord
        }

        await syncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: [validChildRecord, orphanChildRecord, unrelatedRecord],
            deletions: []
          ),
          syncEngine: syncEngine.private
        )

        try await userDatabase.read { db in
          let validChild = try LocalUser.find(2).fetchOne(db)
          #expect(validChild?.name == "Valid Child")

          let orphanChild = try LocalUser.find(3).fetchOne(db)
          #expect(orphanChild == nil)

          let list = try RemindersList.find(1).fetchOne(db)
          #expect(list?.title == "Shopping")
        }

        try await syncEngine.metadatabase.read { db in
          let unsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "3:localUsers" }
            .fetchCount(db)
          #expect(unsyncedCount == 1)
        }

        let missingParentRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 999)
        )
        missingParentRecord.setValue(999, forKey: "id", at: now + 1)
        missingParentRecord.setValue("Missing Parent", forKey: "name", at: now + 1)

        container.privateCloudDatabase.state.withValue { state in
          let zoneID = missingParentRecord.recordID.zoneID
          state.storage[zoneID]?.records[missingParentRecord.recordID] = missingParentRecord.copy() as? CKRecord
        }

        await syncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: [missingParentRecord],
            deletions: []
          ),
          syncEngine: syncEngine.private
        )

        try await userDatabase.read { db in
          let orphanChild = try LocalUser.find(3).fetchOne(db)
          #expect(orphanChild?.name == "Orphan Child")
          #expect(orphanChild?.parentID == 999)
        }

        try await syncEngine.metadatabase.read { db in
          let unsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "3:localUsers" }
            .fetchCount(db)
          #expect(unsyncedCount == 0)
        }
      }

      @Test func deferredFKRetryDoesNotBlockValidChildBeforeParent() async throws {


        let orphanRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 100)
        )
        orphanRecord.setValue(100, forKey: "id", at: now)
        orphanRecord.setValue("Orphan", forKey: "name", at: now)
        orphanRecord.setValue(999, forKey: "parentID", at: now)
        orphanRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 999),
          action: .none
        )

        let validChildRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 5)
        )
        validChildRecord.setValue(5, forKey: "id", at: now)
        validChildRecord.setValue("Valid Child", forKey: "name", at: now)
        validChildRecord.setValue(4, forKey: "parentID", at: now)
        validChildRecord.parent = CKRecord.Reference(
          recordID: LocalUser.recordID(for: 4),
          action: .none
        )

        let validParentRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 4)
        )
        validParentRecord.setValue(4, forKey: "id", at: now)
        validParentRecord.setValue("Valid Parent", forKey: "name", at: now)

        container.privateCloudDatabase.state.withValue { state in
          let zoneID = orphanRecord.recordID.zoneID
          state.storage[zoneID]?.records[orphanRecord.recordID] = orphanRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[validChildRecord.recordID] = validChildRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[validParentRecord.recordID] = validParentRecord.copy() as? CKRecord
        }

        await syncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: [orphanRecord, validChildRecord, validParentRecord],
            deletions: []
          ),
          syncEngine: syncEngine.private
        )

        try await userDatabase.read { db in
          let validParent = try LocalUser.find(4).fetchOne(db)
          #expect(validParent?.name == "Valid Parent")

          let validChild = try LocalUser.find(5).fetchOne(db)
          #expect(validChild?.name == "Valid Child")
          #expect(validChild?.parentID == 4)
        }

        try await syncEngine.metadatabase.read { db in
          let orphanUnsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "100:localUsers" }
            .fetchCount(db)
          #expect(orphanUnsyncedCount == 1)

          let validChildUnsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "5:localUsers" }
            .fetchCount(db)
          #expect(validChildUnsyncedCount == 0)
        }

        let missingParentRecord = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: 999)
        )
        missingParentRecord.setValue(999, forKey: "id", at: now + 1)
        missingParentRecord.setValue("Missing Parent", forKey: "name", at: now + 1)

        container.privateCloudDatabase.state.withValue { state in
          let zoneID = missingParentRecord.recordID.zoneID
          state.storage[zoneID]?.records[missingParentRecord.recordID] = missingParentRecord.copy() as? CKRecord
        }

        await syncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: [missingParentRecord],
            deletions: []
          ),
          syncEngine: syncEngine.private
        )

        try await userDatabase.read { db in
          let orphan = try LocalUser.find(100).fetchOne(db)
          #expect(orphan?.name == "Orphan")
        }
      }

      @Test func multiFKTableWithNoActionCrashesOnReferenceViolation() async throws {

        let database = try DatabaseQueue()
        try await database.write { db in
          try #sql(
            """
            CREATE TABLE "categories" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
              "name" TEXT NOT NULL
            ) STRICT
            """
          )
          .execute(db)

          try #sql(
            """
            CREATE TABLE "todoItems" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
              "title" TEXT NOT NULL,
              "categoryID" INTEGER NOT NULL REFERENCES "categories"("id") ON DELETE CASCADE,
              "parentTodoID" INTEGER REFERENCES "todoItems"("id") ON DELETE NO ACTION
            ) STRICT
            """
          )
          .execute(db)
        }

        await #expect(throws: (any Error).self) {
          try await SyncEngine(
            container: MockCloudContainer(
              containerIdentifier: "deadbeef",
              privateCloudDatabase: MockCloudDatabase(databaseScope: .private),
              sharedCloudDatabase: MockCloudDatabase(databaseScope: .shared)
            ),
            userDatabase: UserDatabase(database: database),
            tables: Category.self, TodoItem.self
          )
        }
      }

      @Test func privateUUIDSelfReferentialRetryPreservesParentOrdering() async throws {

        let testContainerIdentifier = "iCloud.co.pointfree.Testing.\(UUID())"
        let database = try SQLiteDataTests.database(
          containerIdentifier: testContainerIdentifier,
          attachMetadatabase: false
        )

        try await database.write { db in
          try #sql(
            """
            CREATE TABLE "uuidUsers" (
              "id" TEXT PRIMARY KEY NOT NULL,
              "name" TEXT NOT NULL ON CONFLICT REPLACE DEFAULT '',
              "parentID" TEXT REFERENCES "uuidUsers"("id") ON DELETE CASCADE
            ) STRICT
            """
          )
          .execute(db)
        }

        let privateDatabase = MockCloudDatabase(databaseScope: .private)
        let sharedDatabase = MockCloudDatabase(databaseScope: .shared)
        let container = MockCloudContainer(
          accountStatus: .available,
          containerIdentifier: testContainerIdentifier,
          privateCloudDatabase: privateDatabase,
          sharedCloudDatabase: sharedDatabase
        )
        privateDatabase.set(container: container)
        sharedDatabase.set(container: container)

        let privateSyncEngine = try await SyncEngine(
          container: container,
          userDatabase: UserDatabase(database: database),
          tables: UUIDUser.self,
          privateTables: UUIDUser.self,
          startImmediately: true
        )

        let currentUserRecordID = CKRecord.ID(recordName: "currentUser")
        await privateSyncEngine.handleEvent(
          .accountChange(changeType: .signIn(currentUser: currentUserRecordID)),
          syncEngine: privateSyncEngine.private
        )


        let orphanID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let validChildID = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
        let validParentID = UUID(uuidString: "00000000-0000-0000-0000-000000000003")!
        let orphanParentID = UUID(uuidString: "99999999-9999-9999-9999-999999999999")!

        let orphanRecord = CKRecord(
          recordType: "uuidUsers",
          recordID: CKRecord.ID(
            recordName: "\(orphanID.uuidString):uuidUsers",
            zoneID: privateSyncEngine.defaultZone.zoneID
          )
        )
        orphanRecord.setValue(orphanID.uuidString, forKey: "id", at: now)
        orphanRecord.setValue("Orphan Child", forKey: "name", at: now)
        orphanRecord.setValue(orphanParentID.uuidString, forKey: "parentID", at: now)

        let validChildRecord = CKRecord(
          recordType: "uuidUsers",
          recordID: CKRecord.ID(
            recordName: "\(validChildID.uuidString):uuidUsers",
            zoneID: privateSyncEngine.defaultZone.zoneID
          )
        )
        validChildRecord.setValue(validChildID.uuidString, forKey: "id", at: now)
        validChildRecord.setValue("Valid Child", forKey: "name", at: now)
        validChildRecord.setValue(validParentID.uuidString, forKey: "parentID", at: now)

        let validParentRecord = CKRecord(
          recordType: "uuidUsers",
          recordID: CKRecord.ID(
            recordName: "\(validParentID.uuidString):uuidUsers",
            zoneID: privateSyncEngine.defaultZone.zoneID
          )
        )
        validParentRecord.setValue(validParentID.uuidString, forKey: "id", at: now)
        validParentRecord.setValue("Valid Parent", forKey: "name", at: now)

        container.privateCloudDatabase.state.withValue { state in
          let zoneID = orphanRecord.recordID.zoneID
          state.storage[zoneID]?.records[orphanRecord.recordID] = orphanRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[validChildRecord.recordID] = validChildRecord.copy() as? CKRecord
          state.storage[zoneID]?.records[validParentRecord.recordID] = validParentRecord.copy() as? CKRecord
        }

        await privateSyncEngine.handleEvent(
          .fetchedRecordZoneChanges(
            modifications: [orphanRecord, validChildRecord, validParentRecord],
            deletions: []
          ),
          syncEngine: privateSyncEngine.private
        )

        let users = try await database.read { db in
          try UUIDUser.order(by: \.id).fetchAll(db)
        }

        #expect(users.count == 2)
        let sortedUsers = users.sorted { $0.id.uuidString < $1.id.uuidString }

        let parent = sortedUsers.first { $0.id == validParentID }
        let child = sortedUsers.first { $0.id == validChildID }

        #expect(parent != nil)
        #expect(parent?.name == "Valid Parent")
        #expect(parent?.parentID == nil)

        #expect(child != nil)
        #expect(child?.name == "Valid Child")
        #expect(child?.parentID == validParentID)

        try await privateSyncEngine.metadatabase.read { db in
          let orphanUnsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "\(orphanID.uuidString):uuidUsers" }
            .fetchCount(db)
          #expect(orphanUnsyncedCount == 1)

          let validChildUnsyncedCount = try UnsyncedRecordID
            .where { $0.recordName == "\(validChildID.uuidString):uuidUsers" }
            .fetchCount(db)
          #expect(validChildUnsyncedCount == 0)
        }
      }

      @Test func rootToChildAndChildToRoot() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Original Root", parentID: nil)
            LocalUser(id: 2, name: "Parent", parentID: nil)
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.userWrite { db in
          try LocalUser.find(1).update { $0.parentID = 2 }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let user = try LocalUser.find(1).fetchOne(db)
          #expect(user?.parentID == 2)
        }

        let userRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 1)
        )
        #expect(userRecord.parent?.recordID == LocalUser.recordID(for: 2))

        try await userDatabase.userWrite { db in
          try LocalUser.find(1).update { $0.parentID = nil }.execute(db)
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          let user = try LocalUser.find(1).fetchOne(db)
          #expect(user?.parentID == nil)
        }

        let updatedRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 1)
        )
        #expect(updatedRecord.parent == nil)
      }

      @Test func wideTree() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 1, name: "Root", parentID: nil)
          }
          for i in 2...101 {
            try db.seed {
              LocalUser(id: i, name: "Child \(i)", parentID: 1)
            }
          }
        }

        try await syncEngine.processPendingRecordZoneChanges(scope: .private)

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 101)

          let children = try LocalUser.where { $0.id != 1 }.fetchAll(db)
          for child in children {
            #expect(child.parentID == 1)
          }
        }

        let rootRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 1)
        )
        #expect(rootRecord.parent == nil)

        let child50 = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 50)
        )
        #expect(child50.parent?.recordID == LocalUser.recordID(for: 1))
      }

      @Test func branchingTreesInOneBatch() async throws {
        let records = [
          makeUserRecord(id: 4, name: "GrandchildA", parentID: 2),
          makeUserRecord(id: 5, name: "GrandchildB", parentID: 2),
          makeUserRecord(id: 7, name: "GrandchildC", parentID: 3),
          makeUserRecord(id: 2, name: "Child1", parentID: 1),
          makeUserRecord(id: 3, name: "Child2", parentID: 1),
          makeUserRecord(id: 1, name: "Root", parentID: nil),
          makeUserRecord(id: 6, name: "GrandchildD", parentID: 3),
        ]

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: records
        ).notify()

        try await userDatabase.read { db in
          try #expect(LocalUser.fetchCount(db) == 7)

          let root = try LocalUser.find(1).fetchOne(db)
          let child1 = try LocalUser.find(2).fetchOne(db)
          let child2 = try LocalUser.find(3).fetchOne(db)
          let gc4 = try LocalUser.find(4).fetchOne(db)
          let gc5 = try LocalUser.find(5).fetchOne(db)
          let gc6 = try LocalUser.find(6).fetchOne(db)
          let gc7 = try LocalUser.find(7).fetchOne(db)

          #expect(root?.parentID == nil)
          #expect(child1?.parentID == 1)
          #expect(child2?.parentID == 1)
          #expect(gc4?.parentID == 2)
          #expect(gc5?.parentID == 2)
          #expect(gc6?.parentID == 3)
          #expect(gc7?.parentID == 3)
        }

        let rootRecord = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 1)
        )
        #expect(rootRecord.parent == nil)

        let gc4Record = try syncEngine.private.database.record(
          for: LocalUser.recordID(for: 4)
        )
        #expect(gc4Record.parent?.recordID == LocalUser.recordID(for: 2))
      }

      private func makeUserRecord(id: Int, name: String, parentID: Int?) -> CKRecord {
        let record = CKRecord(
          recordType: LocalUser.tableName,
          recordID: LocalUser.recordID(for: id)
        )
        record.setValue(id, forKey: "id", at: now)
        record.setValue(name, forKey: "name", at: now)
        if let parentID = parentID {
          record.setValue(parentID, forKey: "parentID", at: now)
          record.parent = CKRecord.Reference(
            recordID: LocalUser.recordID(for: parentID),
            action: .none
          )
        }
        return record
      }

      @Test func selfReferentialRecordsSentInCorrectOrder() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 2, name: "Child", parentID: 1)
            LocalUser(id: 1, name: "Parent", parentID: nil)
          }
        }

        let batch = await syncEngine.nextRecordZoneChangeBatch(
          syncEngine: syncEngine.private
        )
        let records = batch?.recordsToSave ?? []

        let parentIndex = records.firstIndex { $0.recordID.recordName == "1:localUsers" }
        let childIndex = records.firstIndex { $0.recordID.recordName == "2:localUsers" }
        #expect(parentIndex != nil)
        #expect(childIndex != nil)
        #expect(parentIndex! < childIndex!)
      }

      @Test func deepHierarchySentInCorrectOrder() async throws {
        try await userDatabase.userWrite { db in
          try db.seed {
            LocalUser(id: 3, name: "Grandchild", parentID: 2)
            LocalUser(id: 1, name: "Grandparent", parentID: nil)
            LocalUser(id: 2, name: "Parent", parentID: 1)
          }
        }

        let batch = await syncEngine.nextRecordZoneChangeBatch(
          syncEngine: syncEngine.private
        )
        let records = batch?.recordsToSave ?? []

        let grandparentIndex = records.firstIndex { $0.recordID.recordName == "1:localUsers" }
        let parentIndex = records.firstIndex { $0.recordID.recordName == "2:localUsers" }
        let grandchildIndex = records.firstIndex { $0.recordID.recordName == "3:localUsers" }

        #expect(grandparentIndex != nil)
        #expect(parentIndex != nil)
        #expect(grandchildIndex != nil)
        #expect(grandparentIndex! < parentIndex!)
        #expect(parentIndex! < grandchildIndex!)
      }

      @Test func customTriggerReferencingParentWorksWithCloudKitSync() async throws {
        try await userDatabase.write { db in
          try #sql("""
            CREATE TABLE IF NOT EXISTS "userActivities" (
              "id" INTEGER PRIMARY KEY NOT NULL,
              "userID" INTEGER NOT NULL REFERENCES "localUsers"("id"),
              "action" TEXT NOT NULL
            )
            """).execute(db)

          try #sql("""
            CREATE TRIGGER IF NOT EXISTS "log_user_with_parent"
            AFTER INSERT ON "localUsers"
            WHEN NEW."parentID" IS NOT NULL
            BEGIN
              INSERT INTO "userActivities" ("id", "userID", "action")
              VALUES (NEW."id", NEW."parentID", 'child_created');
            END
            """).execute(db)
        }

        let childRecord = makeUserRecord(id: 2, name: "Child", parentID: 1)
        let parentRecord = makeUserRecord(id: 1, name: "Parent", parentID: nil)

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [childRecord, parentRecord]
        ).notify()

        try await userDatabase.read { db in
          let users = try LocalUser.order(by: \.id).fetchAll(db)
          #expect(users.count == 2)
          #expect(users[0].id == 1)
          #expect(users[1].id == 2)

          let activities = try #sql("SELECT * FROM userActivities", as: UserActivity.self)
            .fetchAll(db)
          #expect(activities.count == 1)
          #expect(activities[0].userID == 1)
          #expect(activities[0].action == "child_created")
        }
      }

      @Test func customTriggerWorksWithDeepHierarchyFromCloudKit() async throws {
        try await userDatabase.write { db in
          try #sql("""
            CREATE TABLE IF NOT EXISTS "userActivities" (
              "id" INTEGER PRIMARY KEY NOT NULL,
              "userID" INTEGER NOT NULL REFERENCES "localUsers"("id"),
              "action" TEXT NOT NULL
            )
            """).execute(db)

          try #sql("""
            CREATE TRIGGER IF NOT EXISTS "log_user_with_parent"
            AFTER INSERT ON "localUsers"
            WHEN NEW."parentID" IS NOT NULL
            BEGIN
              INSERT INTO "userActivities" ("id", "userID", "action")
              VALUES (NEW."id", NEW."parentID", 'child_created');
            END
            """).execute(db)
        }

        let grandchildRecord = makeUserRecord(id: 3, name: "Grandchild", parentID: 2)
        let parentRecord = makeUserRecord(id: 2, name: "Parent", parentID: 1)
        let grandparentRecord = makeUserRecord(id: 1, name: "Grandparent", parentID: nil)

        try await syncEngine.modifyRecords(
          scope: .private,
          saving: [grandchildRecord, parentRecord, grandparentRecord]
        ).notify()

        try await userDatabase.read { db in
          let users = try LocalUser.order(by: \.id).fetchAll(db)
          #expect(users.count == 3)

          let activities = try #sql("SELECT * FROM userActivities ORDER BY id", as: UserActivity.self)
            .fetchAll(db)
          #expect(activities.count == 2)
          #expect(activities[0].id == 2)
          #expect(activities[0].userID == 1)
          #expect(activities[1].id == 3)
          #expect(activities[1].userID == 2)
        }
      }
  }
#endif
