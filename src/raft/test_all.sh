./dstest.py \
TestInitialElection2A TestReElection2A TestManyElections2A \
TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B \
TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B \
TestBackup2B TestCount2B \
TestPersist12C TestPersist22C TestPersist32C \
TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C \
internalChurn TestReliableChurn2C TestUnreliableChurn2C \
TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D \
TestSnapshotInstallUnCrash2D TestSnapshotAllCrash2D TestSnapshotInit2D \
-p 50 \
-n 5000 \
-o ./saved_all_logs \
-R \
-v
