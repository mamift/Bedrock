#include "../BedrockClusterTester.h"

struct MultipleLeaderSyncTest : tpunit::TestFixture {
    MultipleLeaderSyncTest()
        : tpunit::TestFixture("MultipleLeaderSyncTest",
                              BEFORE_CLASS(MultipleLeaderSyncTest::setup),
                              AFTER_CLASS(MultipleLeaderSyncTest::teardown),
                              TEST(MultipleLeaderSyncTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        // create a 5 node cluster
        tester = new BedrockClusterTester(BedrockClusterTester::FIVE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"}, _threadID);

        // make sure the whole cluster is up
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForStates({"LEADING", "MASTERING"}));
        for (int i = 1; i <= 4; i++) {
            ASSERT_TRUE(tester->getBedrockTester(i)->waitForStates({"FOLLOWING", "SLAVING"}));
        }

        // shut down primary leader
        tester->stopNode(0);

        // Wait for node 1 to be leader.
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForStates({"LEADING", "MASTERING"}));

        // increase the delta between primary and secondary leader
        runTrivialWrites(5000, 4);

        // shut down secondary leader
        tester->stopNode(1);

        // Wait for node 2 to be leader.
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForStates({"LEADING", "MASTERING"}));

        // give secondary leader a few commits to sync
        runTrivialWrites(5000, 4);

        // FYI: Because of the way getCommitCount() works, This only works once.
        // Check for the correct number of commits
        sleep(10);
        ASSERT_TRUE(tester->getBedrockTester(4)->getCommitCount() >= 10000);

        // just a check for ready state
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForStates({"LEADING", "MASTERING"}));
        ASSERT_TRUE(tester->getBedrockTester(3)->waitForStates({"FOLLOWING", "SLAVING"}));
        ASSERT_TRUE(tester->getBedrockTester(4)->waitForStates({"FOLLOWING", "SLAVING"}));
    }

    void teardown() {
        delete tester;
    }

    void runTrivialWrites(int writeCount, int nodeID = 0) {
        // Create a bunch of trivial write commands.
        int count = 0;
        while (count <= writeCount) {
            SData request;
            request.methodLine = "Query";
            if (count == 0) {
                request["query"] = "INSERT OR REPLACE INTO test (id, value) VALUES(12345, 1 );";
            } else {
                request["query"] = "UPDATE test SET value=value + 1 WHERE id=12345;";
            }
            request["connection"] = "forget";
            tester->getBedrockTester(nodeID)->executeWaitVerifyContent(request, "202");
            count++;
        }

    }

    void test() {
        // Bring leaders back up in reverse order, should go quickly to SYNCHRONIZING
        tester->startNodeDontWait(1);
        tester->startNodeDontWait(0);
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForStates({"SYNCHRONIZING"}, 10'000'000, true ));
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForStates({"SYNCHRONIZING"}, 10'000'000, true ));

        // tertiary leader should still be MASTERING for a while
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForStates({"LEADING", "MASTERING"}, 5'000'000 ));

        // secondary leader should catch up first and go MASTERING, wait up to 30s
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForStates({"LEADING", "MASTERING"}, 30'000'000 ));

        // when primary leader catches up it should go MASTERING, wait up to 30s
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForStates({"LEADING", "MASTERING"}, 30'000'000 ));
    }

} __MultipleLeaderSyncTest;
