#include "../BedrockClusterTester.h"
#include <fstream>

struct FutureExecutionTest : tpunit::TestFixture {
    FutureExecutionTest()
        : tpunit::TestFixture("FutureExecution",
                              BEFORE_CLASS(FutureExecutionTest::setup),
                              AFTER_CLASS(FutureExecutionTest::teardown),
                              TEST(FutureExecutionTest::FutureExecution)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }

    void FutureExecution() {
        // We only care about leader because future execution only works on leader.
        BedrockTester* brtester = tester->getBedrockTester(0);

        // Let's run a command in the future.
        SData query("Query");

        // Three seconds from now.
        query["commandExecuteTime"] = to_string(STimeNow() + 3000000);
        query["Query"] = "INSERT INTO test VALUES(" + SQ(50011) + ", " + SQ("sent_by_leader") + ");";
        string result = brtester->executeWaitVerifyContent(query, "202"); 

        // Ok, Now let's wait a second
        sleep(1);

        // And make it still hasn't been inserted.
        query.clear();
        query.methodLine = "Query";
        query["Query"] = "SELECT * FROM test WHERE id = 50011;";
        result = brtester->executeWaitVerifyContent(query);
        ASSERT_FALSE(SContains(result, "50011"));

        // Then sleep three more seconds, it *should* be there now.
        sleep(3);

        // And now it should be there, but we'll give it a couple tries.
        int retries = 3;
        bool success = false;
        while (retries) {
            result = brtester->executeWaitVerifyContent(query);
            if (SContains(result, "50011")) {
                success = true;
                break;
            } else {
                sleep(1);
                retries--;
            }
        }
        ASSERT_TRUE(success);
    }

} __FutureExecutionTest;
