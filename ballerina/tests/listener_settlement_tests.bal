// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Settlement by the handler's return, the delivery limit, the order of checks, message
// visibility, and the poison queue (spec 5.4, 5.8, 5.9, 5.10, 5.11).

import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;

// ---- settlement by return (spec 5.9)

@test:Config {}
function testHandlerErrorRedeliversThenPoisonsVerbatim() returns error? {
    EventSource src = check setupEventSource("lsn-poison");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("bad", "bad.bin");
    json event = blobCreatedEvent(src.container, "bad.bin");
    check enqueueEvent(src.queue, event);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 3} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("attempt");
            if recorder.count("attempt") == 2 {
                panic error("handler panic");
            }
            return error("handler failure");
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("attempt") >= 3);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("attempt"), 3, "the message is delivered maxDeliveryCount times");
    test:assertEquals(recorder.count("error"), 0, "handler errors do not notify onError");
    check await(() => approximateMessageCount(src.queue) == 0);
    PeekedMessage[] poisoned = check peekDetails(poisonQueue(src.queue));
    test:assertEquals(poisoned.length(), 1, "the message moved to the auto-created poison queue");
    test:assertEquals(poisoned[0].text, encoded(event), "the poison queue holds the original message body");
    test:assertEquals(poisoned[0].dequeueCount, 0, "the dequeue count starts again in the poison queue");
}

@test:Config {}
function testDeadLetterErrorPoisonsAtOnceOnlyWhenReturnedAsIs() returns error? {
    EventSource src = check setupEventSource("lsn-deadletter");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "direct.bin");
    check srcBlobs->upload("x", "wrapped.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "direct.bin"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "wrapped.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event)
                returns error? {
            recorder.add("attempts", event.path);
            if event.path == "direct.bin" {
                return error DeadLetterError("give up");
            }
            // A DeadLetterError carried as a cause is an ordinary failure.
            error wrapped = error("outer", error DeadLetterError("inner"));
            return wrapped;
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(src.queue) == 0 && approximateMessageCount(poisonQueue(queue)) == 2);
    check lsn.gracefulStop();
    string[] attempts = recorder.list("attempts");
    test:assertEquals(attempts.filter(p => p == "direct.bin").length(), 1, "a DeadLetterError ends after one delivery");
    test:assertEquals(attempts.filter(p => p == "wrapped.bin").length(), 2, "a wrapped one is retried to the limit");
    test:assertEquals(recorder.count("error"), 0);
}

@test:Config {}
function testRetryErrorRedeliversAfterItsDelayAndCountsTowardTheLimit() returns error? {
    EventSource src = check setupEventSource("lsn-retry");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "retry.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "retry.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.add("times", time:utcNow()[0].toString());
            // Fractional delays round up to whole seconds; the final delivery's request is ignored.
            return error RetryError("later", delay = 2.5);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    string[] times = recorder.list("times");
    test:assertEquals(times.length(), 2, "the RetryError delivery counts; the final one moves to the poison queue");
    int gap = check int:fromString(times[1]) - check int:fromString(times[0]);
    test:assertTrue(gap >= 3, string `redelivered after the rounded-up delay, not before (gap ${gap}s)`);
    test:assertEquals(check approximateMessageCount(src.queue), 0);
}

@test:Config {}
function testRedeliveryDelayAppliesToOrdinaryFailures() returns error? {
    EventSource src = check setupEventSource("lsn-redelivery-delay");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "delayed.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "delayed.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2, redeliveryDelay: 2}
    isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.add("times", time:utcNow()[0].toString());
            return error("not yet");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("times") >= 2);
    check lsn.gracefulStop();
    string[] times = recorder.list("times");
    int gap = check int:fromString(times[1]) - check int:fromString(times[0]);
    test:assertTrue(gap >= 2, string `the second delivery waited redeliveryDelay (gap ${gap}s)`);
}

@test:Config {}
function testRedeliveryThatWouldOutliveTheMessagePoisons() returns error? {
    EventSource src = check setupEventSource("lsn-would-expire");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "short.bin");
    check srcBlobs->upload("x", "long.bin");
    // Thirty seconds to live: a 15-second delay plus the hold window reaches past the expiry;
    // an immediate redelivery does not.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "short.bin"), ttlSeconds = 30);
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "long.bin"), ttlSeconds = 30);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 5} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add(event.path, "attempt");
            if event.path == "short.bin" {
                return error RetryError("busy", delay = 15);
            }
            return recorder.count("long.bin") == 1 ? error("once") : ();
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1 && recorder.count("long.bin") >= 2);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("short.bin"), 1, "a delay that would outlive the message is not applied");
    test:assertEquals(recorder.count("long.bin"), 2, "an immediate redelivery within the life is applied");
    check await(() => approximateMessageCount(src.queue) == 0);
}

@test:Config {}
function testSettlementErrorsApplyToOnBlobDelete() returns error? {
    EventSource src = check setupEventSource("lsn-deleted-settlement");
    check enqueueEvent(src.queue, blobDeletedEvent(src.container, "a.txt"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobDelete(BlobEvent event) returns DeadLetterError|error? {
            recorder.hit("deleted");
            return error DeadLetterError("no record of " + event.path);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("deleted"), 1);
}

// ---- concurrency (spec 5.3, 5.9)

@test:Config {}
function testIsolatedServiceRunsHandlersInParallelUpToTheBound() returns error? {
    EventSource src = check setupEventSource("lsn-parallel");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "p.bin");
    foreach int i in 0 ..< 8 {
        check enqueueEvent(src.queue, numberedCreatedEvent(src.container, "p.bin", i));
    }
    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, batchSize: 4, newBatchThreshold: 2} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            _ = recorder.enter();
            runtime:sleep(1);
            recorder.exit();
            recorder.hit("done");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("done") >= 8);
    check lsn.gracefulStop();
    int peak = recorder.maxConcurrent();
    test:assertTrue(peak >= 2 && peak <= 6, string `handlers overlapped within batchSize + newBatchThreshold (${peak})`);
}

@test:Config {}
function testNonIsolatedServiceStillReceivesTheBatch() returns error? {
    EventSource src = check setupEventSource("lsn-nonisolated");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "n.bin");
    foreach int i in 0 ..< 6 {
        check enqueueEvent(src.queue, numberedCreatedEvent(src.container, "n.bin", i));
    }
    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    // The one deliberately non-isolated service of the suite: it is served, in turn.
    Service svc = @ServiceConfig {queueName: queue, batchSize: 3} service object {
        remote function onBlob(byte[] content) returns error? {
            runtime:sleep(0.2);
            recorder.hit("done");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("done") >= 6);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("done"), 6);
    check await(() => approximateMessageCount(src.queue) == 0);
}

// ---- visibility (spec 5.4)

@test:Config {}
function testSlowHandlerKeepsItsMessageAcrossRenewals() returns error? {
    EventSource src = check setupEventSource("lsn-renewal");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "slow.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxPollingInterval: 1} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("attempt");
            // Longer than the hold: only renewal keeps the message from reappearing.
            runtime:sleep(TEST_HOLD + 8);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("attempt") >= 1);
    runtime:sleep(TEST_HOLD + 10);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("attempt"), 1, "the message was never redelivered while its handler ran");
    check await(() => approximateMessageCount(src.queue) == 0);
    test:assertFalse(check queueExists(poisonQueue(src.queue)));
}

@test:Config {}
function testRenewalStopsAtTheExpiryMargin() returns error? {
    EventSource src = check setupEventSource("lsn-expiry");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "expiring.bin");
    // The message lives 25 seconds and its handler runs 30: renewal is clamped to the margin,
    // stops when nothing remains, and the message expires rather than being released early.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "expiring.bin"), ttlSeconds = 25);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxPollingInterval: 1} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("attempt");
            runtime:sleep(30);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("finished") >= 1, timeoutSeconds = 45);
    check lsn.gracefulStop();
    // The message expired under its handler: it was neither poisoned nor left behind. Within
    // the margin it is visible again, so a second delivery may have run; both settle quietly.
    test:assertTrue(recorder.count("attempt") >= 1 && recorder.count("attempt") <= 2,
            string `deliveries: ${recorder.count("attempt")}`);
    // The emulator's approximate count keeps an expired message until a receive purges it, so
    // what is left is checked by peeking: nothing visible, and nothing poisoned.
    check await(() => (check peekMessages(src.queue)).length() == 0, timeoutSeconds = 45);
    test:assertFalse(check queueExists(poisonQueue(src.queue)));
}

// ---- the order of checks and the delivery limit (spec 5.8)

@test:Config {}
function testMessagePastTheLimitIsPoisonedOnReceipt() returns error? {
    EventSource src = check setupEventSource("lsn-past-limit");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "tired.bin");
    json tired = blobCreatedEvent(src.container, "tired.bin");
    check enqueueEvent(src.queue, tired);
    // Three unsettled deliveries before the listener sees it: its fourth is past a limit of 3.
    check driveDequeueCount(src.queue, 3);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 3} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    runtime:sleep(1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0, "no dispatch past the limit");
    string[] errors = recorder.list("errors");
    test:assertEquals(errors.length(), 1, "one notification, before the move");
    test:assertTrue(errors[0].startsWith("Error|tired.bin|caller|delivery limit exceeded on receipt"), errors[0]);
    PeekedMessage[] poisoned = check peekDetails(poisonQueue(queue));
    test:assertEquals(poisoned[0].text, encoded(tired), "moved verbatim");
    test:assertEquals(poisoned[0].dequeueCount, 0);
}

@test:Config {}
function testPastTheLimitNotificationCarriesNoEventWhenNotOurs() returns error? {
    EventSource src = check setupEventSource("lsn-past-limit-notours");
    check enqueueMessage(src.queue, "not an event");
    check enqueueEvent(src.queue, blobTierChangedEvent(src.container, "cold.bin"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "foreign.bin", topic = storageTopic("other")));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "nocontent.bin", api = "CreateFile"));
    check driveDequeueCount(src.queue, 2);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 1} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller));
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 4);
    runtime:sleep(1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    test:assertEquals(recorder.list("errors"), ["Error|()|()", "Error|()|()", "Error|()|()", "Error|()|()"],
            "one notification each, with neither event nor Caller, and no separate parse notification");
}

@test:Config {}
function testUnparseableMessagesArePoisonedAtOnce() returns error? {
    EventSource src = check setupEventSource("lsn-unparsable");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "a.bin");
    check enqueueMessage(src.queue, "not an event");
    // A blob event without its storage account: a missing topic, and an Event Grid custom
    // topic relay, which stamps its own topic.
    map<json> noTopic = <map<json>>blobCreatedEvent(src.container, "a.bin");
    _ = noTopic.remove("topic");
    check enqueueEvent(src.queue, noTopic);
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "a.bin",
            topic = "/subscriptions/x/resourceGroups/rg/providers/Microsoft.EventGrid/topics/relay"));
    // A CloudEvents relay that keeps the original src parses normally.
    check enqueueEvent(src.queue, blobCreatedCloudEvent(src.container, "a.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 5} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("dispatched", event.api);
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 3 && recorder.count("dispatched") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.list("dispatched"), ["PutBlockList"]);
    string[] errors = recorder.list("errors");
    test:assertEquals(errors.length(), 3, "each unparseable message notifies once and moves at once");
    foreach string entry in errors {
        test:assertTrue(entry.startsWith("Error|()|()|"), entry);
        test:assertTrue(entry.includes(src.queue), "the error names the queue: " + entry);
    }
    test:assertEquals(check approximateMessageCount(src.queue), 0);
}

// ---- the poison queue (spec 5.10)

@test:Config {}
function testCustomPoisonQueueName() returns error? {
    EventSource src = check setupEventSource("lsn-custom-poison");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "x.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "x.bin"));
    final string dead = testQueue("lsn-custom-dead");

    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, poisonQueueName: dead} isolated service object {
        isolated remote function onBlob(byte[] content) returns DeadLetterError? {
            return error DeadLetterError("park it");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => check queueExists(dead) && check approximateMessageCount(dead) == 1);
    check lsn.gracefulStop();
    test:assertFalse(check queueExists(poisonQueue(src.queue)), "the default poison queue is not created");
}

@test:Config {}
function testFailedPoisonMoveNotifiesAndLeavesTheMessageHidden() returns error? {
    EventSource src = check setupEventSource("lsn-move-fails");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "stuck.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "stuck.bin"));
    // Without add and create, the copy cannot be sent to the poison queue nor the queue created.
    string sas = check mintAccountSas({read: true, process: true, update: true, delete: true, write: true});

    final Recorder recorder = new;
    Listener lsn = check newListenerWith(accountSasAuth(sas));
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns DeadLetterError? {
            recorder.hit("blob");
            return error DeadLetterError("park it");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("errors") >= 1);
    runtime:sleep(2);
    check lsn.gracefulStop();
    string[] errors = recorder.list("errors");
    test:assertEquals(errors.length(), 1);
    test:assertTrue(errors[0].startsWith("Error|stuck.bin|caller|"), "the failed move carries the event and Caller");
    test:assertTrue(errors[0].includes(poisonQueue(src.queue)), "the error names the poison queue: " + errors[0]);
    test:assertEquals(recorder.count("blob"), 1, "the message stays hidden rather than being redelivered");
    test:assertEquals(check approximateMessageCount(src.queue), 1);
    test:assertFalse(check queueExists(poisonQueue(src.queue)));
}

@test:Config {}
function testFailedMovePastTheLimitNotifiesTwicePerReceipt() returns error? {
    EventSource src = check setupEventSource("lsn-move-fails-twice");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "tired.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "tired.bin"));
    check driveDequeueCount(src.queue, 2);
    string sas = check mintAccountSas({read: true, process: true, update: true, delete: true});

    final Recorder recorder = new;
    Listener lsn = check newListenerWith(accountSasAuth(sas));
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 1} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("errors") >= 2);
    runtime:sleep(1);
    check lsn.gracefulStop();
    string[] errors = recorder.list("errors");
    test:assertEquals(errors.length(), 2, "the limit notification, then the failed move's");
    test:assertTrue(errors[0].startsWith("Error|tired.bin|caller|delivery limit exceeded on receipt"), errors[0]);
    test:assertTrue(errors[1].startsWith("Error|tired.bin|caller|") && errors[1].includes(poisonQueue(queue)), errors[1]);
    test:assertEquals(recorder.count("blob"), 0);
}
