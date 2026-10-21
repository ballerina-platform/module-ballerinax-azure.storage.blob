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

// The listener's construction, the service rules applied at attach, and the lifecycle
// (spec 5.2 and 5.3). The services are anonymous service objects, so the compiler plugin leaves
// them alone and the tests control the shapes directly.

import ballerina/lang.runtime;
import ballerina/test;

// ---- init: local validation only (spec 2.1, 5.2)

@test:Config {}
function testListenerInitValidation() {
    SharedKeyConfig auth = sharedKeyAuth();
    test:assertTrue(new Listener(auth = auth, queueServiceUrl = "not a url") is Error,
            "an invalid queueServiceUrl must be rejected");
    test:assertTrue(new Listener(auth = {accountName: "nosuchaccount", accountKey: AZURITE_KEY}) is Listener,
            "init makes no call to Azure");
    string accountSas = "sv=2024-11-04&ss=bq&srt=co&sp=racup&sig=abc";
    test:assertTrue(new Listener(auth = {sasUrl: string `https://acc.example.com/?${accountSas}`}) is Error,
            "a SAS URL on a non-blob host needs queueServiceUrl");
    test:assertTrue(new Listener(auth = {sasUrl: string `https://acc.blob.core.windows.net/?${accountSas}`})
            is Listener, "a SAS URL on the blob host derives the queue endpoint");
    // The listener takes an account SAS covering both services; the kind and the services are
    // checked locally, in every credential form that carries a SAS.
    test:assertTrue(new Listener(auth = {accountName: "acc", sasToken: "sv=2024-11-04&sr=c&sp=rl&sig=abc"})
            is Error, "a service SAS must be rejected");
    test:assertTrue(new Listener(auth = {accountName: "acc", sasToken: "sv=2024-11-04&ss=b&srt=co&sp=r&sig=abc"})
            is Error, "an account SAS without the queue service must be rejected");
    test:assertTrue(new Listener(auth = {sasUrl: string `https://acc.blob.core.windows.net/?sv=2024&ss=q&sig=abc`})
            is Error, "an account SAS without the blob service must be rejected");
    test:assertTrue(new Listener(auth = {connectionString: "BlobEndpoint=https://acc.blob.core.windows.net;"
            + "QueueEndpoint=https://acc.queue.core.windows.net;SharedAccessSignature=sv=2024-11-04&sr=b&sp=r&sig=abc"})
            is Error, "a connection string's service SAS must be rejected");
    test:assertTrue(new Listener(auth = {connectionString: "BlobEndpoint=https://acc.blob.core.windows.net;"
            + string `QueueEndpoint=https://acc.queue.core.windows.net;SharedAccessSignature=${accountSas}`})
            is Listener, "a connection string's account SAS covering both services is accepted");
    // An explicit blob serviceUrl derives the queue endpoint by relabelling its host; a host
    // without the blob label (the emulator) needs queueServiceUrl, in a connection string too.
    test:assertTrue(new Listener(auth = {accountName: "acc", accountKey: AZURITE_KEY,
            serviceUrl: "https://acc.blob.core.chinacloudapi.cn"}) is Listener,
            "a sovereign blob serviceUrl derives the queue endpoint");
    test:assertTrue(new Listener(auth = {accountName: AZURITE_ACCOUNT, accountKey: AZURITE_KEY,
            serviceUrl: AZURITE_URL}) is Error, "an emulator serviceUrl without queueServiceUrl must be rejected");
    test:assertTrue(new Listener(auth = {connectionString: string `DefaultEndpointsProtocol=http;`
            + string `AccountName=${AZURITE_ACCOUNT};AccountKey=${AZURITE_KEY};BlobEndpoint=${AZURITE_URL}`}) is Error,
            "a connection string with an emulator BlobEndpoint and no QueueEndpoint must be rejected");
    test:assertTrue(new Listener(auth = {connectionString: string `DefaultEndpointsProtocol=http;`
            + string `AccountName=${AZURITE_ACCOUNT};AccountKey=${AZURITE_KEY};BlobEndpoint=${AZURITE_URL};`
            + string `QueueEndpoint=${AZURITE_QUEUE_URL}`}) is Listener,
            "a connection string's QueueEndpoint locates the queue service");
}

// ---- attach: the attach point and the annotation (spec 5.2, 5.3)

isolated function plainService(string queue) returns Service {
    return @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
}

@test:Config {}
function testAttachPointMustBeEmpty() returns error? {
    Listener lsn = check newListener();
    Service svc = plainService(testQueue("lsn-attach-point"));
    test:assertTrue(lsn.attach(svc, "invoices") is error, "a string attach point must be rejected");
    test:assertTrue(lsn.attach(svc, ["invoices"]) is error, "a path attach point must be rejected");
    test:assertTrue(lsn.attach(svc, "/invoices") is error, "a slashed attach point must be rejected");
    check lsn.attach(svc, []);
    check lsn.detach(svc);
    check lsn.attach(svc);
    check lsn.detach(svc);
}

@test:Config {}
function testServiceConfigRules() returns error? {
    Listener lsn = check newListener();
    Service unannotated = isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    error? missing = lsn.attach(unannotated);
    test:assertTrue(missing is error && missing.message().includes("ServiceConfig"),
            "a service without the annotation must be rejected naming it: " + describe(missing));

    foreach string bad in ["Invoices", "ab", "a--b", "-abc", "abc-", "a_b"] {
        test:assertTrue(lsn.attach(plainService(bad)) is error, "queue name must be rejected: " + bad);
    }
    Service badPoison = @ServiceConfig {queueName: "events", poisonQueueName: "Poison"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    test:assertTrue(lsn.attach(badPoison) is error, "an invalid poisonQueueName must be rejected");

    // 57 characters: valid on its own, too long once the default poison suffix is added.
    string long = "q" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    test:assertEquals(long.length(), 57);
    Service longWithoutPoison = @ServiceConfig {queueName: long} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    error? tooLong = lsn.attach(longWithoutPoison);
    test:assertTrue(tooLong is error && tooLong.message().includes("poisonQueueName"),
            "a 57-character queue name needs an explicit poisonQueueName: " + describe(tooLong));
    Service longWithPoison = @ServiceConfig {queueName: long, poisonQueueName: "long-poison"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(longWithPoison);
    check lsn.detach(longWithPoison);

    Service selfPoison = @ServiceConfig {queueName: "events", poisonQueueName: "events"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    test:assertTrue(lsn.attach(selfPoison) is error, "a poison queue equal to the queue must be rejected");

    test:assertTrue(lsn.attach(settingsService("events", batchSize = 0)) is error, "batchSize 0");
    test:assertTrue(lsn.attach(settingsService("events", batchSize = 33)) is error, "batchSize 33");
    test:assertTrue(lsn.attach(settingsService("events", newBatchThreshold = -1)) is error, "threshold -1");
    test:assertTrue(lsn.attach(settingsService("events", batchSize = 4, newBatchThreshold = 5)) is error,
            "threshold above batchSize");
    test:assertTrue(lsn.attach(settingsService("events", maxPollingInterval = 0)) is error, "polling 0");
    test:assertTrue(lsn.attach(settingsService("events", redeliveryDelay = -1)) is error, "redelivery -1");
    test:assertTrue(lsn.attach(settingsService("events", redeliveryDelay = 604801)) is error,
            "redelivery above seven days");
    test:assertTrue(lsn.attach(settingsService("events", maxDeliveryCount = 0)) is error, "maxDeliveryCount 0");
    Service atTheBounds = settingsService("events", batchSize = 32, newBatchThreshold = 32,
            maxPollingInterval = 0.5d, redeliveryDelay = 604800, maxDeliveryCount = 1);
    check lsn.attach(atTheBounds);
    check lsn.detach(atTheBounds);

    Service onlyOnError = @ServiceConfig {queueName: "events"} isolated service object {
        isolated remote function onError(Error err) returns error? {
        }
    };
    test:assertTrue(lsn.attach(onlyOnError) is error, "onError alone does not make a service");
    Service nonNilableEvent = @ServiceConfig {queueName: "events"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, BlobEvent event) returns error? {
        }
    };
    test:assertTrue(lsn.attach(nonNilableEvent) is error, "onError's event parameter must be nilable");
    Service callerBeforeEvent = @ServiceConfig {queueName: "events"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, Caller? caller, BlobEvent? event) returns error? {
        }
    };
    test:assertTrue(lsn.attach(callerBeforeEvent) is error, "onError takes the event before the Caller");
}

isolated function settingsService(string queue, int batchSize = 16, int newBatchThreshold = batchSize / 2,
        decimal maxPollingInterval = 60, decimal redeliveryDelay = 0, int maxDeliveryCount = 5)
        returns Service {
    return @ServiceConfig {
        queueName: queue,
        batchSize,
        newBatchThreshold,
        maxPollingInterval,
        redeliveryDelay,
        maxDeliveryCount
    } isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
}

@test:Config {}
function testQueueRelationsAmongServices() returns error? {
    Listener lsn = check newListener();
    Service a = plainService("queue-a");
    Service aAgain = plainService("queue-a");
    check lsn.attach(a);
    test:assertTrue(lsn.attach(aAgain) is error, "two services may not consume one queue");
    check lsn.detach(a);
    check lsn.attach(aAgain);
    // Another service may consume a's poison queue, or name a's queue as its poison queue.
    Service consumesPoison = plainService("queue-a-poison");
    check lsn.attach(consumesPoison);
    Service poisonsIntoA = @ServiceConfig {queueName: "queue-b", poisonQueueName: "queue-a"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(poisonsIntoA);
    // Two services may share a poison queue.
    Service sharedOne = @ServiceConfig {queueName: "queue-c", poisonQueueName: "dead"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    Service sharedTwo = @ServiceConfig {queueName: "queue-d", poisonQueueName: "dead"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(sharedOne);
    check lsn.attach(sharedTwo);
    // A two-service cycle: queue-a poisons into queue-b (attached) and queue-b poisons into queue-a.
    Service closesTwoCycle = @ServiceConfig {queueName: "queue-e", poisonQueueName: "queue-f"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    Service closesTwoCycleBack = @ServiceConfig {queueName: "queue-f", poisonQueueName: "queue-e"}
    isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(closesTwoCycle);
    error? cycle = lsn.attach(closesTwoCycleBack);
    test:assertTrue(cycle is error && cycle.message().includes("queue-e") && cycle.message().includes("queue-f"),
            "a poison cycle must be refused naming the queues: " + describe(cycle));
    // A three-service cycle: x -> y -> z -> x, refused at the service that closes it.
    Service x = @ServiceConfig {queueName: "queue-x", poisonQueueName: "queue-y"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    Service y = @ServiceConfig {queueName: "queue-y", poisonQueueName: "queue-z"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    Service z = @ServiceConfig {queueName: "queue-z", poisonQueueName: "queue-x"} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(x);
    check lsn.attach(y);
    test:assertTrue(lsn.attach(z) is error, "a three-service poison cycle must be refused");
    // Detached services no longer count: once y is gone, z closes no cycle.
    check lsn.detach(y);
    check lsn.attach(z);
}

// ---- lifecycle (spec 5.2)

@test:Config {}
function testListenerStartStopRules() returns error? {
    Listener lsn = check newListener();
    Service svc = plainService(testQueue("lsn-lifecycle"));
    check createQueue(testQueue("lsn-lifecycle"));
    check lsn.attach(svc);
    test:assertTrue(lsn.gracefulStop() is error, "stopping a listener that never started must fail");
    test:assertTrue(lsn.immediateStop() is error, "stopping a listener that never started must fail");
    check lsn.'start();
    test:assertTrue(lsn.'start() is error, "a running listener cannot be started again");
    check lsn.gracefulStop();
    test:assertTrue(lsn.'start() is error, "a stopped listener cannot be restarted");
    test:assertTrue(lsn.immediateStop() is (), "stopping a stopped listener again does nothing");
    test:assertTrue(lsn.gracefulStop() is (), "stopping a stopped listener again does nothing");
    test:assertTrue(lsn.attach(plainService("other")) is error, "attaching to a stopped listener must fail");
    test:assertTrue(lsn.detach(plainService("never")) is error, "detaching a service that is not attached must fail");
}

@test:Config {}
function testLateAttachPollsAtOnce() returns error? {
    EventSource first = check setupEventSource("lsn-late-attach");
    EventSource second = check setupEventSource("lsn-late-attach-2");
    Client secondBlobs = second.blobs;
    check secondBlobs->upload("late", "late.bin");
    check enqueueEvent(second.queue, blobCreatedEvent(second.container, "late.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    check lsn.attach(plainService(first.queue));
    check lsn.'start();
    final string secondQueue = second.queue;
    Service late = @ServiceConfig {queueName: secondQueue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.put("late", check string:fromBytes(content));
        }
    };
    check lsn.attach(late);
    check await(() => recorder.count("late") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("late"), "late", "a service attached to a running listener is polled at once");
}

@test:Config {}
function testDetachKeepsTheRunningHandler() returns error? {
    EventSource src = check setupEventSource("lsn-detach");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("slow", "slow.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            // The Caller keeps working after the detach.
            check caller->setTags(event.path, {seen: "yes"});
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.detach(svc);
    test:assertEquals(recorder.count("finished"), 0, "detach returns at once");
    // No new message is received after the detach, while the running handler settles its own.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slow.bin"));
    check await(() => recorder.count("finished") >= 1);
    runtime:sleep(2);
    test:assertEquals(recorder.count("started"), 1, "a detached service's queue is no longer polled");
    test:assertEquals(check approximateMessageCount(src.queue), 1, "the handled message settled; the new one waits");
    map<string> tags = check srcBlobs->getTags("slow.bin");
    test:assertEquals(tags, {seen: "yes"});
    check lsn.gracefulStop();
}

@test:Config {}
function testGracefulStopWaitsForHandlersOfDetachedServicesToo() returns error? {
    EventSource src = check setupEventSource("lsn-graceful");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("slow", "slow.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.detach(svc);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("finished"), 1, "gracefulStop returns after the detached service's handler");
    check await(() => approximateMessageCount(src.queue) == 0);
}

@test:Config {}
function testGracefulStopBoundThenTheHandlerSettles() returns error? {
    EventSource src = check setupEventSource("lsn-graceful-bound");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("slower", "slower.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slower.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("started");
            runtime:sleep(TEST_HOLD + 5);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("finished"), 0, "gracefulStop returns at its bound with the handler unfinished");
    // The handler continues, settles, and the message is gone.
    check await(() => recorder.count("finished") >= 1, timeoutSeconds = 30);
    check await(() => approximateMessageCount(src.queue) == 0);
}

@test:Config {}
function testImmediateStopNeverReleasesTheMessage() returns error? {
    EventSource src = check setupEventSource("lsn-immediate");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("slow", "slow.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.immediateStop();
    test:assertEquals(recorder.count("finished"), 0, "immediateStop returns while the handler still runs");
    test:assertEquals(check approximateMessageCount(src.queue), 1, "the message stays hidden, not released");
    check await(() => recorder.count("finished") >= 1);
    check await(() => approximateMessageCount(src.queue) == 0);
    test:assertEquals(check peekMessages(src.queue), [], "the handler settled its message after the stop");
}

@test:Config {}
function testCompetingConsumersShareAQueue() returns error? {
    EventSource src = check setupEventSource("lsn-competing");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "shared.bin");
    foreach int i in 0 ..< 10 {
        check enqueueEvent(src.queue, numberedCreatedEvent(src.container, "shared.bin", i));
    }
    final Recorder recorder = new;
    final string queue = src.queue;
    Service first = @ServiceConfig {queueName: queue, batchSize: 2} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            runtime:sleep(0.2);
            recorder.add("first", event.sequencer);
        }
    };
    Service second = @ServiceConfig {queueName: queue, batchSize: 2} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            runtime:sleep(0.2);
            recorder.add("second", event.sequencer);
        }
    };
    Listener one = check newListener();
    Listener two = check newListener();
    check one.attach(first);
    check two.attach(second);
    check one.'start();
    check two.'start();
    check await(() => recorder.count("first") + recorder.count("second") >= 10);
    runtime:sleep(1);
    check one.gracefulStop();
    check two.gracefulStop();
    string[] handled = [...recorder.list("first"), ...recorder.list("second")];
    test:assertEquals(handled.length(), 10, "every message handled once");
    string[] expected = from int i in 0 ..< 10 select string `seq-${i}`;
    test:assertEquals(handled.sort(), expected.sort(),
            "the union of the two consumers is the whole queue, without duplicates");
    test:assertTrue(recorder.count("first") > 0 && recorder.count("second") > 0, "both consumers took part");
    check await(() => approximateMessageCount(src.queue) == 0);
}
