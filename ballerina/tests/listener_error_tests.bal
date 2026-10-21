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

// Content fetching and binding, the stream forms, and error notification (spec 5.6, 5.12).

import ballerina/lang.runtime;
import ballerina/test;

// ---- fetch and binding (spec 5.6)

@test:Config {}
function testContentIsReadAtFetchTime() returns error? {
    EventSource src = check setupEventSource("lsn-fetch-time");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("version one", "v.txt");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "v.txt", "text/plain", 11));
    check srcBlobs->upload("version two", "v.txt");

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobText(string content, BlobEvent event, Caller caller) returns error? {
            recorder.put("text", content);
            BlobProperties props = check caller->getBlobProperties(event.path);
            recorder.put("etagDiffers", (props.eTag != event.eTag).toString());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("text") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("text"), "version two", "the content is the blob's at fetch time");
    test:assertEquals(recorder.payload("etagDiffers"), "true", "event.eTag lets the handler detect the change");
}

@test:Config {}
function testEmptyBlobsBindToEmptyContentOrFailTypedBinding() returns error? {
    EventSource src = check setupEventSource("lsn-empty");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("", "e.bin");
    check srcBlobs->upload("", "e.txt");
    check srcBlobs->upload("", "s.bin");
    check srcBlobs->upload("", "e.json");
    check srcBlobs->upload("", "e.csv");
    foreach string path in ["e.bin", "e.txt", "e.json", "e.csv"] {
        check enqueueEvent(src.queue, blobCreatedEvent(src.container, path, contentLength = 0));
    }

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.put("bytes", content.length().toString());
        }

        isolated remote function onBlobText(string content) returns error? {
            recorder.put("text", "[" + content + "]");
        }

        isolated remote function onBlobJson(Invoice content) returns error? {
            recorder.hit("json");
        }

        isolated remote function onBlobCsv(Invoice[] rows) returns error? {
            recorder.hit("csv");
        }

        isolated remote function onError(Error err, BlobEvent? event) returns error? {
            if err is ContentBindingError && event is BlobEvent {
                byte[]? content = err.detail().content;
                recorder.add("binding", event.path + ":" + (content is byte[] ? content.length().toString() : "none"));
            }
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("bytes") >= 1 && recorder.count("text") >= 1 && recorder.count("binding") >= 2);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("bytes"), "0", "an empty blob binds to empty bytes");
    test:assertEquals(recorder.payload("text"), "[]", "an empty blob binds to an empty string");
    test:assertEquals(recorder.count("json") + recorder.count("csv"), 0);
    test:assertEquals(recorder.list("binding").sort(), ["e.csv:0", "e.json:0"],
            "typed targets fail binding, with the empty content in the error");
    check await(() => approximateMessageCount(poisonQueue(queue)) == 2);

    // The byte stream of an empty blob ends at once.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "s.bin", contentLength = 0));
    Listener second = check newListener();
    Service streaming = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(stream<byte[], error?> content) returns error? {
            int chunks = 0;
            int bytes = 0;
            check from byte[] chunk in content
                do {
                    chunks += 1;
                    bytes += chunk.length();
                };
            recorder.put("chunks", string `${chunks}/${bytes}`);
        }
    };
    check second.attach(streaming);
    check second.'start();
    check await(() => recorder.count("chunks") >= 1);
    check second.gracefulStop();
    test:assertEquals(recorder.payload("chunks"), "0/0");
}

@test:Config {}
function testListenerLaxDataBinding() returns error? {
    EventSource src = check setupEventSource("lsn-lax");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("{\"name\": \"ann\"}", "c.json");
    check srcBlobs->upload("<Contact><name>bob</name><extra>x</extra></Contact>", "c.xml");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "c.json"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "c.xml"));

    final Recorder recorder = new;
    Listener lsn = check newListener(laxDataBinding = true);
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobJson(Contact content) returns error? {
            recorder.put("json", content.name + ":" + (content.email ?: "none"));
        }

        isolated remote function onBlobXml(Named content) returns error? {
            recorder.put("xml", content.name);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("json") >= 1 && recorder.count("xml") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("json"), "ann:none", "an absent field binds as nil under lax binding");
    test:assertEquals(recorder.payload("xml"), "bob", "an undeclared element is tolerated under lax binding");
}

@test:Config {}
function testFetchNotFoundAcknowledgesButContainerNotFoundRetries() returns error? {
    EventSource src = check setupEventSource("lsn-notfound");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "missing.bin"));
    string noContainer = testContainer("lsn-notfound-nocontainer");
    check enqueueEvent(src.queue, blobCreatedEvent(noContainer, "any.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            string code = err is ServiceError ? err.detail().errorCode : "";
            recorder.add("errors", describeNotification(err, event, caller) + "|" + code);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(queue) == 0 && approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    string[] errors = recorder.list("errors").sort();
    test:assertEquals(errors, ["NotFoundError|any.bin|caller|ContainerNotFound",
            "NotFoundError|any.bin|caller|ContainerNotFound", "NotFoundError|missing.bin|caller|BlobNotFound"],
            "BlobNotFound is acknowledged after one notification; ContainerNotFound is retried then poisoned");
}

@test:Config {}
function testBindingFailurePoisonsWithTheContent() returns error? {
    EventSource src = check setupEventSource("lsn-binding");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("{\"id\": 42}", "bad.json");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "bad.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobJson(Invoice content) returns error? {
            recorder.hit("json");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            if err is ContentBindingError {
                ContentBindingErrorDetail detail = err.detail();
                byte[]? content = detail.content;
                recorder.put("detail", detail.containerName + "|" + detail.path + "|"
                        + (content is byte[] ? check string:fromBytes(content) : "none"));
            }
            recorder.put("error", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("json"), 0);
    test:assertEquals(recorder.payload("detail"), src.container + "|bad.json|{\"id\": 42}");
    test:assertTrue(recorder.payload("error").startsWith("ContentBindingError|bad.json|caller|")
            && recorder.payload("error").includes("onBlobJson"), recorder.payload("error"));
    test:assertEquals(check approximateMessageCount(src.queue), 0);
}

@test:Config {}
function testFetchAuthorizationFailureIsAnOrdinaryFailure() returns error? {
    EventSource src = check setupEventSource("lsn-fetch-403");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("secret", "s.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "s.bin"));
    // The queue permissions without blob read: the fetch is refused, the message retried.
    string sas = check mintAccountSas({process: true, update: true, add: true, create: true});

    final Recorder recorder = new;
    Listener lsn = check newListenerWith(accountSasAuth(sas));
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.add("errors", describeNotification(err, event, caller));
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    test:assertEquals(recorder.list("errors"), ["AuthorizationError|s.bin|caller", "AuthorizationError|s.bin|caller"],
            "each delivery's fetch failure is notified; the limit then poisons the message");
}

// ---- the stream forms (spec 5.6)

// Puts a CSV blob whose second row cannot bind to Invoice on the queue, with a handler that
// reacts to the stream's error entry as the test directs.
function streamOutcome(string base, string reaction) returns [Recorder, EventSource]|error {
    EventSource src = check setupEventSource(base);
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("id,total\ninv-1,1.5\ninv-2,not-a-number\n", "rows.csv");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "rows.csv"));
    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    final string how = reaction;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlobCsv(stream<Invoice, error?> rows) returns error? {
            recorder.hit("attempt");
            string[] ids = [];
            error? failed = from Invoice row in rows
                do {
                    ids.push(row.id);
                };
            foreach string id in ids {
                recorder.add("rows", id);
            }
            if failed is () {
                return;
            }
            match how {
                "return" => {
                    return failed;
                }
                "deadLetter" => {
                    return error DeadLetterError("unreadable rows");
                }
                "retry" => {
                    return error RetryError("later", delay = 1);
                }
                "swallow" => {
                    return;
                }
            }
            return error("my own failure");
        }

        isolated remote function onError(Error err, BlobEvent? event) returns error? {
            if err is ContentBindingError {
                byte[]? content = err.detail().content;
                recorder.add("binding", content is () ? "no content" : "content");
            } else {
                recorder.add("other", err.message());
            }
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(queue) == 0);
    runtime:sleep(1);
    check lsn.gracefulStop();
    return [recorder, src];
}

@test:Config {}
function testReturningTheStreamsBindingErrorIsABindingFailure() returns error? {
    [Recorder, EventSource] [recorder, src] = check streamOutcome("lsn-stream-return", "return");
    test:assertEquals(recorder.count("attempt"), 1);
    test:assertEquals(recorder.list("rows"), ["inv-1"]);
    test:assertEquals(recorder.list("binding"), ["no content"], "onError gets a ContentBindingError without content");
    test:assertEquals(check approximateMessageCount(poisonQueue(src.queue)), 1);
}

@test:Config {}
function testDeadLetterAndRetryErrorsWinOverTheStreamsError() returns error? {
    [Recorder, EventSource] [dead, deadSource] = check streamOutcome("lsn-stream-deadletter", "deadLetter");
    test:assertEquals(dead.count("attempt"), 1);
    test:assertEquals(dead.count("binding"), 0, "a DeadLetterError is honoured as always, without a binding notice");
    test:assertEquals(check approximateMessageCount(poisonQueue(deadSource.queue)), 1);

    [Recorder, EventSource] [retried, retriedSource] = check streamOutcome("lsn-stream-retry", "retry");
    test:assertEquals(retried.count("attempt"), 2, "a RetryError redelivers; the final delivery then poisons");
    test:assertEquals(retried.count("binding"), 0);
    test:assertEquals(check approximateMessageCount(poisonQueue(retriedSource.queue)), 1);
}

@test:Config {}
function testSwallowingOrReplacingTheStreamsError() returns error? {
    [Recorder, EventSource] [swallowed, swallowedSource] = check streamOutcome("lsn-stream-swallow", "swallow");
    test:assertEquals(swallowed.count("attempt"), 1, "returning () after the bad row acknowledges");
    test:assertFalse(check queueExists(poisonQueue(swallowedSource.queue)));

    [Recorder, EventSource] [own, ownSource] = check streamOutcome("lsn-stream-own", "own");
    test:assertEquals(own.count("attempt"), 2, "any other error is an ordinary failure: retried, then poisoned");
    test:assertEquals(own.count("binding"), 0);
    test:assertEquals(check approximateMessageCount(poisonQueue(ownSource.queue)), 1);
}

@test:Config {}
function testStreamReadIsLockedToTheBlobVersion() returns error? {
    EventSource src = check setupEventSource("lsn-stream-version");
    Client srcBlobs = src.blobs;
    // Larger than one ranged read, so a replacement lands between two chunks.
    byte[] big = [];
    big.setLength(5 * 1024 * 1024);
    check srcBlobs->upload(big, "big.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "big.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue, maxDeliveryCount: 2} isolated service object {
        isolated remote function onBlob(stream<byte[], error?> content, BlobEvent event, Caller caller)
                returns error? {
            int attempt = recorder.next("attempt");
            int total = 0;
            // Pulled chunk by chunk: the stream's own entries carry the typed error.
            while true {
                record {|byte[] value;|}|error? chunk = content.next();
                if chunk is () {
                    break;
                }
                if chunk is error {
                    recorder.put("failure", chunk is PreconditionFailedError ? "PreconditionFailedError" : chunk.message());
                    return chunk;
                }
                total += chunk.value.length();
                if attempt == 1 && total <= chunk.value.length() {
                    check caller->upload("v2", event.path);
                }
            }
            recorder.put("total", total.toString());
            return;
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("total") >= 1, timeoutSeconds = 60);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("failure"), "PreconditionFailedError", "the next chunk after a replacement fails");
    test:assertEquals(recorder.count("attempt"), 2, "the handler's returned error makes the message visible again");
    test:assertEquals(recorder.payload("total"), "2", "the redelivery reads the new content");
}

// ---- error notification (spec 5.12)

@test:Config {}
function testOnErrorTakesItsOptionalParametersByType() returns error? {
    final Recorder recorder = new;
    Listener lsn = check newListener();
    string[] queues = [];
    foreach string form in ["e", "ee", "ec", "eec"] {
        string queue = testQueue("lsn-onerror-" + form);
        queues.push(queue);
        check createQueue(queue);
        check enqueueMessage(queue, "not an event");
    }
    final string q0 = queues[0];
    final string q1 = queues[1];
    final string q2 = queues[2];
    final string q3 = queues[3];
    Service errorOnly = @ServiceConfig {queueName: q0} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(error err) returns error? {
            recorder.put("e", err.message().includes(q0).toString());
        }
    };
    Service withEvent = @ServiceConfig {queueName: q1} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, BlobEvent? event) returns error? {
            recorder.put("ee", event is () ? "()" : "event");
        }
    };
    Service withCaller = @ServiceConfig {queueName: q2} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, Caller? caller) returns error? {
            recorder.put("ec", caller is () ? "()" : "caller");
        }
    };
    Service withBoth = @ServiceConfig {queueName: q3} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.put("eec", describeNotification(err, event, caller));
        }
    };
    check lsn.attach(errorOnly);
    check lsn.attach(withEvent);
    check lsn.attach(withCaller);
    check lsn.attach(withBoth);
    check lsn.'start();
    check await(() => recorder.count("e") >= 1 && recorder.count("ee") >= 1 && recorder.count("ec") >= 1
        && recorder.count("eec") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("e"), "true", "the error names the queue");
    test:assertEquals(recorder.payload("ee"), "()");
    test:assertEquals(recorder.payload("ec"), "()");
    test:assertEquals(recorder.payload("eec"), "Error|()|()");
}

@test:Config {}
function testPollFailureNotifiesOnlyTheOwningService() returns error? {
    string missing = testQueue("lsn-noqueue");
    EventSource src = check setupEventSource("lsn-noqueue-other");
    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string missingQueue = missing;
    final string otherQueue = src.queue;
    Service owner = @ServiceConfig {queueName: missingQueue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.put("owner", describeNotification(err, event, caller) + "|" + err.message());
        }
    };
    Service other = @ServiceConfig {queueName: otherQueue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("other");
        }
    };
    check lsn.attach(owner);
    check lsn.attach(other);
    check lsn.'start();
    check await(() => recorder.count("owner") >= 1);
    runtime:sleep(1);
    check lsn.immediateStop();
    string notified = recorder.payload("owner");
    test:assertTrue(notified.startsWith("NotFoundError|()|()|"), "polling a missing queue reports NotFoundError: " + notified);
    test:assertTrue(notified.includes(missing), "the error's message names the queue: " + notified);
    test:assertEquals(recorder.count("other"), 0, "the other service's queue is unaffected");
}

@test:Config {}
function testOnErrorRunsBeforeDisposalAndNeverSettles() returns error? {
    EventSource src = check setupEventSource("lsn-onerror-before");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("{\"id\": 1}", "bad.json");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "bad.json"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobJson(Invoice content) returns error? {
            recorder.hit("json");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            if err is ContentBindingError && event is BlobEvent && caller is Caller {
                // Runs before the message moves: the mark is visible when it has moved.
                check caller->setTags(event.path, {status: "unreadable"});
                runtime:sleep(2);
                recorder.hit("tagged");
            }
            // An error returned by onError changes nothing about the outcome.
            return error("onError's own complaint");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    test:assertEquals(recorder.count("tagged"), 1, "the move waited for onError");
    map<string> tags = check srcBlobs->getTags("bad.json");
    test:assertEquals(tags, {status: "unreadable"});
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("json"), 0);
    test:assertEquals(check approximateMessageCount(src.queue), 0, "the returned error did not keep the message");
}

@test:Config {}
function testGracefulStopWaitsForOnError() returns error? {
    EventSource src = check setupEventSource("lsn-onerror-graceful");
    check enqueueMessage(src.queue, "not an event");

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("finished"), 1, "gracefulStop returned after onError finished");
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
}

@test:Config {enable: liveRun}
function testArchivedBlobIsPoisonedAtOnce() returns error? {
    EventSource src = check setupEventSource("lsn-archived");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("cold", "cold.bin", {accessTier: ARCHIVE});
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "cold.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onError(Error err, BlobEvent? event, Caller? caller) returns error? {
            recorder.put("error", describeNotification(err, event, caller));
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(poisonQueue(queue)) == 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    test:assertEquals(recorder.payload("error"), "ArchivedBlobError|cold.bin|caller");
}
