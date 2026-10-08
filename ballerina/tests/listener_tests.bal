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

// Dual-mode listener and caller tests. Each test uploads a blob, puts the Event Grid message
// its subscription would deliver on the test's queue, starts a Listener, and awaits the
// dispatch its attached service records. The services are anonymous service objects, so the
// compiler plugin leaves them alone and the tests control the handler shapes directly.

import ballerina/file;
import ballerina/lang.runtime;
import ballerina/test;

// Records handler dispatches so a test can await and assert them across the listener's
// dispatch threads.
isolated class Recorder {
    private final map<int> hits = {};
    private final map<string> payloads = {};

    isolated function hit(string key) {
        lock {
            self.hits[key] = (self.hits[key] ?: 0) + 1;
        }
    }

    isolated function put(string key, string payload) {
        lock {
            self.hits[key] = (self.hits[key] ?: 0) + 1;
            self.payloads[key] = payload;
        }
    }

    isolated function count(string key) returns int {
        lock {
            return self.hits[key] ?: 0;
        }
    }

    isolated function payload(string key) returns string {
        lock {
            return self.payloads[key] ?: "";
        }
    }
}

type Invoice record {|
    string id;
    decimal total;
|};

type Contact record {|
    string name;
    string? email;
|};

// ---- dispatch and the event

@test:Config {}
function testListenerOnBlobDispatchWithEventAndCaller() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-onblob");
    Client container = setup[0];
    check container->upload("payload-onblob", "incoming/note.dat");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "incoming/note.dat", "application/octet-stream", 14));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.put("onblob", check string:fromBytes(content));
            recorder.put("eventType", event.eventType);
            recorder.put("container", event.containerName);
            recorder.put("path", event.path);
            recorder.put("url", event.url);
            recorder.put("api", event.api);
            recorder.put("contentType", event.contentType ?: "");
            recorder.put("contentLength", (event.contentLength ?: -1).toString());
            recorder.put("blobType", event.blobType ?: "");
            recorder.put("eTag", event.eTag ?: "");
            recorder.put("sequencer", event.sequencer);
            recorder.put("eventTime", event.eventTime[0].toString());
            check caller->deleteBlob(event.path);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("onblob") >= 1);
    check lsn.gracefulStop();

    test:assertEquals(recorder.payload("onblob"), "payload-onblob");
    test:assertEquals(recorder.payload("eventType"), "BLOB_CREATED");
    test:assertEquals(recorder.payload("container"), setup[1]);
    test:assertEquals(recorder.payload("path"), "incoming/note.dat");
    test:assertEquals(recorder.payload("url"), blobUrl(setup[1], "incoming/note.dat"));
    test:assertEquals(recorder.payload("api"), "PutBlob");
    test:assertEquals(recorder.payload("contentType"), "application/octet-stream");
    test:assertEquals(recorder.payload("contentLength"), "14");
    test:assertEquals(recorder.payload("blobType"), "BlockBlob");
    test:assertEquals(recorder.payload("eTag"), "0x8D4BCC2E4835CD0");
    test:assertEquals(recorder.payload("sequencer"), "00000000000004420000000000028963");
    test:assertEquals(recorder.payload("eventTime"), "1498502460", "eventTime must be the event's timestamp");
    // The Caller acted on the event's blob, and the handled message is gone from the queue.
    boolean stillThere = check container->hasBlob("incoming/note.dat");
    test:assertFalse(stillThere);
    check await(() => approximateMessageCount(setup[2]) == 0);
}

@test:Config {}
function testListenerCloudEventsSchemaAndRawBody() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-cloudevents");
    Client origin = setup[0];
    check origin->upload("raw-body", "ce.bin");
    check enqueueEvent(setup[2], blobCreatedCloudEvent(setup[1], "ce.bin", "application/octet-stream", 8),
            encode = false);

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("onblob", check string:fromBytes(content));
            recorder.put("api", event.api);
            recorder.put("eventTime", event.eventTime[0].toString());
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("onblob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("onblob"), "raw-body");
    test:assertEquals(recorder.payload("api"), "PutBlockList");
    test:assertEquals(recorder.payload("eventTime"), "1498502460");
}

@test:Config {}
function testListenerOnBlobStream() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-stream");
    Client origin = setup[0];
    check origin->upload("streamed-content", "big.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "big.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(stream<byte[], error?> content) returns error? {
            byte[] all = [];
            check from byte[] chunk in content
                do {
                    all.push(...chunk);
                };
            recorder.put("stream", check string:fromBytes(all));
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("stream") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("stream"), "streamed-content");
}

@test:Config {}
function testListenerTypedHandlers() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-typed");
    Client container = setup[0];
    check container->upload("hello text", "a.txt");
    Invoice first = {id: "inv-1", total: 12.5d};
    check container->upload(first, "b.json");
    xml second = xml `<invoice><id>inv-2</id><total>7.25</total></invoice>`;
    check container->upload(second, "c.xml");
    check container->upload("id,total\ninv-3,1.5\ninv-4,2.5\n", "d.csv");
    foreach string path in ["a.txt", "b.json", "c.xml", "d.csv"] {
        check enqueueEvent(setup[2], blobCreatedEvent(setup[1], path));
    }

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobText(string content) returns error? {
            recorder.put("text", content);
        }

        remote function onBlobJson(Invoice content) returns error? {
            recorder.put("json", content.id + ":" + content.total.toString());
        }

        remote function onBlobXml(Invoice content) returns error? {
            recorder.put("xml", content.id + ":" + content.total.toString());
        }

        remote function onBlobCsv(Invoice[] rows) returns error? {
            recorder.put("csv", rows.length().toString() + ":" + rows[1].id);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("text") >= 1 && recorder.count("json") >= 1
        && recorder.count("xml") >= 1 && recorder.count("csv") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("text"), "hello text");
    test:assertEquals(recorder.payload("json"), "inv-1:12.5");
    test:assertEquals(recorder.payload("xml"), "inv-2:7.25");
    test:assertEquals(recorder.payload("csv"), "2:inv-4");
}

@test:Config {}
function testListenerBareJsonXmlAndCsvForms() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-bare");
    Client container = setup[0];
    json plain = {id: "j"};
    check container->upload(plain, "j.json");
    xml doc = xml `<x>v</x>`;
    check container->upload(doc, "x.xml");
    check container->upload("h1,h2\na,b\nc,d\n", "m.csv");
    check container->upload("id,total\ninv-5,3.5\n", "s1.csv");
    check container->upload("h1,h2\ne,f\n", "s2.csv");
    foreach string path in ["j.json", "x.xml", "m.csv"] {
        check enqueueEvent(setup[2], blobCreatedEvent(setup[1], path));
    }

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobJson(json content) returns error? {
            recorder.put("json", content.toJsonString());
        }

        remote function onBlobXml(xml content) returns error? {
            recorder.put("xml", content.toString());
        }

        remote function onBlobCsv(string[][] rows) returns error? {
            recorder.put("csv", rows.length().toString() + ":" + rows[2][1]);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("json") >= 1 && recorder.count("xml") >= 1 && recorder.count("csv") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("json"), "{\"id\":\"j\"}");
    test:assertEquals(recorder.payload("xml"), "<x>v</x>");
    test:assertEquals(recorder.payload("csv"), "3:d", "a string matrix keeps the header row");

    // The two CSV stream forms, on a second listener over the same queue.
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "s1.csv"));
    final Recorder streams = new;
    Listener second = check newListener(setup[2]);
    Service recordStream = service object {
        remote function onBlobCsv(stream<Invoice, error?> rows) returns error? {
            string[] ids = [];
            check from Invoice row in rows
                do {
                    ids.push(row.id);
                };
            streams.put("records", string:'join(",", ...ids));
        }
    };
    check second.attach(recordStream, setup[1]);
    check second.'start();
    check await(() => streams.count("records") >= 1);
    check second.gracefulStop();
    test:assertEquals(streams.payload("records"), "inv-5");

    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "s2.csv"));
    Listener third = check newListener(setup[2]);
    Service rowStream = service object {
        remote function onBlobCsv(stream<string[], error?> rows) returns error? {
            string[] cells = [];
            check from string[] row in rows
                do {
                    cells.push(...row);
                };
            streams.put("rows", string:'join(",", ...cells));
        }
    };
    check third.attach(rowStream, setup[1]);
    check third.'start();
    check await(() => streams.count("rows") >= 1);
    check third.gracefulStop();
    test:assertEquals(streams.payload("rows"), "h1,h2,e,f");
}

// ---- routing

@test:Config {}
function testListenerRoutingByExtensionContentTypeAndFallback() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-routing");
    Client container = setup[0];
    check container->upload("t", "notes.TXT");
    check container->upload("{}", "noext-json");
    check container->upload("x", "image.png");
    check container->upload("{}", "data.json");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "notes.TXT", "application/octet-stream"));
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "noext-json", "application/json; charset=utf-8"));
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "image.png", "image/png"));
    // A .json name with onBlobJson declared routes there, not to onBlob.
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "data.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobText(string content, BlobEvent event) returns error? {
            recorder.put("text", event.path);
        }

        remote function onBlobJson(json content, BlobEvent event) returns error? {
            recorder.put("json", event.path);
        }

        remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("blob:" + event.path, event.path);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("text") >= 1 && recorder.count("json") >= 2
        && recorder.count("blob:image.png") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("text"), "notes.TXT", "the extension match is case-insensitive");
    test:assertEquals(recorder.count("blob:data.json"), 0, "a .json name with onBlobJson declared routes there");
}

@test:Config {}
function testListenerRoutingUndeclaredTypedHandlerFallsBackToOnBlob() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-fallback");
    Client origin = setup[0];
    check origin->upload("<x/>", "doc.xml");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "doc.xml", "application/xml"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("blob", event.path);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("blob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("blob"), "doc.xml");
}

@test:Config {}
function testListenerFunctionConfigOverridesRouting() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-funcconfig");
    Client container = setup[0];
    check container->upload("{}", "invoice-9.dat");
    check container->upload("plain", "report.bin");
    check container->upload("{}", "other.json");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "invoice-9.dat"));
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "report.bin", "text/plain"));
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "other.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        @FunctionConfig {namePattern: "^invoice-.*"}
        remote function onBlobJson(json content, BlobEvent event) returns error? {
            recorder.put("json", event.path);
        }

        @FunctionConfig {contentTypePattern: "text/.*"}
        remote function onBlobText(string content, BlobEvent event) returns error? {
            recorder.put("text", event.path);
        }

        remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("blob", event.path);
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("json") >= 2 && recorder.count("text") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("text"), "report.bin", "the content type pattern routes a .bin to text");
    test:assertEquals(recorder.count("blob"), 0, "every event found a typed handler");
}

@test:Config {}
function testListenerNoHandlerAcknowledges() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-nohandler");
    Client origin = setup[0];
    check origin->upload("x", "file.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "file.bin"));
    check enqueueEvent(setup[2], blobDeletedEvent(setup[1], "gone.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobText(string content) returns error? {
            recorder.hit("text");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => approximateMessageCount(setup[2]) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("text"), 0);
}

@test:Config {}
function testListenerOtherEventTypesAreAcknowledged() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-tierchanged");
    check enqueueEvent(setup[2], blobTierChangedEvent(setup[1], "cold.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => approximateMessageCount(setup[2]) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    test:assertEquals(recorder.count("error"), 0);
}

// ---- containers and services

@test:Config {}
function testListenerRoutesByContainerWithCatchAll() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-containers");
    string other = testContainer("lsn-containers-other");
    string unclaimed = testContainer("lsn-containers-unclaimed");
    AdminClient admin = check newAdmin();
    check createTestContainer(admin, other);
    check createTestContainer(admin, unclaimed);
    Client origin = setup[0];
    check origin->upload("named", "n.bin");
    Client otherClient = check newContainerClient(other);
    check otherClient->upload("caught", "o.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "n.bin"));
    check enqueueEvent(setup[2], blobCreatedEvent(other, "o.bin"));
    check enqueueEvent(setup[2], blobCreatedEvent(unclaimed, "u.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service named = service object {
        remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("named", check string:fromBytes(content));
        }
    };
    Service catchAll = service object {
        remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.put("catchall", check string:fromBytes(content));
            recorder.put("catchall-container", event.containerName);
            // The per-event Caller is bound to the event's container.
            BlobProperties props = check caller->getBlobProperties(event.path);
            recorder.put("catchall-size", props.contentLength.toString());
        }
    };
    check lsn.attach(named, setup[1]);
    check lsn.attach(catchAll);
    check lsn.'start();
    check await(() => recorder.count("named") >= 1 && recorder.count("catchall") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("named"), "named");
    test:assertEquals(recorder.payload("catchall"), "caught");
    test:assertEquals(recorder.payload("catchall-container"), other);
    test:assertEquals(recorder.payload("catchall-size"), "6");
    test:assertEquals(recorder.count("catchall"), 1, "the catch-all must not receive the named container's events");
}

@test:Config {}
function testListenerUnclaimedContainerIsAcknowledged() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-unclaimed");
    check enqueueEvent(setup[2], blobCreatedEvent(testContainer("lsn-unclaimed-x"), "u.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => approximateMessageCount(setup[2]) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
}

@test:Config {}
function testListenerAttachRules() returns error? {
    Listener lsn = check newListener(testQueue("lsn-attach"));
    Service first = service object {
        remote function onBlob(byte[] content) returns error? {
        }
    };
    Service second = service object {
        remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(first, "invoices");
    error? duplicate = lsn.attach(second, ["invoices"]);
    test:assertTrue(duplicate is error, "a second service for the same container must be rejected");
    check lsn.attach(second);
    Service third = service object {
        remote function onBlob(byte[] content) returns error? {
        }
    };
    error? secondCatchAll = lsn.attach(third);
    test:assertTrue(secondCatchAll is error, "a second catch-all must be rejected");
    test:assertTrue(lsn.attach(third, "Invoices") is error, "an uppercase container name must be rejected");
    test:assertTrue(lsn.attach(third, ["a", "b"]) is error, "a two-segment attach point must be rejected");
    test:assertTrue(lsn.attach(third, "ab") is error, "a two-character name must be rejected");
    string tooLong = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    test:assertTrue(lsn.attach(third, tooLong) is error, "a 64-character name must be rejected");
    check lsn.attach(third, "/$root");
    test:assertTrue(lsn.detach(third) is (), "a $root service attaches and detaches");
    test:assertTrue(lsn.detach(third) is error, "detaching a service that is not attached must fail");
    check lsn.detach(first);
    check lsn.detach(second);
}

@test:Config {}
function testListenerInitValidation() {
    SharedKeyConfig auth = sharedKeyAuth();
    test:assertTrue(new Listener("q", auth = auth, batchSize = 0) is Error, "batchSize 0 must be rejected");
    test:assertTrue(new Listener("q", auth = auth, batchSize = 33) is Error, "batchSize 33 must be rejected");
    test:assertTrue(new Listener("q", auth = auth, batchSize = 4, newBatchThreshold = 5) is Error,
            "newBatchThreshold above batchSize must be rejected");
    test:assertTrue(new Listener("q", auth = auth, maxPollingIntervalSeconds = 0) is Error,
            "maxPollingIntervalSeconds 0 must be rejected");
    test:assertTrue(new Listener("q", auth = auth, redeliveryDelaySeconds = -1) is Error,
            "a negative redeliveryDelaySeconds must be rejected");
    test:assertTrue(new Listener("q", auth = auth, maxDeliveryCount = 0) is Error, "maxDeliveryCount 0 must be rejected");
    test:assertTrue(new Listener(" ", auth = auth) is Error, "an empty queue name must be rejected");
    test:assertTrue(new Listener("q", auth = auth, queueServiceUrl = "not a url") is Error,
            "an invalid queueServiceUrl must be rejected");
    test:assertTrue(new Listener("q", auth = {sasUrl: "https://acc.example.com/?sv=2024-11-04&sig=abc"}) is Error,
            "a SAS URL on a non-blob host needs queueServiceUrl");
    test:assertTrue(new Listener("q", auth = {sasUrl: "https://acc.blob.core.windows.net/?sv=2024-11-04&sig=abc"}) is Listener,
            "a SAS URL on the blob host derives the queue endpoint");
}

// ---- lifecycle

@test:Config {}
function testListenerStartStopRules() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-lifecycle");
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
        }
    };
    check lsn.attach(svc, setup[1]);
    test:assertTrue(lsn.gracefulStop() is error, "stopping a listener that never started must fail");
    check lsn.'start();
    test:assertTrue(lsn.'start() is error, "a running listener cannot be started again");
    check lsn.gracefulStop();
    test:assertTrue(lsn.'start() is error, "a stopped listener cannot be restarted");
    test:assertTrue(lsn.immediateStop() is error, "a stopped listener cannot be stopped again");
}

@test:Config {}
function testListenerGracefulStopWaitsForHandlers() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-graceful");
    Client origin = setup[0];
    check origin->upload("slow", "slow.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("finished"), 1, "gracefulStop returns after the running handler finished");
    check await(() => approximateMessageCount(setup[2]) == 0);
}

@test:Config {}
function testListenerImmediateStopReturnsWhileHandlerRuns() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-immediate");
    Client origin = setup[0];
    check origin->upload("slow", "slow.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "slow.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("started");
            runtime:sleep(3);
            recorder.hit("finished");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("started") >= 1);
    check lsn.immediateStop();
    test:assertEquals(recorder.count("finished"), 0, "immediateStop returns while the handler still runs");
    // The released message stays on the queue for redelivery once its visibility expires.
    test:assertEquals(check approximateMessageCount(setup[2]), 1);
}

// ---- failures and delivery

@test:Config {}
function testListenerHandlerErrorRedeliversThenPoisons() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-poison");
    Client origin = setup[0];
    check origin->upload("bad", "bad.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "bad.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2], maxDeliveryCount = 3);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("attempt");
            if recorder.count("attempt") == 2 {
                panic error("handler panic");
            }
            return error("handler failure");
        }

        remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("attempt") >= 3);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("attempt"), 3, "the message is delivered maxDeliveryCount times");
    test:assertEquals(recorder.count("error"), 0, "handler errors do not notify onError");
    check await(() => approximateMessageCount(setup[2]) == 0);
    string[] poisoned = check peekMessages(setup[2] + "-poison");
    test:assertEquals(poisoned.length(), 1, "the message moved to the poison queue");
    test:assertEquals(poisoned[0], blobCreatedEvent(setup[1], "bad.bin").toJsonString().toBytes().toBase64(),
            "the poison queue holds the original message body");
}

@test:Config {}
function testListenerUnparsableMessageNotifiesAndPoisons() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-unparsable");
    check enqueueMessage(setup[2], "not an event");

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2], maxDeliveryCount = 2);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        remote function onError(Error err, Caller caller) returns error? {
            recorder.put("error", err.message());
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("error") >= 2);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 0);
    test:assertTrue(recorder.payload("error").includes("not a JSON storage event"), recorder.payload("error"));
    check await(() => approximateMessageCount(setup[2]) == 0);
    test:assertEquals((check peekMessages(setup[2] + "-poison")).length(), 1);
}

@test:Config {}
function testListenerFetchNotFoundNotifiesAndAcknowledges() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-notfound");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "missing.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        remote function onError(Error err) returns error? {
            recorder.put("error", err is NotFoundError ? "NotFoundError" : err.message());
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("error") >= 1);
    check await(() => approximateMessageCount(setup[2]) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("error"), "NotFoundError");
    test:assertEquals(recorder.count("error"), 1, "a missing blob is acknowledged, not redelivered");
    test:assertEquals(recorder.count("blob"), 0);
}

@test:Config {}
function testListenerBindingFailureNotifiesAndAcknowledges() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-binding");
    Client origin = setup[0];
    check origin->upload("{\"id\": 42}", "bad.json");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "bad.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobJson(Invoice content) returns error? {
            recorder.hit("json");
        }

        remote function onError(Error err) returns error? {
            recorder.put("error", err.message());
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("error") >= 1);
    check await(() => approximateMessageCount(setup[2]) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("json"), 0);
    test:assertTrue(recorder.payload("error").includes("onBlobJson"), recorder.payload("error"));
}

@test:Config {}
function testListenerLaxDataBinding() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-lax");
    Client origin = setup[0];
    check origin->upload("{\"name\": \"ann\"}", "c.json");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "c.json"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2], laxDataBinding = true);
    Service svc = service object {
        remote function onBlobJson(Contact content) returns error? {
            recorder.put("json", content.name + ":" + (content.email ?: "none"));
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("json") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("json"), "ann:none", "an absent field binds as nil under lax binding");
}

@test:Config {}
function testListenerPollFailureNotifiesOnError() returns error? {
    string queue = testQueue("lsn-noqueue");
    final Recorder recorder = new;
    Listener lsn = check newListener(queue);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
        }

        remote function onError(Error err, Caller caller) returns error? {
            recorder.put("error", err is NotFoundError ? "NotFoundError" : err.message());
        }
    };
    check lsn.attach(svc, testContainer("lsn-noqueue"));
    check lsn.'start();
    check await(() => recorder.count("error") >= 1);
    check lsn.immediateStop();
    test:assertEquals(recorder.payload("error"), "NotFoundError", "polling a missing queue reports NotFoundError");
}

@test:Config {}
function testListenerOnBlobDeleted() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-deleted");
    check enqueueEvent(setup[2], blobDeletedEvent(setup[1], "old/gone.txt"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        remote function onBlobDeleted(BlobEvent event, Caller caller) returns error? {
            recorder.put("deleted", event.path);
            recorder.put("eventType", event.eventType);
            recorder.put("contentLength", event.contentLength is () ? "absent" : "present");
            recorder.put("eTag", event.eTag is () ? "absent" : "present");
            byte[]|Error gone = caller->getBlob("old/gone.txt");
            recorder.put("getBlob", gone is NotFoundError ? "NotFoundError" : "other");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("deleted") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("deleted"), "old/gone.txt");
    test:assertEquals(recorder.payload("eventType"), "BLOB_DELETED");
    test:assertEquals(recorder.payload("contentLength"), "absent", "a deleted event carries no content length");
    test:assertEquals(recorder.payload("eTag"), "absent", "a deleted event carries no eTag");
    test:assertEquals(recorder.count("blob"), 0);
    test:assertEquals(recorder.payload("getBlob"), "NotFoundError", "the Caller reads nothing for a deleted blob");
}

// ---- the Caller

@test:Config {}
function testCallerOperations() returns error? {
    [Client, string, string] setup = check setupEventSource("lsn-caller");
    Client container = setup[0];
    Invoice invoice = {id: "inv-7", total: 70.0d};
    check container->upload(invoice, "in/inv-7.json");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "in/inv-7.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener(setup[2]);
    Service svc = service object {
        remote function onBlobJson(Invoice content, BlobEvent event, Caller caller) returns error? {
            Invoice again = check caller->getBlob(event.path);
            recorder.put("getBlob", again.id);
            BlobProperties props = check caller->getBlobProperties(event.path);
            recorder.put("contentType", props.contentHeaders.contentType ?: "");
            check caller->setTags(event.path, {status: "processed"});
            check caller->upload(content, "done/" + content.id + ".json");
            CopyInfo copy = check caller->copyBlobFromUrl(event.url, "archive/" + content.id + ".json");
            recorder.put("copy", copy.copyStatus);
            string target = check file:createTemp("-caller.json");
            check file:remove(target);
            check caller->download(event.path, target);
            check file:remove(target);
            check caller->deleteBlob(event.path);
            recorder.hit("done");
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("done") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("getBlob"), "inv-7");
    test:assertEquals(recorder.payload("contentType"), "application/json");
    test:assertEquals(recorder.payload("copy"), "success");
    boolean done = check container->hasBlob("done/inv-7.json");
    boolean archived = check container->hasBlob("archive/inv-7.json");
    boolean original = check container->hasBlob("in/inv-7.json");
    test:assertTrue(done);
    test:assertTrue(archived);
    test:assertFalse(original);
}

// ---- the credential forms the listener documentation names

// Drives one created event through a listener built over the given credential.
function assertListenerDispatches(string base, AuthConfig auth) returns error? {
    [Client, string, string] setup = check setupEventSource(base);
    Client origin = setup[0];
    check origin->upload("via " + base, "cred.bin");
    check enqueueEvent(setup[2], blobCreatedEvent(setup[1], "cred.bin"));
    final Recorder recorder = new;
    Listener lsn = check newListenerWith(setup[2], auth);
    Service svc = service object {
        remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.put("blob", check string:fromBytes(content));
            // The Caller shares the listener's credential; a write proves it covers the blob service.
            check caller->setTags(event.path, {seen: "yes"});
        }
    };
    check lsn.attach(svc, setup[1]);
    check lsn.'start();
    check await(() => recorder.count("blob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("blob"), "via " + base);
    map<string> tags = check origin->getTags("cred.bin");
    test:assertEquals(tags, {seen: "yes"});
}

@test:Config {}
function testListenerOverAccountSas() returns error? {
    string sas = check queueSas();
    return assertListenerDispatches("lsn-sas", accountSasAuth(sas));
}

@test:Config {}
function testListenerOverConnectionString() returns error? {
    return assertListenerDispatches("lsn-connstr", connectionStringAuth());
}

@test:Config {}
function testClientsOverConnectionString() returns error? {
    string container = testContainer("connstr");
    AdminClient admin = check new (auth = connectionStringAuth());
    check createTestContainer(admin, container);
    boolean exists = check admin->hasContainer(container);
    test:assertTrue(exists);
    Client blobClient = check new (container, auth = connectionStringAuth());
    check blobClient->upload("over a connection string", "cs.txt");
    string content = check blobClient->getBlob("cs.txt");
    test:assertEquals(content, "over a connection string");
}

// A listener over a SAS URL whose host carries no blob service label, with no queue endpoint
// given, cannot locate its queue; the live form derives it from the blob host.
@test:Config {enable: liveRun}
function testListenerOverSasUrlDerivesQueueEndpoint() returns error? {
    string sas = check queueSas();
    string sasUrl = string `https://${liveAccountName}.blob.core.windows.net/?${sas}`;
    return assertListenerDispatches("lsn-sasurl", {sasUrl});
}
