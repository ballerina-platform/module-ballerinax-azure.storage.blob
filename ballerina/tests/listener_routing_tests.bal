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

// Dispatch, the event, routing, the never-ours and foreign-account checks, the Caller, and the
// credential forms (spec 5.5, 5.7, 5.8, 5.13). Each test uploads a blob, puts the Event Grid
// message its subscription would deliver on the test's queue, starts a Listener, and awaits the
// dispatch its attached service records.

import ballerina/file;
import ballerina/lang.runtime;
import ballerina/test;

// ---- dispatch and the event

@test:Config {}
function testListenerOnBlobDispatchWithEventAndCaller() returns error? {
    EventSource src = check setupEventSource("lsn-onblob");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("payload-onblob", "incoming/note.dat");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "incoming/note.dat", "application/octet-stream", 14));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.put("onblob", check string:fromBytes(content));
            recorder.put("eventType", event.eventType);
            recorder.put("accountName", event.accountName);
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
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("onblob") >= 1);
    check lsn.gracefulStop();

    test:assertEquals(recorder.payload("onblob"), "payload-onblob");
    test:assertEquals(recorder.payload("eventType"), "BLOB_CREATED");
    test:assertEquals(recorder.payload("accountName"), accountName());
    test:assertEquals(recorder.payload("container"), src.container);
    test:assertEquals(recorder.payload("path"), "incoming/note.dat");
    test:assertEquals(recorder.payload("url"), blobUrl(src.container, "incoming/note.dat"));
    test:assertEquals(recorder.payload("api"), "PutBlob");
    test:assertEquals(recorder.payload("contentType"), "application/octet-stream");
    test:assertEquals(recorder.payload("contentLength"), "14");
    test:assertEquals(recorder.payload("blobType"), "BlockBlob");
    test:assertEquals(recorder.payload("eTag"), "0x8D4BCC2E4835CD0");
    test:assertEquals(recorder.payload("sequencer"), "00000000000004420000000000028963");
    test:assertEquals(recorder.payload("eventTime"), "1498502460", "eventTime must be the event's timestamp");
    // The Caller acted on the event's blob, and the handled message is gone from the queue.
    boolean stillThere = check srcBlobs->hasBlob("incoming/note.dat");
    test:assertFalse(stillThere);
    check await(() => approximateMessageCount(src.queue) == 0);
}

@test:Config {}
function testListenerCloudEventsSchemaAndRawBody() returns error? {
    EventSource src = check setupEventSource("lsn-cloudevents");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("raw-body", "ce.bin");
    check enqueueEvent(src.queue, blobCreatedCloudEvent(src.container, "ce.bin", "application/octet-stream", 8),
            encode = false);

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("onblob", check string:fromBytes(content));
            recorder.put("api", event.api);
            recorder.put("accountName", event.accountName);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("onblob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("onblob"), "raw-body");
    test:assertEquals(recorder.payload("api"), "PutBlockList");
    test:assertEquals(recorder.payload("accountName"), accountName(), "the account comes from the CloudEvents src");
}

@test:Config {}
function testListenerOnBlobStream() returns error? {
    EventSource src = check setupEventSource("lsn-stream");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("streamed-content", "big.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "big.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(stream<byte[], error?> content) returns error? {
            byte[] all = [];
            check from byte[] chunk in content
                do {
                    all.push(...chunk);
                };
            recorder.put("stream", check string:fromBytes(all));
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("stream") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("stream"), "streamed-content");
}

@test:Config {}
function testListenerTypedHandlers() returns error? {
    EventSource src = check setupEventSource("lsn-typed");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("hello text", "a.txt");
    Invoice first = {id: "inv-1", total: 12.5d};
    check srcBlobs->upload(first, "b.json");
    xml second = xml `<invoice><id>inv-2</id><total>7.25</total></invoice>`;
    check srcBlobs->upload(second, "c.xml");
    check srcBlobs->upload("id,total\ninv-3,1.5\ninv-4,2.5\n", "d.csv");
    foreach string path in ["a.txt", "b.json", "c.xml", "d.csv"] {
        check enqueueEvent(src.queue, blobCreatedEvent(src.container, path));
    }

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobText(string content) returns error? {
            recorder.put("text", content);
        }

        isolated remote function onBlobJson(Invoice content) returns error? {
            recorder.put("json", content.id + ":" + content.total.toString());
        }

        isolated remote function onBlobXml(Invoice content) returns error? {
            recorder.put("xml", content.id + ":" + content.total.toString());
        }

        isolated remote function onBlobCsv(Invoice[] rows) returns error? {
            recorder.put("csv", rows.length().toString() + ":" + rows[1].id);
        }
    };
    check lsn.attach(svc);
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
    EventSource src = check setupEventSource("lsn-bare");
    Client srcBlobs = src.blobs;
    json plain = {id: "j"};
    check srcBlobs->upload(plain, "j.json");
    xml doc = xml `<x>v</x>`;
    check srcBlobs->upload(doc, "x.xml");
    check srcBlobs->upload("h1,h2\na,b\nc,d\n", "m.csv");
    check srcBlobs->upload("id,total\ninv-5,3.5\n", "s1.csv");
    check srcBlobs->upload("h1,h2\ne,f\n", "s2.csv");
    foreach string path in ["j.json", "x.xml", "m.csv"] {
        check enqueueEvent(src.queue, blobCreatedEvent(src.container, path));
    }

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobJson(json content) returns error? {
            recorder.put("json", content.toJsonString());
        }

        isolated remote function onBlobXml(xml content) returns error? {
            recorder.put("xml", content.toString());
        }

        isolated remote function onBlobCsv(string[][] rows) returns error? {
            recorder.put("csv", rows.length().toString() + ":" + rows[2][1]);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("json") >= 1 && recorder.count("xml") >= 1 && recorder.count("csv") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("json"), "{\"id\":\"j\"}");
    test:assertEquals(recorder.payload("xml"), "<x>v</x>");
    test:assertEquals(recorder.payload("csv"), "3:d", "a string matrix keeps the header row");

    // The two CSV stream forms, on a second listener over the same queue.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "s1.csv"));
    final Recorder streams = new;
    Listener second = check newListener();
    Service recordStream = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobCsv(stream<Invoice, error?> rows) returns error? {
            string[] ids = [];
            check from Invoice row in rows
                do {
                    ids.push(row.id);
                };
            streams.put("records", string:'join(",", ...ids));
        }
    };
    check second.attach(recordStream);
    check second.'start();
    check await(() => streams.count("records") >= 1);
    check second.gracefulStop();
    test:assertEquals(streams.payload("records"), "inv-5");

    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "s2.csv"));
    Listener third = check newListener();
    Service rowStream = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobCsv(stream<string[], error?> rows) returns error? {
            string[] cells = [];
            check from string[] row in rows
                do {
                    cells.push(...row);
                };
            streams.put("rows", string:'join(",", ...cells));
        }
    };
    check third.attach(rowStream);
    check third.'start();
    check await(() => streams.count("rows") >= 1);
    check third.gracefulStop();
    test:assertEquals(streams.payload("rows"), "h1,h2,e,f");
}

@test:Config {}
function testListenerOnBlobDeleted() returns error? {
    EventSource src = check setupEventSource("lsn-deleted");
    check enqueueEvent(src.queue, blobDeletedEvent(src.container, "old/gone.txt"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content) returns error? {
            recorder.hit("blob");
        }

        isolated remote function onBlobDeleted(BlobEvent event, Caller caller) returns error? {
            recorder.put("deleted", event.path);
            recorder.put("eventType", event.eventType);
            recorder.put("contentLength", event.contentLength is () ? "absent" : "present");
            recorder.put("eTag", event.eTag is () ? "absent" : "present");
            byte[]|Error gone = caller->getBlob("old/gone.txt");
            recorder.put("getBlob", gone is NotFoundError ? "NotFoundError" : "other");
        }
    };
    check lsn.attach(svc);
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

// ---- routing (spec 5.5)

@test:Config {}
function testListenerRoutingByExtensionContentTypeAndFallback() returns error? {
    EventSource src = check setupEventSource("lsn-routing");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("t", "notes.TXT");
    check srcBlobs->upload("{}", "noext-json");
    check srcBlobs->upload("x", "image.png");
    check srcBlobs->upload("{}", "data.json");
    check srcBlobs->upload("a,b", "noext-csv");
    check srcBlobs->upload("plain", "noext-text");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "notes.TXT", "application/octet-stream"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "noext-json", "application/json; charset=utf-8"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "image.png", "image/png"));
    // A .json name with onBlobJson declared routes there, not to onBlob.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "data.json", "application/json"));
    // text/csv goes to onBlobCsv before the text/* rule; another text/* goes to onBlobText.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "noext-csv", "text/csv"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "noext-text", "text/markdown"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobText(string content, BlobEvent event) returns error? {
            recorder.add("text", event.path);
        }

        isolated remote function onBlobJson(json content, BlobEvent event) returns error? {
            recorder.add("json", event.path);
        }

        isolated remote function onBlobCsv(string[][] rows, BlobEvent event) returns error? {
            recorder.add("csv", event.path);
        }

        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("blob", event.path);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("text") >= 2 && recorder.count("json") >= 2
        && recorder.count("blob") >= 1 && recorder.count("csv") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.list("text").sort(), ["noext-text", "notes.TXT"], "extension match is case-insensitive");
    test:assertEquals(recorder.list("json").sort(), ["data.json", "noext-json"]);
    test:assertEquals(recorder.list("csv"), ["noext-csv"]);
    test:assertEquals(recorder.list("blob"), ["image.png"]);
}

@test:Config {}
function testListenerRoutingUndeclaredTypedHandlerFallsBackToOnBlob() returns error? {
    EventSource src = check setupEventSource("lsn-fallback");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("<x/>", "doc.xml");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "doc.xml", "application/xml"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("blob", event.path);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("blob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("blob"), "doc.xml");
}

@test:Config {}
function testListenerFunctionConfigOverridesRouting() returns error? {
    EventSource src = check setupEventSource("lsn-funcconfig");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("{}", "invoice-9.dat");
    check srcBlobs->upload("plain", "report.bin");
    check srcBlobs->upload("{}", "other.json");
    check srcBlobs->upload("both", "invoice-both.txt");
    check srcBlobs->upload("{}", "unmatched.json");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "invoice-9.dat"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "report.bin", "text/plain"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "other.json", "application/json"));
    // Matches both pattern handlers: the fixed order makes onBlobText win over onBlobJson.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "invoice-both.txt", "application/json"));
    // Matches no pattern: the extension routing applies to the pattern handler too.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "unmatched.json", "application/octet-stream"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        @FunctionConfig {namePattern: "^invoice-.*"}
        isolated remote function onBlobJson(json content, BlobEvent event) returns error? {
            recorder.add("json", event.path);
        }

        @FunctionConfig {contentTypePattern: "text/.*|application/json"}
        isolated remote function onBlobText(string content, BlobEvent event) returns error? {
            recorder.add("text", event.path);
        }

        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("blob", event.path);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("json") >= 2 && recorder.count("text") >= 3);
    check lsn.gracefulStop();
    test:assertEquals(recorder.list("json").sort(), ["invoice-9.dat", "unmatched.json"]);
    test:assertEquals(recorder.list("text").sort(), ["invoice-both.txt", "other.json", "report.bin"],
            "onBlobText's pattern wins in the fixed order, and the content type pattern routes a .bin to text");
    test:assertEquals(recorder.count("blob"), 0, "every event found a typed handler");
}

// ---- never ours (spec 5.5, 5.7, 5.8)

@test:Config {}
function testListenerNoHandlerAcknowledges() returns error? {
    EventSource src = check setupEventSource("lsn-nohandler");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "file.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin"));
    check enqueueEvent(src.queue, blobDeletedEvent(src.container, "gone.bin"));
    check enqueueEvent(src.queue, blobTierChangedEvent(src.container, "cold.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobText(string content) returns error? {
            recorder.hit("text");
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(src.queue) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("text"), 0);
    test:assertEquals(recorder.count("error"), 0, "never-ours events are acknowledged without onError");
    test:assertFalse(check queueExists(poisonQueue(src.queue)), "nothing was poisoned");
}

@test:Config {}
function testNoContentCreatedEventsAreNeverOurs() returns error? {
    EventSource src = check setupEventSource("lsn-nocontent");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "file.bin");
    // Acknowledged without dispatch: the three no-content operations and append/page creates.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "CreateFile", blobType = ()));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "SftpCreate"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "SftpWrite"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "PutBlob",
            blobType = "AppendBlob"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "PutBlob",
            blobType = "PageBlob"));
    // Dispatched: an absent or unknown blob type, a copy of an append blob, the commits.
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "PutBlob", blobType = ()));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "PutBlob",
            blobType = "Mystery"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "CopyBlob",
            blobType = "AppendBlob"));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "FlushWithClose",
            blobType = ()));
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "file.bin", api = "SftpCommit"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("api", event.api);
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(src.queue) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.list("api").sort(), ["CopyBlob", "FlushWithClose", "PutBlob", "PutBlob", "SftpCommit"]);
    test:assertEquals(recorder.count("error"), 0);
}

@test:Config {}
function testDataLakeUrlsAreGivenOnTheBlobEndpoint() returns error? {
    EventSource src = check setupEventSource("lsn-dfs");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("x", "dir/file.bin");
    string dfsUrl = string `https://${accountName()}.dfs.core.windows.net/${src.container}/dir/file.bin`;
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "dir/file.bin", api = "FlushWithClose",
            blobType = (), url = dfsUrl));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.put("url", event.url);
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("url") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("url"),
            string `https://${accountName()}.blob.core.windows.net/${src.container}/dir/file.bin`);
}

@test:Config {}
function testEventsOfAnotherAccountAreAcknowledged() returns error? {
    EventSource src = check setupEventSource("lsn-foreign");
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("ours", "ours.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "ours.bin", topic = storageTopic("otheracct")));
    // The comparison ignores case.
    string upper = accountName().toUpperAscii();
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "ours.bin", topic = storageTopic(upper)));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("account", event.accountName);
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => approximateMessageCount(src.queue) == 0);
    check lsn.gracefulStop();
    test:assertEquals(recorder.list("account"), [upper], "only this account's event is dispatched");
    test:assertEquals(recorder.count("error"), 0, "a foreign event is acknowledged, not reported to onError");
    test:assertFalse(check queueExists(poisonQueue(src.queue)), "nothing was poisoned");
}

// ---- the Caller (spec 5.13)

@test:Config {}
function testOneCallerPerContainerOnOneQueue() returns error? {
    EventSource src = check setupEventSource("lsn-containers");
    Client srcBlobs = src.blobs;
    string other = testContainer("lsn-containers-other");
    AdminClient admin = check newAdmin();
    check createTestContainer(admin, other);
    check srcBlobs->upload("first", "n.bin");
    Client otherClient = check newContainerClient(other);
    check otherClient->upload("second!", "o.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "n.bin"));
    check enqueueEvent(src.queue, blobCreatedEvent(other, "o.bin"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            // The Caller is bound to the event's container: it reads the event's blob.
            BlobProperties props = check caller->getBlobProperties(event.path);
            recorder.add("sizes", event.containerName + "=" + props.contentLength.toString());
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("sizes") >= 2);
    check lsn.gracefulStop();
    string[] expected = [src.container + "=5", other + "=7"];
    test:assertEquals(recorder.list("sizes").sort(), expected.sort());
}

@test:Config {}
function testCallerOperations() returns error? {
    EventSource src = check setupEventSource("lsn-caller");
    Client srcBlobs = src.blobs;
    Invoice invoice = {id: "inv-7", total: 70.0d};
    check srcBlobs->upload(invoice, "in/inv-7.json");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "in/inv-7.json", "application/json"));

    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlobJson(Invoice content, BlobEvent event, Caller caller) returns error? {
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
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("done") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("getBlob"), "inv-7");
    test:assertEquals(recorder.payload("contentType"), "application/json");
    test:assertEquals(recorder.payload("copy"), "success");
    boolean done = check srcBlobs->hasBlob("done/inv-7.json");
    boolean archived = check srcBlobs->hasBlob("archive/inv-7.json");
    boolean original = check srcBlobs->hasBlob("in/inv-7.json");
    test:assertTrue(done);
    test:assertTrue(archived);
    test:assertFalse(original);
}

// ---- the credential forms the listener documentation names (spec 2.1, 5.2)

// Drives one created event through a listener built over the given credential.
function assertListenerDispatches(string base, AuthConfig auth) returns error? {
    EventSource src = check setupEventSource(base);
    Client srcBlobs = src.blobs;
    check srcBlobs->upload("via " + base, "cred.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "cred.bin"));
    final Recorder recorder = new;
    Listener lsn = check newListenerWith(auth);
    final string queue = src.queue;
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event, Caller caller) returns error? {
            recorder.put("blob", check string:fromBytes(content));
            // The Caller shares the listener's credential; a write proves it covers the blob service.
            check caller->setTags(event.path, {seen: "yes"});
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("blob") >= 1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.payload("blob"), "via " + base);
    map<string> tags = check srcBlobs->getTags("cred.bin");
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

// A listener over a SAS URL on the blob host derives the queue endpoint from it; Azurite's SAS
// URL carries no service label, so this form runs live only.
@test:Config {enable: liveRun}
function testListenerOverSasUrlDerivesQueueEndpoint() returns error? {
    string sas = check queueSas();
    string sasUrl = string `https://${liveAccountName}.blob.core.windows.net/?${sas}`;
    return assertListenerDispatches("lsn-sasurl", {sasUrl});
}

// The subscription filters, not the listener, keep a handler's own output out of its queue;
// the listener's part is to dispatch what arrives, so a handler writing elsewhere is unaffected.
@test:Config {}
function testHandlerWritesOutsideTheContainerFireNothingHere() returns error? {
    EventSource src = check setupEventSource("lsn-own-output");
    Client srcBlobs = src.blobs;
    string archive = testContainer("lsn-own-output-archive");
    AdminClient admin = check newAdmin();
    check createTestContainer(admin, archive);
    check srcBlobs->upload("x", "in.bin");
    check enqueueEvent(src.queue, blobCreatedEvent(src.container, "in.bin"));
    final Recorder recorder = new;
    Listener lsn = check newListener();
    final string queue = src.queue;
    final Client archiveClient = check newContainerClient(archive);
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            check archiveClient->upload(content, event.path);
            recorder.hit("blob");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    check await(() => recorder.count("blob") >= 1);
    runtime:sleep(1);
    check lsn.gracefulStop();
    test:assertEquals(recorder.count("blob"), 1);
    boolean copied = check archiveClient->hasBlob("in.bin");
    test:assertTrue(copied);
}
