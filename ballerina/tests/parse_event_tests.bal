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

import ballerina/lang.array;
import ballerina/test;

// `parseEvent` (spec 5.14) is a pure function, so these tests need no backend. The inputs are
// the same synthetic events the listener tests enqueue.

// The event a message must parse to; `()` and errors fail the test.
isolated function expectEvent(BlobEvent|Error? parsed) returns BlobEvent|error {
    if parsed is BlobEvent {
        return parsed;
    }
    return parsed is Error ? parsed : error("expected a blob event, got ()");
}

@test:Config {}
function testParseEventAcceptsEveryInputForm() returns error? {
    json event = blobCreatedEvent("invoices", "2026/a.json", "application/json", 42, "PutBlockList");
    BlobEvent fromJson = check expectEvent(parseEvent(event));
    BlobEvent fromString = check expectEvent(parseEvent(event.toJsonString()));
    BlobEvent fromBytes = check expectEvent(parseEvent(event.toJsonString().toBytes()));
    BlobEvent fromBase64 = check expectEvent(parseEvent(array:toBase64(event.toJsonString().toBytes())));
    BlobEvent fromArray = check expectEvent(parseEvent([event]));
    foreach BlobEvent parsed in [fromJson, fromString, fromBytes, fromBase64, fromArray] {
        test:assertEquals(parsed.eventType, BLOB_CREATED);
        test:assertEquals(parsed.accountName, accountName());
        test:assertEquals(parsed.containerName, "invoices");
        test:assertEquals(parsed.path, "2026/a.json");
        test:assertEquals(parsed.api, "PutBlockList");
        test:assertEquals(parsed.contentLength, 42);
        test:assertEquals(parsed.sequencer, "00000000000004420000000000028963");
    }
}

@test:Config {}
function testParseEventReadsBothSchemasAndDeletes() returns error? {
    BlobEvent cloud = check expectEvent(parseEvent(blobCreatedCloudEvent("c", "x.txt", "text/plain", 3)));
    test:assertEquals(cloud.accountName, accountName());
    test:assertEquals(cloud.api, "PutBlockList");
    BlobEvent deleted = check expectEvent(parseEvent(blobDeletedEvent("c", "gone.txt")));
    test:assertEquals(deleted.eventType, BLOB_DELETED);
    test:assertTrue(deleted.contentLength is ());
    test:assertTrue(deleted.eTag is ());
}

@test:Config {}
function testParseEventReturnsNilForOtherTypesAndNoContentCreates() returns error? {
    test:assertTrue(check parseEvent(blobTierChangedEvent("c", "x")) is ());
    // Operations that commit no content, and append or page blob creates (spec 5.7).
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "CreateFile", blobType = ())) is ());
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "SftpCreate")) is ());
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "SftpWrite")) is ());
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "a.log", blobType = "AppendBlob")) is ());
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "d.vhd", blobType = "PageBlob")) is ());
    // An absent or unknown blob type, a copy of an append blob, and the commit operations dispatch.
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", blobType = ())) is BlobEvent);
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", blobType = "Mystery")) is BlobEvent);
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "CopyBlob", blobType = "AppendBlob")) is BlobEvent);
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "FlushWithClose", blobType = ())) is BlobEvent);
    test:assertTrue(check parseEvent(blobCreatedEvent("c", "f", api = "SftpCommit")) is BlobEvent);
}

@test:Config {}
function testParseEventRejectsMalformedAndRelayedMessages() {
    test:assertTrue(parseEvent("not an event") is Error);
    test:assertTrue(parseEvent(42) is Error);
    test:assertTrue(parseEvent(()) is Error);
    json[] none = [];
    test:assertTrue(parseEvent(none) is Error);
    json[] two = [blobCreatedEvent("c", "a"), blobCreatedEvent("c", "b")];
    test:assertTrue(parseEvent(two) is Error);
    // A blob event must name its storage account: a custom-topic relay in the Event Grid schema
    // carries the custom topic instead, and so is unparseable (spec 5.8).
    string relay = "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.EventGrid/topics/relay";
    test:assertTrue(parseEvent(blobCreatedEvent("c", "a", topic = relay)) is Error);
    map<json> noTopic = <map<json>>blobCreatedEvent("c", "a");
    _ = noTopic.remove("topic");
    test:assertTrue(parseEvent(noTopic) is Error);
    // A CloudEvents relay that keeps the original source parses normally.
    BlobEvent|Error? kept = parseEvent(blobCreatedCloudEvent("c", "a", 'source = storageTopic()));
    test:assertTrue(kept is BlobEvent);
}

@test:Config {}
function testParseEventNormalisesDataLakeUrls() returns error? {
    string dfsUrl = string `https://${accountName()}.dfs.core.windows.net/invoices/2026/a.json`;
    BlobEvent event = check expectEvent(parseEvent(blobCreatedEvent("invoices", "2026/a.json",
            api = "FlushWithClose", blobType = (), url = dfsUrl)));
    test:assertEquals(event.url, string `https://${accountName()}.blob.core.windows.net/invoices/2026/a.json`);
}
