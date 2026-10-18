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

// The listener tests' view of the queue service. The connector exposes no queue client, so the
// suite talks to the queue REST API directly (over the account SAS the listener documentation
// asks for), to create the queues the Event Grid subscription would otherwise fill, to put
// synthetic events on them, and to inspect what the listener left behind.

import ballerina/http;
import ballerina/test;
import ballerina/time;

const string AZURITE_QUEUE_URL = "http://127.0.0.1:10001/devstoreaccount1";

// The queue service endpoint of the active backend.
isolated function queueServiceUrl() returns string => liveRun
    ? string `https://${liveAccountName}.queue.core.windows.net`
    : AZURITE_QUEUE_URL;

// The storage account the active backend serves; the listener's account check compares every
// event's account with it, so the synthetic events name it (spec 5.8).
isolated function accountName() returns string => liveRun ? liveAccountName : AZURITE_ACCOUNT;

// The Event Grid topic (or CloudEvents source) of a storage account's system topic.
isolated function storageTopic(string account = accountName()) returns string =>
    string `/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/${account}`;

// The queue name a test works in; per-run unique like the container names.
isolated function testQueue(string base) returns string => string `${containerPrefix}-${base}`;

// The poison queue a service without an explicit poisonQueueName moves its failures to.
isolated function poisonQueue(string queue) returns string => queue + "-poison";

// Listener factory for the current backend. The consumption settings live on each service.
isolated function newListener(boolean laxDataBinding = false) returns Listener|Error {
    return newListenerWith(sharedKeyAuth(), laxDataBinding);
}

// A listener over an explicit credential, for the tests of the documented credential forms.
isolated function newListenerWith(AuthConfig auth, boolean laxDataBinding = false) returns Listener|Error {
    ListenerConfiguration config = {auth, laxDataBinding};
    if !liveRun {
        config.queueServiceUrl = AZURITE_QUEUE_URL;
    }
    return new (config);
}

// An account SAS spanning the queue and blob services, minted once per run by the connector's
// own AdminClient: it authorizes every REST call below.
isolated string? queueSasCache = ();

isolated function queueSas() returns string|error {
    lock {
        string? cached = queueSasCache;
        if cached is string {
            return cached;
        }
    }
    string sas = check mintAccountSas({read: true, write: true, delete: true, list: true, add: true, create: true,
        update: true, process: true, tag: true, filter: true});
    lock {
        queueSasCache = sas;
    }
    return sas;
}

// An account SAS over both services with the given permissions, for the tests of what a
// listener can do with less than the documented set.
isolated function mintAccountSas(AccountSasPermissions permissions) returns string|error {
    AdminClient admin = check newAdmin();
    return admin.generateAccountSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 3600),
        permissions,
        services: {blob: true, queue: true},
        resourceTypes: {'service: true, container: true, 'object: true}
    });
}

isolated function queueHttp() returns http:Client|error => new (queueServiceUrl());

// Creates a queue, tolerating one that already exists.
function createQueue(string queue) returns error? {
    http:Client queues = check queueHttp();
    http:Response response = check queues->put(string `/${queue}?${check queueSas()}`, ());
    if response.statusCode != 201 && response.statusCode != 204 {
        return error(string `queue ${queue} could not be created: HTTP ${response.statusCode}`);
    }
}

function deleteQueue(string queue) returns error? {
    http:Client queues = check queueHttp();
    http:Response response = check queues->delete(string `/${queue}?${check queueSas()}`);
    if response.statusCode != 204 && response.statusCode != 404 {
        return error(string `queue ${queue} could not be deleted: HTTP ${response.statusCode}`);
    }
}

// Puts one message on the queue, as the Event Grid subscription would: base64-encoded JSON
// by default, or the raw text when the test probes that form. A time to live shorter than
// the service's seven-day default models a subscription's message time to live setting.
function enqueueMessage(string queue, string text, int? ttlSeconds = ()) returns error? {
    http:Client queues = check queueHttp();
    xml body = xml `<QueueMessage><MessageText>${text}</MessageText></QueueMessage>`;
    string ttl = ttlSeconds is int ? string `messagettl=${ttlSeconds}&` : "";
    http:Response response = check queues->post(string `/${queue}/messages?${ttl}${check queueSas()}`, body);
    if response.statusCode != 201 {
        return error(string `message could not be enqueued on ${queue}: HTTP ${response.statusCode}`);
    }
}

function enqueueEvent(string queue, json event, boolean encode = true, int? ttlSeconds = ()) returns error? {
    string text = event.toJsonString();
    return enqueueMessage(queue, encode ? text.toBytes().toBase64() : text, ttlSeconds);
}

isolated function encoded(json event) returns string => event.toJsonString().toBytes().toBase64();

// The message texts currently visible on the queue, without dequeuing them.
function peekMessages(string queue) returns string[]|error {
    return from PeekedMessage message in check peekDetails(queue)
        select message.text;
}

// A peeked message: its text and how often it has been dequeued.
type PeekedMessage record {|
    string text;
    int dequeueCount;
|};

function peekDetails(string queue) returns PeekedMessage[]|error {
    http:Client queues = check queueHttp();
    xml listing = check queues->get(string `/${queue}/messages?peekonly=true&numofmessages=32&${check queueSas()}`);
    PeekedMessage[] messages = [];
    foreach xml item in listing/<QueueMessage> {
        messages.push({
            text: (item/<MessageText>).data(),
            dequeueCount: check int:fromString((item/<DequeueCount>).data())
        });
    }
    return messages;
}

// Receives and releases the queue's messages, so each one's dequeue count rises by one per
// round, as a consumer that never settles would leave them.
function driveDequeueCount(string queue, int rounds) returns error? {
    http:Client queues = check queueHttp();
    foreach int round in 0 ..< rounds {
        xml received = check queues->get(
                string `/${queue}/messages?numofmessages=32&visibilitytimeout=30&${check queueSas()}`);
        foreach xml item in received/<QueueMessage> {
            string id = (item/<MessageId>).data();
            // The pop receipt is base64, so its +, / and = must be encoded in the query.
            string receipt = re `\+`.replaceAll((item/<PopReceipt>).data(), "%2B");
            receipt = re `/`.replaceAll(receipt, "%2F");
            receipt = re `=`.replaceAll(receipt, "%3D");
            xml body = xml `<QueueMessage><MessageText>${(item/<MessageText>).data()}</MessageText></QueueMessage>`;
            http:Response response = check queues->put(
                    string `/${queue}/messages/${id}?popreceipt=${receipt}&visibilitytimeout=0&${check queueSas()}`,
                    body);
            if response.statusCode != 204 {
                return error(string `message ${id} could not be released: HTTP ${response.statusCode}`);
            }
        }
    }
}

// Whether a queue exists, for the tests that assert a poison queue was never created.
function queueExists(string queue) returns boolean|error {
    http:Client queues = check queueHttp();
    http:Response response = check queues->get(string `/${queue}?comp=metadata&${check queueSas()}`);
    if response.statusCode == 200 {
        return true;
    }
    if response.statusCode == 404 {
        return false;
    }
    return error(string `queue ${queue} metadata could not be read: HTTP ${response.statusCode}`);
}

// The service's approximate message count, which counts invisible messages too.
function approximateMessageCount(string queue) returns int|error {
    http:Client queues = check queueHttp();
    http:Response response = check queues->get(string `/${queue}?comp=metadata&${check queueSas()}`);
    if response.statusCode != 200 {
        return error(string `queue ${queue} metadata could not be read: HTTP ${response.statusCode}`);
    }
    return int:fromString(check response.getHeader("x-ms-approximate-messages-count"));
}

// Every queue this run created, by prefix, for the end-of-suite cleanup.
function listQueues() returns string[]|error {
    http:Client queues = check queueHttp();
    xml listing = check queues->get(string `/?comp=list&prefix=${containerPrefix}&${check queueSas()}`);
    string[] names = [];
    foreach xml queue in listing/<Queues>/<Queue> {
        names.push((queue/<Name>).data());
    }
    return names;
}

@test:AfterSuite
function cleanUpQueues() returns error? {
    foreach string queue in check listQueues() {
        check deleteQueue(queue);
    }
}

// Event Grid-shaped messages for a blob of the active backend. The subject and the data
// fields follow the published Blob Storage event samples; the url is the blob's real URL,
// so a handler's Caller and the listener's fetch both resolve it.
isolated function blobUrl(string container, string path) returns string => liveRun
    ? string `https://${liveAccountName}.blob.core.windows.net/${container}/${path}`
    : string `${AZURITE_URL}/${container}/${path}`;

isolated function eventData(string container, string path, string api, string? contentType,
        int? contentLength, string? blobType = "BlockBlob", string? url = ()) returns map<json> {
    map<json> data = {
        api,
        clientRequestId: "6d79dbfb-0e37-4fc4-981f-442c9ca65760",
        requestId: "831e1650-001e-001b-66ab-eeb76e000000",
        url: url ?: blobUrl(container, path),
        sequencer: "00000000000004420000000000028963",
        storageDiagnostics: {batchId: "b68529f3-68cd-4744-baa4-3c0498ec19f0"}
    };
    if blobType is string {
        data["blobType"] = blobType;
    }
    if contentType is string {
        data["contentType"] = contentType;
        data["eTag"] = "0x8D4BCC2E4835CD0";
    }
    if contentLength is int {
        data["contentLength"] = contentLength;
    }
    return data;
}

// The Event Grid schema form of a BlobCreated event.
isolated function blobCreatedEvent(string container, string path, string contentType = "application/octet-stream",
        int contentLength = 0, string api = "PutBlob", string? blobType = "BlockBlob", string? topic = (),
        string? url = ()) returns json => {
    topic: topic ?: storageTopic(),
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    eventType: "Microsoft.Storage.BlobCreated",
    eventTime: "2017-06-26T18:41:00.9584103Z",
    id: "831e1650-001e-001b-66ab-eeb76e069631",
    data: eventData(container, path, api, contentType, contentLength, blobType, url),
    dataVersion: "",
    metadataVersion: "1"
};

// A created event with a distinct id and sequencer, for the tests that enqueue several.
isolated function numberedCreatedEvent(string container, string path, int number) returns json {
    map<json> event = <map<json>>blobCreatedEvent(container, path);
    event["id"] = string `evt-${number}`;
    map<json> data = <map<json>>event["data"];
    data["sequencer"] = string `seq-${number}`;
    return event;
}

// The CloudEvents schema form of a BlobCreated event.
isolated function blobCreatedCloudEvent(string container, string path,
        string contentType = "application/octet-stream", int contentLength = 0, string? 'source = ()) returns json => {
    "source": 'source ?: storageTopic(),
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    'type: "Microsoft.Storage.BlobCreated",
    time: "2017-06-26T18:41:00.9584103Z",
    id: "831e1650-001e-001b-66ab-eeb76e069631",
    data: eventData(container, path, "PutBlockList", contentType, contentLength),
    specversion: "1.0"
};

// A BlobDeleted event carries the content type but neither the eTag nor the content length.
isolated function blobDeletedEvent(string container, string path, string? topic = ()) returns json {
    map<json> data = eventData(container, path, "DeleteBlob", "text/plain", ());
    _ = data.remove("eTag");
    return {
        topic: topic ?: storageTopic(),
        subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
        eventType: "Microsoft.Storage.BlobDeleted",
        eventTime: "2017-11-07T20:09:22.5674003Z",
        id: "4c2359fe-001e-00ba-0e04-58586806d298",
        data,
        dataVersion: "",
        metadataVersion: "1"
    };
}

isolated function blobTierChangedEvent(string container, string path) returns json => {
    topic: storageTopic(),
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    eventType: "Microsoft.Storage.BlobTierChanged",
    eventTime: "2021-05-04T15:00:00.8350154Z",
    id: "0fdefc06-b01e-0034-39f6-4016610696f6",
    data: eventData(container, path, "SetBlobTier", "application/octet-stream", 0),
    dataVersion: "",
    metadataVersion: "1"
};

// The test's container and queue, created, with a client bound to the container.
type EventSource record {|
    Client blobs;
    string container;
    string queue;
|};

function setupEventSource(string base) returns EventSource|error {
    string container = testContainer(base);
    string queue = testQueue(base);
    AdminClient admin = check newAdmin();
    check createTestContainer(admin, container);
    check createQueue(queue);
    return {blobs: check newContainerClient(container), container, queue};
}

// ---- the credential forms the listener documentation names

// Azurite's published connection string, carrying both service endpoints; the live form is the
// portal's, which names only the account and lets the endpoints derive.
isolated function connectionStringAuth() returns ConnectionStringConfig => liveRun
    ? {connectionString: string `DefaultEndpointsProtocol=https;AccountName=${liveAccountName};`
        + string `AccountKey=${liveAccountKey};EndpointSuffix=core.windows.net`}
    : {connectionString: string `DefaultEndpointsProtocol=http;AccountName=${AZURITE_ACCOUNT};`
        + string `AccountKey=${AZURITE_KEY};BlobEndpoint=${AZURITE_URL};QueueEndpoint=${AZURITE_QUEUE_URL}`};

// An account SAS spanning the queue and blob services, in the form a listener takes: a bare
// token with the account name, or Azurite's SAS URL (whose host carries no service label, so
// the queue endpoint is given explicitly).
isolated function accountSasAuth(string sas) returns AuthConfig => liveRun
    ? {accountName: liveAccountName, sasToken: sas}
    : {sasUrl: string `${AZURITE_URL}?${sas}`};

// ---- recording what the services saw

// Records handler dispatches so a test can await and assert them across the listener's
// dispatch threads: counts and the last payload per key, and how many handlers overlapped.
isolated class Recorder {
    private final map<int> hits = {};
    private final map<string> payloads = {};
    private final map<string[]> lists = {};
    private int running = 0;
    private int maxRunning = 0;

    isolated function hit(string key) {
        lock {
            self.hits[key] = (self.hits[key] ?: 0) + 1;
        }
    }

    // Counts a hit and returns the new count.
    isolated function next(string key) returns int {
        lock {
            int count = (self.hits[key] ?: 0) + 1;
            self.hits[key] = count;
            return count;
        }
    }

    isolated function put(string key, string payload) {
        lock {
            self.hits[key] = (self.hits[key] ?: 0) + 1;
            self.payloads[key] = payload;
        }
    }

    isolated function add(string key, string payload) {
        lock {
            string[] values = self.lists[key] ?: [];
            values.push(payload);
            self.lists[key] = values;
            self.hits[key] = (self.hits[key] ?: 0) + 1;
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

    isolated function list(string key) returns string[] {
        lock {
            return (self.lists[key] ?: []).clone();
        }
    }

    // Marks a handler's start, returning how many run at the same time, it included.
    isolated function enter() returns int {
        lock {
            self.running += 1;
            if self.running > self.maxRunning {
                self.maxRunning = self.running;
            }
            return self.running;
        }
    }

    isolated function exit() {
        lock {
            self.running -= 1;
        }
    }

    isolated function maxConcurrent() returns int {
        lock {
            return self.maxRunning;
        }
    }
}

// What an onError call received, flattened for assertions.
isolated function describeNotification(Error err, BlobEvent? event, Caller? caller) returns string {
    string kind = err is ContentBindingError ? "ContentBindingError"
        : err is NotFoundError ? "NotFoundError"
        : err is AuthorizationError ? "AuthorizationError"
        : err is ArchivedBlobError ? "ArchivedBlobError"
        : err is ServiceError ? "ServiceError" : "Error";
    return string `${kind}|${event is BlobEvent ? event.path : "()"}|${caller is Caller ? "caller" : "()"}`;
}

type Invoice record {|
    string id;
    decimal total;
|};

type Contact record {|
    string name;
    string? email;
|};

type Named record {|
    string name;
|};
