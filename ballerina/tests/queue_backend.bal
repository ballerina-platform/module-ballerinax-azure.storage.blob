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
import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;

const string AZURITE_QUEUE_URL = "http://127.0.0.1:10001/devstoreaccount1";

// The queue service endpoint of the active backend.
isolated function queueServiceUrl() returns string => liveRun
    ? string `https://${liveAccountName}.queue.core.windows.net`
    : AZURITE_QUEUE_URL;

// The queue name a test works in; per-run unique like the container names.
isolated function testQueue(string base) returns string => string `${containerPrefix}-${base}`;

// Listener factory for the current backend, with the handling knobs a test may tune.
isolated function newListener(string queue, int maxDeliveryCount = 5, boolean laxDataBinding = false)
        returns Listener|Error {
    ListenerConfiguration config = {auth: sharedKeyAuth(), maxDeliveryCount, laxDataBinding};
    if !liveRun {
        config.queueServiceUrl = AZURITE_QUEUE_URL;
    }
    return new (queue, config);
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
    AdminClient admin = check newAdmin();
    string sas = check admin.generateAccountSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 3600),
        permissions: {read: true, write: true, delete: true, list: true, add: true, create: true,
            update: true, process: true},
        services: {blob: true, queue: true},
        resourceTypes: {'service: true, container: true, 'object: true}
    });
    lock {
        queueSasCache = sas;
    }
    return sas;
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
// by default, or the raw text when the test probes that form.
function enqueueMessage(string queue, string text) returns error? {
    http:Client queues = check queueHttp();
    xml body = xml `<QueueMessage><MessageText>${text}</MessageText></QueueMessage>`;
    http:Response response = check queues->post(string `/${queue}/messages?${check queueSas()}`, body);
    if response.statusCode != 201 {
        return error(string `message could not be enqueued on ${queue}: HTTP ${response.statusCode}`);
    }
}

function enqueueEvent(string queue, json event, boolean encode = true) returns error? {
    string text = event.toJsonString();
    return enqueueMessage(queue, encode ? text.toBytes().toBase64() : text);
}

// The message texts currently visible on the queue, without dequeuing them.
function peekMessages(string queue) returns string[]|error {
    http:Client queues = check queueHttp();
    xml listing = check queues->get(string `/${queue}/messages?peekonly=true&numofmessages=32&${check queueSas()}`);
    string[] texts = [];
    foreach xml item in listing/<QueueMessage> {
        texts.push((item/<MessageText>).data());
    }
    return texts;
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

// Polls until the probe reports true, for the dispatch a listener performs asynchronously.
function await(function () returns boolean|error probe, decimal timeoutSeconds = 30,
        decimal intervalSeconds = 0.25) returns error? {
    decimal waited = 0;
    while true {
        boolean|error met = probe();
        if met is boolean && met {
            return;
        }
        if waited >= timeoutSeconds {
            if met is error {
                return met;
            }
            return error(string `condition not met within ${timeoutSeconds}s`);
        }
        runtime:sleep(intervalSeconds);
        waited += intervalSeconds;
    }
}

// Event Grid-shaped messages for a blob of the active backend. The subject and the data
// fields follow the published Blob Storage event samples; the url is the blob's real URL,
// so a handler's Caller and the listener's fetch both resolve it.
isolated function blobUrl(string container, string path) returns string => liveRun
    ? string `https://${liveAccountName}.blob.core.windows.net/${container}/${path}`
    : string `${AZURITE_URL}/${container}/${path}`;

isolated function eventData(string container, string path, string api, string? contentType,
        int? contentLength) returns map<json> {
    map<json> data = {
        api,
        clientRequestId: "6d79dbfb-0e37-4fc4-981f-442c9ca65760",
        requestId: "831e1650-001e-001b-66ab-eeb76e000000",
        blobType: "BlockBlob",
        url: blobUrl(container, path),
        sequencer: "00000000000004420000000000028963",
        storageDiagnostics: {batchId: "b68529f3-68cd-4744-baa4-3c0498ec19f0"}
    };
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
        int contentLength = 0, string api = "PutBlob") returns json => {
    topic: "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    eventType: "Microsoft.Storage.BlobCreated",
    eventTime: "2017-06-26T18:41:00.9584103Z",
    id: "831e1650-001e-001b-66ab-eeb76e069631",
    data: eventData(container, path, api, contentType, contentLength),
    dataVersion: "",
    metadataVersion: "1"
};

// The CloudEvents schema form of a BlobCreated event.
isolated function blobCreatedCloudEvent(string container, string path,
        string contentType = "application/octet-stream", int contentLength = 0) returns json => {
    "source": "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    'type: "Microsoft.Storage.BlobCreated",
    time: "2017-06-26T18:41:00.9584103Z",
    id: "831e1650-001e-001b-66ab-eeb76e069631",
    data: eventData(container, path, "PutBlockList", contentType, contentLength),
    specversion: "1.0"
};

// A BlobDeleted event carries the content type but neither the eTag nor the content length.
isolated function blobDeletedEvent(string container, string path) returns json {
    map<json> data = eventData(container, path, "DeleteBlob", "text/plain", ());
    _ = data.remove("eTag");
    return {
        topic: "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
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
    topic: "/subscriptions/{subscription-id}/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
    subject: string `/blobServices/default/containers/${container}/blobs/${path}`,
    eventType: "Microsoft.Storage.BlobTierChanged",
    eventTime: "2021-05-04T15:00:00.8350154Z",
    id: "0fdefc06-b01e-0034-39f6-4016610696f6",
    data: eventData(container, path, "SetBlobTier", "application/octet-stream", 0),
    dataVersion: "",
    metadataVersion: "1"
};

// Creates the test's container and queue, and returns a client bound to the container.
function setupEventSource(string base) returns [Client, string, string]|error {
    string container = testContainer(base);
    string queue = testQueue(base);
    AdminClient admin = check newAdmin();
    check createTestContainer(admin, container);
    check createQueue(queue);
    return [check newContainerClient(container), container, queue];
}
