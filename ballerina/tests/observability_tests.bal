// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/file;
import ballerina/http;
import ballerina/test;
import ballerinax/prometheus as _;

// The suite runs with metrics enabled and the Prometheus reporter on this port (tests/Config.toml),
// so the metrics the connector records can be read back as the scrape page shows them: counters
// and gauges carry a `_value` suffix, and the dots in tag keys become underscores.
const string METRICS_ENDPOINT = "http://127.0.0.1:9797";
const string EVENTS = "file_events_total_value";
const string BYTES = "file_bytes_transferred_total_value";
const string CONNECTIONS = "file_active_connections_value";
const string REQUESTS = "requests_total_value";
const string DATABINDING_DURATION = "file_databinding_duration_seconds_value";
const string EXECUTION_DURATION = "file_resource_execution_duration_seconds_value";

final http:Client metricsClient = check new (METRICS_ENDPOINT);

type Series record {|
    string name;
    map<string> labels;
    float value;
|};

function scrapeMetrics() returns Series[]|error {
    string page = check metricsClient->get("/metrics");
    Series[] all = [];
    foreach string line in re `\n`.split(page) {
        if line.startsWith("#") || line.trim() == "" {
            continue;
        }
        int? open = line.indexOf("{");
        int? close = line.lastIndexOf("}");
        string name;
        map<string> labels = {};
        string rest;
        if open is int && close is int {
            name = line.substring(0, open);
            foreach string pair in re `,`.split(line.substring(open + 1, close)) {
                int? eq = pair.indexOf("=");
                if eq is int && pair.length() > eq + 2 {
                    labels[pair.substring(0, eq)] = pair.substring(eq + 2, pair.length() - 1);
                }
            }
            rest = line.substring(close + 1).trim();
        } else {
            int? space = line.indexOf(" ");
            if space is () {
                continue;
            }
            name = line.substring(0, space);
            rest = line.substring(space + 1).trim();
        }
        float|error value = float:fromString(rest);
        if value is float {
            all.push({name, labels, value});
        }
    }
    return all;
}

// The sum of a metric over the series carrying every given label.
isolated function total(Series[] all, string name, map<string> labels) returns float {
    float sum = 0.0;
    foreach Series series in all {
        if series.name != name {
            continue;
        }
        boolean matches = true;
        foreach [string, string] [key, value] in labels.entries() {
            if series.labels[key] != value {
                matches = false;
                break;
            }
        }
        if matches {
            sum += series.value;
        }
    }
    return sum;
}

// Every series of a metric carries the same label keys (the standard's consistency rule), checked
// against the exact set the specification names; a stray or missing key fails here.
function assertLabelKeys(Series[] all, string name, string[] keys) {
    Series[] series = all.filter(s => s.name == name);
    test:assertTrue(series.length() > 0, string `no ${name} series to check`);
    foreach Series s in series {
        test:assertEquals(s.labels.keys().sort(), keys.clone().sort(), string `label keys of ${name}`);
    }
}

isolated function delta(Series[] before, Series[] after, string name, map<string> labels) returns float =>
    total(after, name, labels) - total(before, name, labels);

isolated function expectedRemoteUrl() returns string => liveRun
    ? string `${liveAccountName}.blob.core.windows.net` : "127.0.0.1:10000";

isolated function expectedProtocol() returns string => liveRun ? "https" : "http";

isolated function identity(string context) returns map<string> => {
    module: "azure_blob",
    'type: context,
    remote_url: expectedRemoteUrl(),
    protocol: expectedProtocol()
};

// The Prometheus page writes a hyphen in a label value as an underscore.
isolated function labelValue(string value) returns string => re `-`.replaceAll(value, "_");

isolated function withLabels(map<string> base, map<string> more) returns map<string> {
    map<string> merged = base.clone();
    foreach [string, string] [key, value] in more.entries() {
        merged[key] = value;
    }
    return merged;
}

@test:Config {groups: ["observability"]}
function testClientInitRaisesTheConnectionGauge() returns error? {
    Series[] before = check scrapeMetrics();
    Client _ = check newContainerClient(testContainer("obs-gauge"));
    AdminClient _ = check newAdmin();
    Series[] after = check scrapeMetrics();
    test:assertEquals(delta(before, after, CONNECTIONS, identity("client")), 2.0,
            "each client init raises the connection gauge under the blob endpoint's identity");
}

@test:Config {groups: ["observability"]}
function testTransfersCountBytesByOperationType() returns error? {
    string container = testContainer("obs-bytes");
    check createTestContainer(check newAdmin(), container);
    Client blobs = check newContainerClient(container);
    // Crosses the 64 KiB chunk size of the stream form, so the stream is counted chunk by chunk.
    int size = 70000;
    byte[] content = [];
    content.setLength(size);
    string dir = check file:createTempDir();
    string local = check file:joinPath(dir, "bytes.bin");

    Series[] before = check scrapeMetrics();
    check blobs->upload(content, "bytes.bin");
    byte[] fetched = check blobs->getBlob("bytes.bin");
    stream<byte[], Error?> chunks = check blobs->getBlob("bytes.bin");
    int streamed = 0;
    record {|byte[] value;|}? next = check chunks.next();
    while next is record {|byte[] value;|} {
        streamed += next.value.length();
        next = check chunks.next();
    }
    check blobs->download("bytes.bin", local);
    check blobs->uploadFromFile(local, "bytes-copy.bin");
    _ = check blobs->getBlobProperties("bytes.bin");
    Series[] after = check scrapeMetrics();

    test:assertEquals(fetched.length(), size);
    test:assertEquals(streamed, size);
    map<string> clientIdentity = identity("client");
    test:assertEquals(delta(before, after, BYTES, withLabels(clientIdentity, {operation_type: "put"})),
            <float>(2 * size),
            "the upload and the file upload count their bytes as put");
    test:assertEquals(delta(before, after, BYTES, withLabels(clientIdentity, {operation_type: "get"})),
            <float>(3 * size),
            "the typed read, the stream, and the download count their bytes as get");
    map<string> operation = withLabels(clientIdentity, {action_type: "client_operation", outcome: "success"});
    test:assertEquals(delta(before, after, REQUESTS, withLabels(operation, {operation_type: "put"})), 2.0,
            "every put operation's own span carries the connector's tags");
    test:assertEquals(delta(before, after, REQUESTS, withLabels(operation, {operation_type: "get"})), 4.0,
            "every get operation's own span carries the connector's tags, property reads included");
    test:assertTrue(after.filter(s => s.name == REQUESTS && s.labels["module"] == "azure_blob")
            .every(s => s.labels.hasKey("host")), "every client operation series carries the host");
}

@test:Config {groups: ["observability"]}
function testFailedOperationSpansCarryTheErrorType() returns error? {
    Client blobs = check newContainerClient(testContainer("obs-missing"));
    Series[] before = check scrapeMetrics();
    BlobProperties|Error missing = blobs->getBlobProperties("absent.bin");
    Series[] after = check scrapeMetrics();
    test:assertTrue(missing is NotFoundError);
    map<string> failed = withLabels(identity("client"),
            {action_type: "client_operation", operation_type: "get", outcome: "failure", error_type: "NotFoundError"});
    test:assertEquals(delta(before, after, REQUESTS, failed), 1.0,
            "a failed operation's span is tagged with the error's type name");
}

@test:Config {groups: ["observability"]}
function testListenerCountsEveryStageOfAMessage() returns error? {
    EventSource src = check setupEventSource("obs-lsn");
    Client srcBlobs = src.blobs;
    int size = 5;
    byte[] payload = [];
    payload.setLength(size);
    check srcBlobs->upload(payload, "ok.bin");
    check srcBlobs->upload(payload, "poison.bin");
    final string queue = src.queue;
    final Recorder recorder = new;

    Series[] beforeListener = check scrapeMetrics();
    Listener lsn = check newListener();
    Service svc = @ServiceConfig {queueName: queue} isolated service object {
        isolated remote function onBlob(byte[] content, BlobEvent event) returns error? {
            recorder.add("handled", event.path);
            if event.path == "poison.bin" {
                return error DeadLetterError("give up");
            }
        }

        isolated remote function onError(Error err) returns error? {
            recorder.hit("error");
        }
    };
    check lsn.attach(svc);
    check lsn.'start();
    Series[] beforeEvents = check scrapeMetrics();

    check enqueueEvent(queue, blobCreatedEvent(src.container, "ok.bin"));
    check enqueueEvent(queue, blobCreatedEvent(src.container, "poison.bin"));
    check enqueueMessage(queue, "not an event");
    check enqueueEvent(queue, blobCreatedEvent(src.container, "ok.bin", topic = storageTopic("someoneelse")));
    check enqueueEvent(queue, blobDeletedEvent(src.container, "ok.bin"));
    check await(() => approximateMessageCount(queue) == 0 && approximateMessageCount(poisonQueue(queue)) == 2);
    Series[] afterEvents = check scrapeMetrics();

    // A deleted queue fails the next receive, which counts as a failed poll cycle.
    check deleteQueue(queue);
    map<string> poll = withLabels(identity("listener"), {action_type: "poll_cycle", watched_path: labelValue(queue)});
    check await(function() returns boolean|error {
        Series[] now = check scrapeMetrics();
        return delta(beforeEvents, now, EVENTS, withLabels(poll, {outcome: "failure"})) >= 1.0;
    });
    check lsn.gracefulStop();
    Series[] afterStop = check scrapeMetrics();

    test:assertEquals(recorder.list("handled").sort(), ["ok.bin", "poison.bin"],
            "both created events reached the handler");
    map<string> listenerIdentity = identity("listener");
    test:assertEquals(delta(beforeListener, beforeEvents, CONNECTIONS, listenerIdentity), 1.0,
            "the listener's init raises the connection gauge");
    test:assertEquals(delta(beforeListener, afterStop, CONNECTIONS, listenerIdentity), 0.0,
            "the stop lowers it once the listener's transport is released");
    test:assertTrue(delta(beforeEvents, afterEvents, EVENTS, withLabels(poll, {outcome: "success"})) >= 1.0,
            "every receive on the service's queue counts as a poll cycle");

    map<string> event = withLabels(listenerIdentity, {action_type: "file_event", watched_path: labelValue(queue)});
    map<string> found = withLabels(event, {file_stage: "found"});
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS, withLabels(found, {outcome: "success"})), 2.0,
            "a message routed to a handler is found once");
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(found, {outcome: "failure", error_type: "unparseable_message"})), 1.0);
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(found, {outcome: "skipped", error_type: "foreign_account"})), 1.0);
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(found, {outcome: "skipped", error_type: "no_handler_matched", event_type: "delete"})), 1.0);
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS, withLabels(event,
            {file_stage: "dispatched", handler_name: "onBlob", event_type: "create", outcome: "success"})), 2.0);
    test:assertEquals(delta(beforeEvents, afterEvents, REQUESTS,
            withLabels(event, {handler_name: "onBlob", event_type: "create", src_function_name: "onBlob"})), 2.0,
            "the runtime's span for the handler call carries the connector's tags");
    test:assertEquals(delta(beforeEvents, afterEvents, REQUESTS,
            withLabels(event, {handler_name: "onError", event_type: "error", error_type: "Error"})), 1.0,
            "the onError call for the unparseable message is tagged as an error event with the error's type");
    map<string> handled = withLabels(event, {file_stage: "handled", handler_name: "onBlob"});
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS, withLabels(handled, {outcome: "success"})), 1.0);
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(handled, {outcome: "failure", error_type: "DeadLetterError"})), 1.0,
            "a returned error's type name is the handled stage's error type");
    map<string> cleanedUp = withLabels(event, {file_stage: "cleaned_up", outcome: "success"});
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(cleanedUp, {cleanup_action: "acknowledge"})), 3.0,
            "the handled, the foreign, and the unrouted messages are acknowledged");
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS, withLabels(cleanedUp, {cleanup_action: "poison"})),
            2.0, "the dead-lettered and the unparseable messages are poisoned");
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(cleanedUp, {cleanup_action: "poison", event_type: "create"})), 1.0,
            "a settlement carries the event type when the message parsed");
    test:assertEquals(delta(beforeEvents, afterEvents, EVENTS,
            withLabels(cleanedUp, {cleanup_action: "acknowledge", event_type: "delete"})), 1.0);
    assertLabelKeys(afterEvents, EVENTS, ["action_type", "cleanup_action", "error_type", "event_type", "file_stage",
            "handler_name", "host", "module", "operation_type", "outcome", "protocol", "remote_url", "type",
            "watched_path"]);
    assertLabelKeys(afterEvents, BYTES, ["host", "module", "operation_type", "protocol", "remote_url", "type"]);
    assertLabelKeys(afterEvents, CONNECTIONS, ["host", "module", "protocol", "remote_url", "type"]);
    assertLabelKeys(afterEvents, DATABINDING_DURATION, ["handler_name", "host", "module", "outcome", "protocol",
            "remote_url", "type"]);
    assertLabelKeys(afterEvents, EXECUTION_DURATION, ["handler_name", "host", "module", "outcome", "protocol",
            "remote_url", "type"]);
    test:assertEquals(delta(beforeEvents, afterEvents, BYTES, withLabels(listenerIdentity, {operation_type: "get"})),
            <float>(2 * size), "the content fetched for the handlers counts as listener get bytes");
    test:assertTrue(total(afterEvents, DATABINDING_DURATION,
            withLabels(listenerIdentity, {handler_name: "onBlob", outcome: "success"})) > 0.0,
            "the fetch and bind time is recorded under the handler's name");
    test:assertTrue(afterEvents.some(s => s.name == EXECUTION_DURATION && s.labels["handler_name"] == "onBlob"
            && s.labels["outcome"] == "failure" && s.labels["module"] == "azure_blob"),
            "the handler's run time is recorded with its outcome");
}
