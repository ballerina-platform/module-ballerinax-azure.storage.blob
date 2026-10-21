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

import ballerina/jballerina.java;
import ballerina/log;

# Configuration for an `azure.storage.blob` `Listener`.
public type ListenerConfiguration record {|
    # The authentication configuration (see `AuthConfig`). One credential serves every queue,
    # every poison queue, and every `Caller` of the listener; a SAS credential must be an
    # account SAS covering both the queue and the blob services
    AuthConfig auth;
    # Relaxed data binding for the typed content handlers: JSON, XML, and CSV record binding
    # treat a null value as an optional field and an absent field as a nilable field
    boolean laxDataBinding = false;
    # Retry behaviour for service requests; omit to keep the connector's defaults. The
    # secondary host, if given, serves the blob requests only
    RetryConfig retryConfig?;
    # HTTP transport settings (proxy, connection pool, TLS), shared by every service and
    # `Caller` of the listener
    TransportConfig transportConfig = {};
    # The queue service endpoint URL, including the scheme. Omit to derive it from the
    # credential's blob endpoint or account name
    string queueServiceUrl?;
|};

# The queue a service consumes and how it is consumed, supplied through the
# `@blob:ServiceConfig` annotation.
public type ServiceConfiguration record {|
    # The storage queue the service's Event Grid subscription delivers to
    string queueName;
    # The queue failed messages move to. Defaults to `{queueName}-poison`
    string poisonQueueName?;
    # How many messages one receive fetches, 1 to 32
    int batchSize = 16;
    # The number of events still being handled at which the next batch is fetched, 0 to
    # `batchSize`. Defaults to half the batch size, rounded down
    int newBatchThreshold?;
    # The longest wait between polls of an empty queue, in seconds
    decimal maxPollingInterval = 60;
    # How long a message stays invisible after its handler fails, before it is redelivered,
    # in seconds, 0 to 604,800, rounded up to whole seconds
    decimal redeliveryDelay = 0;
    # How many deliveries a message gets before it is moved to the poison queue, at least 1
    int maxDeliveryCount = 5;
|};

# Names the queue a service consumes and sets its consumption settings.
public annotation ServiceConfiguration ServiceConfig on service, class;

# The per-handler routing configuration, supplied through the `@blob:FunctionConfig`
# annotation. It overrides the extension-based routing of created events.
public type FunctionConfiguration record {|
    # A regular expression matched against the blob name (the last path segment)
    string namePattern?;
    # A regular expression matched against the blob's content type
    string contentTypePattern?;
|};

# Declares the routing configuration of a content handler.
public annotation FunctionConfiguration FunctionConfig on object function;

# The service type attached to a `Listener`.
public type Service distinct service object {
};

# Consumes the blob events Azure Event Grid subscriptions deliver to Azure Storage queues,
# and dispatches each to the service that names the queue it arrived on.
public isolated class Listener {

    // The client settings every Caller of this listener is built from.
    private final ClientConfiguration & readonly clientConfig;

    # Initializes the listener. Every value is validated locally; no call reaches Azure.
    #
    # + config - The listener configuration (authentication, data binding, transport)
    # + return - An `Error` if the listener could not be initialized, otherwise `()`
    public isolated function init(*ListenerConfiguration config) returns Error? {
        ClientConfiguration clientConfig = {auth: config.auth, transportConfig: config.transportConfig};
        RetryConfig? retryConfig = config?.retryConfig;
        if retryConfig is RetryConfig {
            clientConfig.retryConfig = retryConfig;
        }
        self.clientConfig = clientConfig.cloneReadOnly();
        return externInit(self, config);
    }

    # Attaches a service, which names the queue it consumes in its `@blob:ServiceConfig`
    # annotation. On a running listener the service's queue is polled at once.
    #
    # + serviceRef - The service to attach
    # + name - The attach point, which must be empty
    # + return - An `error` if the service could not be attached, otherwise `()`
    public isolated function attach(Service serviceRef, string[]|string? name = ()) returns error? {
        if name is string || (name is string[] && name.length() > 0) {
            return error Error("a service attaches with no name; it names its queue in '@blob:ServiceConfig'");
        }
        return externAttach(self, serviceRef);
    }

    # Detaches a service: its queue is no longer polled, while its running handlers finish and
    # settle their messages. Detaching a service that is not attached fails.
    #
    # + serviceRef - The service to detach
    # + return - An `error` if the service could not be detached, otherwise `()`
    public isolated function detach(Service serviceRef) returns error? {
        return externDetach(self, serviceRef);
    }

    # Starts polling the queue of every attached service. Starting a listener that is already
    # running fails, and a stopped listener cannot be started again.
    #
    # + return - An `error` if the listener could not be started, otherwise `()`
    public isolated function 'start() returns error? {
        return externStart(self);
    }

    # Stops polling and waits for the handlers still running, for up to ten minutes.
    #
    # + return - An `error` if the listener could not be stopped, otherwise `()`
    public isolated function gracefulStop() returns error? {
        return externGracefulStop(self);
    }

    # Stops polling and returns at once. Handlers still running keep their messages hidden
    # and settle normally.
    #
    # + return - An `error` if the listener could not be stopped, otherwise `()`
    public isolated function immediateStop() returns error? {
        return externImmediateStop(self);
    }
}

// Builds the Caller for one event's container. Called from the native dispatcher on a strand.
isolated function newCaller(string containerName, ClientConfiguration config) returns Caller|Error {
    return new (containerName, config);
}

// How a handler's returned error settles its message: recognised by the module's own types,
// and only on the returned value itself, so a wrapped settlement error stays an ordinary failure.
type HandlerOutcome record {|
    # `deadLetter`, `retry`, or `failure`
    string kind;
    # The requested delay of a `RetryError`
    decimal delay = 0;
    # The error's message
    string message;
|};

isolated function classifyHandlerError(error e) returns HandlerOutcome {
    if e is DeadLetterError {
        return {kind: "deadLetter", message: messageWithCauses(e)};
    }
    if e is RetryError {
        return {kind: "retry", delay: e.detail().delay, message: messageWithCauses(e)};
    }
    return {kind: "failure", message: messageWithCauses(e)};
}

// The error's message followed by its causes', so a wrapped failure keeps its detail in the
// log line and the poison move's reason.
isolated function messageWithCauses(error e) returns string {
    string message = e.message();
    error? cause = e.cause();
    int depth = 0;
    while cause is error && depth < 8 {
        message += ": " + cause.message();
        cause = cause.cause();
        depth += 1;
    }
    return message;
}

// The listener's Java side reports its diagnostics through this, so they reach the user's log
// with their key-value fields. Calling `log` from Java would need an slf4j binding, which the
// distribution does not carry.
isolated function logListener(string level, string message, map<anydata> fields) {
    log:KeyValues keyValues = {};
    foreach [string, anydata] [key, value] in fields.entries() {
        keyValues[key] = value;
    }
    match level {
        "error" => {
            log:printError(message, (), (), keyValues);
        }
        "warn" => {
            log:printWarn(message, (), (), keyValues);
        }
        _ => {
            log:printDebug(message, (), (), keyValues);
        }
    }
}

isolated function externInit(Listener listenerObj, ListenerConfiguration config) returns Error? = @java:Method {
    name: "initListener",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externAttach(Listener listenerObj, Service serviceRef) returns error? = @java:Method {
    name: "attachService",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externDetach(Listener listenerObj, Service serviceRef) returns error? = @java:Method {
    name: "detachService",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externStart(Listener listenerObj) returns error? = @java:Method {
    name: "startListener",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externGracefulStop(Listener listenerObj) returns error? = @java:Method {
    name: "gracefulStopListener",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externImmediateStop(Listener listenerObj) returns error? = @java:Method {
    name: "immediateStopListener",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;
