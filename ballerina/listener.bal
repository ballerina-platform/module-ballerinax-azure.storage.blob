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
    # The authentication configuration (see `AuthConfig`). A SAS credential must be an account
    # SAS covering both the queue and the blob services
    AuthConfig auth;
    # The longest wait between polls of an empty queue, in seconds
    decimal maxPollingIntervalSeconds = 60;
    # How many messages one receive fetches, 1 to 32
    int batchSize = 16;
    # The number of in-flight events at which the next batch is fetched. Defaults to half the batch size
    int newBatchThreshold?;
    # How long a message stays invisible after its handler fails, in seconds, before it is redelivered
    decimal redeliveryDelaySeconds = 0;
    # How many deliveries a message gets before it is moved to the poison queue
    int maxDeliveryCount = 5;
    # Retry behaviour for service requests; omit for the service defaults
    RetryConfig retryConfig?;
    # HTTP transport settings (proxy, connection pool, TLS)
    TransportConfig transportConfig = {};
    # Relaxed data binding for the typed content handlers: JSON, XML, and CSV record binding
    # treat a null value as an optional field and an absent field as a nilable field
    boolean laxDataBinding = false;
    # The queue service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.queue.core.windows.net`
    string queueServiceUrl?;
|};

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

# Consumes the blob events an Azure Event Grid subscription delivers to an Azure Storage queue,
# and dispatches each to the service attached for the event's container.
public isolated class Listener {

    // The client settings every Caller of this listener is built from.
    private final ClientConfiguration & readonly clientConfig;

    # Initializes the listener for a queue.
    #
    # + queueName - The name of the storage queue the Event Grid subscription delivers to
    # + config - The listener configuration (authentication, polling, delivery, transport)
    # + return - An `Error` if the listener could not be initialized, otherwise `()`
    public isolated function init(string queueName, *ListenerConfiguration config) returns Error? {
        if config.batchSize < 1 || config.batchSize > 32 {
            return error Error("batchSize must be between 1 and 32");
        }
        int newBatchThreshold = config?.newBatchThreshold ?: config.batchSize / 2;
        if newBatchThreshold < 0 || newBatchThreshold > config.batchSize {
            return error Error("newBatchThreshold must be between 0 and batchSize");
        }
        config.newBatchThreshold = newBatchThreshold;
        if config.maxPollingIntervalSeconds <= 0d {
            return error Error("maxPollingIntervalSeconds must be greater than zero");
        }
        if config.redeliveryDelaySeconds < 0d {
            return error Error("redeliveryDelaySeconds must not be negative");
        }
        if config.maxDeliveryCount < 1 {
            return error Error("maxDeliveryCount must be greater than zero");
        }
        if queueName.trim() == "" {
            return error Error("queueName must not be empty");
        }
        ClientConfiguration clientConfig = {auth: config.auth, transportConfig: config.transportConfig};
        RetryConfig? retryConfig = config?.retryConfig;
        if retryConfig is RetryConfig {
            clientConfig.retryConfig = retryConfig;
        }
        self.clientConfig = clientConfig.cloneReadOnly();
        return externInit(self, queueName, config);
    }

    # Attaches a service for the container named by its attach point. A service with no attach
    # point handles the events of every container no other service claims.
    #
    # + serviceRef - The service to attach
    # + name - The container name, from the service's attach point
    # + return - An `error` if the service could not be attached, otherwise `()`
    public isolated function attach(Service serviceRef, string[]|string? name = ()) returns error? {
        string? container = check attachContainer(name);
        Caller? caller = ();
        if container is string {
            caller = check new Caller(container, self.clientConfig);
        }
        return externAttach(self, serviceRef, container, caller);
    }

    # Detaches a service from the listener. Detaching a service that is not attached fails.
    #
    # + serviceRef - The service to detach
    # + return - An `error` if the service could not be detached, otherwise `()`
    public isolated function detach(Service serviceRef) returns error? {
        return externDetach(self, serviceRef);
    }

    # Starts consuming the queue. Starting a listener that is already running fails.
    #
    # + return - An `error` if the listener could not be started, otherwise `()`
    public isolated function 'start() returns error? {
        return externStart(self);
    }

    # Stops polling and waits for the handlers already running to finish.
    #
    # + return - An `error` if the listener could not be stopped, otherwise `()`
    public isolated function gracefulStop() returns error? {
        return externGracefulStop(self);
    }

    # Stops polling and returns without waiting. Messages whose handlers were still running
    # reappear on the queue when their visibility window expires.
    #
    # + return - An `error` if the listener could not be stopped, otherwise `()`
    public isolated function immediateStop() returns error? {
        return externImmediateStop(self);
    }
}

// Resolves the attach point to a container name: one segment (a leading slash removed) that
// satisfies the service's container-name rule, or `$root` / `$logs`; nil is the catch-all.
isolated function attachContainer(string[]|string? name) returns string?|error {
    if name is () {
        return ();
    }
    string value;
    if name is string[] {
        if name.length() != 1 {
            return error("the attach point must be a single container name");
        }
        value = name[0];
    } else {
        value = name;
    }
    if value.startsWith("/") {
        value = value.substring(1);
    }
    if value == "$root" || value == "$logs" {
        return value;
    }
    if !re `^[a-z0-9](?:-?[a-z0-9]){2,62}$`.isFullMatch(value) {
        return error(string `'${value}' is not a valid container name: 3 to 63 lowercase letters, digits `
                + "and single hyphens, starting and ending with a letter or digit");
    }
    return value;
}

// Builds the Caller a catch-all service receives for one event's container. Called from the
// native dispatcher on a strand.
isolated function newCaller(string containerName, ClientConfiguration config) returns Caller|Error {
    return new (containerName, config);
}

// The listener's Java side reports its diagnostics through these, so they reach the user's log.
// Calling `log` from Java would need an slf4j binding, which the distribution does not carry.
isolated function logListenerWarn(string message) {
    log:printWarn(message);
}

isolated function logListenerError(string message) {
    log:printError(message);
}

isolated function logListenerDebug(string message) {
    log:printDebug(message);
}

isolated function externInit(Listener listenerObj, string queueName, ListenerConfiguration config)
        returns Error? = @java:Method {
    name: "initListener",
    'class: "io.ballerina.lib.azure.storage.blob.server.Listener"
} external;

isolated function externAttach(Listener listenerObj, Service serviceRef, string? container, Caller? caller)
        returns error? = @java:Method {
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
