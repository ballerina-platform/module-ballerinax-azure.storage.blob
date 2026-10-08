/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.blob.server;

import com.azure.storage.queue.QueueClient;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Per-listener state: the configuration values the poller and dispatcher read, the queue
 * clients, the attached services by container, and the lifecycle state.
 */
final class ListenerContext {

    enum State { NEW, RUNNING, STOPPED }

    final String queueName;
    final int batchSize;
    final int newBatchThreshold;
    final int maxDeliveryCount;
    final long maxPollingMillis;
    final long redeliverySeconds;
    final boolean laxDataBinding;
    // The ListenerConfiguration (for the queue clients) and the ClientConfiguration derived
    // from it (for the per-event Callers of the catch-all service).
    final BMap<BString, Object> listenerConfig;
    final BMap<BString, Object> clientConfig;
    final QueueClient queue;

    final Map<String, ServiceContext> services = new ConcurrentHashMap<>();
    // The catch-all service's Callers, one per container seen, built on first use: each holds a
    // client with its own connection pool, so they are kept rather than rebuilt per event.
    final Map<String, BObject> catchAllCallers = new ConcurrentHashMap<>();
    volatile ServiceContext catchAll;
    volatile Runtime runtime;
    volatile Poller poller;
    volatile PoisonQueue poison;
    volatile State state = State.NEW;
    // Messages whose handling is still running; the poller fetches the next batch when this
    // drops to newBatchThreshold, and a graceful stop waits for it to reach zero.
    final AtomicInteger inFlight = new AtomicInteger();
    // Messages received and not yet disposed of; an immediate stop releases them.
    final Set<MessageHandle> active = ConcurrentHashMap.newKeySet();

    ListenerContext(String queueName, int batchSize, int newBatchThreshold, int maxDeliveryCount,
                    long maxPollingMillis, long redeliverySeconds, boolean laxDataBinding,
                    BMap<BString, Object> listenerConfig, BMap<BString, Object> clientConfig, QueueClient queue) {
        this.queueName = queueName;
        this.batchSize = batchSize;
        this.newBatchThreshold = newBatchThreshold;
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxPollingMillis = maxPollingMillis;
        this.redeliverySeconds = redeliverySeconds;
        this.laxDataBinding = laxDataBinding;
        this.listenerConfig = listenerConfig;
        this.clientConfig = clientConfig;
        this.queue = queue;
    }

    /** The service for a container: the one attached under that name, else the catch-all. */
    ServiceContext serviceFor(String container) {
        ServiceContext named = services.get(container);
        return named != null ? named : catchAll;
    }

    // An attached service, its container (null for the catch-all), its Caller, and its handlers.
    record ServiceContext(BObject service, String container, BObject caller,
                          Map<String, HandlerConfig> handlers, int onErrorArity) {
    }

    // One handler: its routing patterns, parameter-list shape, and declared content type.
    record HandlerConfig(String methodName, Pattern namePattern, Pattern contentTypePattern, int arity,
                         boolean secondIsCaller, Type contentType) {
    }
}
