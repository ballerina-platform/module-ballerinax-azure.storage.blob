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

import com.azure.storage.common.policy.RequestRetryOptions;
import io.ballerina.lib.azure.storage.blob.observability.BlobMetricsUtil;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.Credentials;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.lib.azure.storage.blob.util.SharedTransports;
import io.ballerina.lib.azure.storage.blob.util.TransportConfigMapper;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-listener state: the one credential and transport every service and {@code Caller}
 * shares, the attached services, the Callers kept per container, the account check's memory,
 * the in-flight count a graceful stop waits on, and the lifecycle state.
 */
final class ListenerContext {

    enum State { NEW, RUNNING, STOPPED }

    private static final String NEW_CALLER_FUNCTION = "newCaller";

    final BMap<BString, Object> clientConfig;
    final Credentials.Resolved credential;
    final String queueEndpoint;
    final TransportConfigMapper.Transport transport;
    // The retry options of the queue clients: the configured ones without a secondary host, or
    // the SDK's defaults when none are configured; and the single try of the interpreted calls.
    final RequestRetryOptions retrying;
    final RequestRetryOptions retryFree;
    final int maxConnections;
    final String accountName;
    // The blob endpoint's host and scheme, the identity every metric and span of this listener carries.
    final String remoteUrl;
    final String protocol;
    final boolean laxDataBinding;
    final HoldTiming timing;
    final Clock clock = Clock.systemUTC();
    final Runtime runtime;
    final ListenerLog log;
    // One scheduler per listener that only fires attempts; the attempts run on virtual threads.
    final ScheduledExecutorService scheduler;
    final Executor attempts = Thread::startVirtualThread;

    private final List<ServiceContext> services = new ArrayList<>();
    private final Map<String, BObject> callers = new ConcurrentHashMap<>();
    final Map<String, AtomicLong> foreignAccounts = new ConcurrentHashMap<>();
    final AtomicBoolean unknownAccountWarned = new AtomicBoolean();
    // Messages being handled and onError calls running, across every service, detached ones
    // included; a graceful stop waits for it to reach zero.
    private final AtomicInteger inFlight = new AtomicInteger();
    private State state = State.NEW;
    private boolean released;
    // Bumped under the monitor each time the in-flight count drains, for the graceful wait.
    private long drains;

    ListenerContext(BMap<BString, Object> clientConfig, Credentials.Resolved credential, String queueEndpoint,
                    TransportConfigMapper.Transport transport, RequestRetryOptions retrying,
                    RequestRetryOptions retryFree, int maxConnections, String accountName, String blobEndpoint,
                    boolean laxDataBinding, HoldTiming timing, Runtime runtime) {
        this.clientConfig = clientConfig;
        this.credential = credential;
        this.queueEndpoint = queueEndpoint;
        this.transport = transport;
        this.retrying = retrying;
        this.retryFree = retryFree;
        this.maxConnections = maxConnections;
        this.accountName = accountName;
        this.remoteUrl = BallerinaAzureClient.extractHost(blobEndpoint);
        this.protocol = BallerinaAzureClient.extractProtocol(blobEndpoint);
        this.laxDataBinding = laxDataBinding;
        this.timing = timing;
        this.runtime = runtime;
        this.log = new ListenerLog.RuntimeLog(runtime);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "azure-blob-listener-timer");
            thread.setDaemon(true);
            return thread;
        });
    }

    synchronized State state() {
        return state;
    }

    synchronized void state(State next) {
        state = next;
    }

    synchronized List<ServiceContext> services() {
        return new ArrayList<>(services);
    }

    synchronized void addService(ServiceContext service) {
        services.add(service);
    }

    synchronized ServiceContext removeService(BObject service) {
        for (ServiceContext candidate : services) {
            if (candidate.service == service) {
                services.remove(candidate);
                return candidate;
            }
        }
        return null;
    }

    /** Counts one message or onError call as in flight. */
    void enter() {
        inFlight.incrementAndGet();
    }

    /** Ends one in-flight unit; the last one out after a stop releases the shared resources. */
    void exit() {
        if (inFlight.decrementAndGet() == 0) {
            synchronized (this) {
                drains++;
                notifyAll();
            }
            releaseIfIdle();
        }
    }

    int inFlight() {
        return inFlight.get();
    }

    /** Waits for the in-flight count to reach zero, up to the bound; true when it did. */
    synchronized boolean awaitIdle(Duration bound) throws InterruptedException {
        long deadline = System.nanoTime() + bound.toNanos();
        while (inFlight.get() > 0) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            wait(Math.max(1, remaining / 1_000_000));
        }
        return true;
    }

    /** Once stopped and idle, disposes of the shared transport and the timer, exactly once. */
    synchronized void releaseIfIdle() {
        if (state != State.STOPPED || inFlight.get() > 0 || released) {
            return;
        }
        released = true;
        SharedTransports.unregister(clientConfig);
        scheduler.shutdownNow();
        transport.provider().dispose();
        BlobMetricsUtil.reportConnectionClose(remoteUrl, protocol, BlobMetricsUtil.CONTEXT_LISTENER);
    }

    /** The Caller bound to a container, built on its first event and kept for the listener's life. */
    BObject callerFor(String containerName) {
        BObject cached = callers.get(containerName);
        if (cached != null) {
            return cached;
        }
        Object built = runtime.callFunction(ModuleUtils.getModule(), NEW_CALLER_FUNCTION,
                new StrandMetadata(true, null), StringUtils.fromString(containerName), clientConfig);
        if (built instanceof BError e) {
            throw e;
        }
        BObject previous = callers.putIfAbsent(containerName, (BObject) built);
        return previous != null ? previous : (BObject) built;
    }

    /** Whether an event's account is this listener's, or the listener's account is unknown; no logging. */
    boolean sameAccount(String eventAccount) {
        return accountName == null || accountName.equalsIgnoreCase(eventAccount);
    }

    /**
     * Whether an event's account is another account's. An unknown listener account skips the
     * check with a one-time warning; the comparison ignores case.
     */
    boolean isForeign(String eventAccount, String queueName) {
        if (accountName == null) {
            if (unknownAccountWarned.compareAndSet(false, true)) {
                log.warn("the storage account cannot be told from the credential, so events are not checked "
                        + "against it; the listener assumes its queues carry the events of a single account");
            }
            return false;
        }
        if (accountName.equalsIgnoreCase(eventAccount)) {
            return false;
        }
        long seen = foreignAccounts.computeIfAbsent(eventAccount.toLowerCase(Locale.ROOT),
                key -> new AtomicLong()).incrementAndGet();
        Map<String, Object> fields = ListenerLog.fields("queue", queueName, "eventAccount", eventAccount,
                "listenerAccount", accountName, "count", seen);
        if (seen == 1) {
            log.error("queue " + queueName + " carries an event of storage account '" + eventAccount
                    + "', not this listener's '" + accountName + "'; acknowledged without dispatch. A subscription "
                    + "of another account delivers to this queue, which drops that account's events", fields);
        } else {
            log.debug("another event of storage account '" + eventAccount + "' on queue " + queueName
                    + " acknowledged without dispatch (" + seen + " so far)", fields);
        }
        return true;
    }
}
