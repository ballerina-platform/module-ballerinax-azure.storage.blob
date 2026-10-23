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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import io.ballerina.lib.azure.storage.blob.client.ClientInit;
import io.ballerina.lib.azure.storage.blob.observability.BlobMetricsUtil;
import io.ballerina.lib.azure.storage.blob.server.ListenerContext.State;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ConfigurationException;
import io.ballerina.lib.azure.storage.blob.util.Credentials;
import io.ballerina.lib.azure.storage.blob.util.SharedTransports;
import io.ballerina.lib.azure.storage.blob.util.TransportConfigMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.ArrayList;
import java.util.List;

/**
 * Native backing of the queue-consuming {@code Listener}: builds the shared credential and
 * transport at {@code init}, validates and registers the attached services, and drives the
 * lifecycle. The receive loop of each service is its {@link Poller}; the per-message work is
 * the {@link Dispatcher}.
 */
public final class Listener {

    private static final String NATIVE_LISTENER_CONTEXT = "listenerContext";
    // The listener's private field carrying the client settings its Callers are built from.
    private static final BString CLIENT_CONFIG_FIELD = StringUtils.fromString("clientConfig");
    private static final BString QUEUE_SERVICE_URL = StringUtils.fromString("queueServiceUrl");
    private static final BString LAX_DATA_BINDING = StringUtils.fromString("laxDataBinding");
    private static final BString CONNECTION_POOL = StringUtils.fromString("connectionPool");
    private static final BString MAX_CONNECTIONS = StringUtils.fromString("maxConnections");

    static final String ON_BLOB = "onBlob";
    static final String ON_BLOB_TEXT = "onBlobText";
    static final String ON_BLOB_JSON = "onBlobJson";
    static final String ON_BLOB_XML = "onBlobXml";
    static final String ON_BLOB_CSV = "onBlobCsv";
    static final String ON_BLOB_DELETE = "onBlobDelete";
    static final String ON_ERROR = "onError";
    // Routing patterns are checked in this fixed order, so overlapping patterns resolve the
    // same way on every runtime, independent of method enumeration order.
    static final List<String> ROUTING_ORDER = List.of(ON_BLOB_TEXT, ON_BLOB_JSON, ON_BLOB_XML, ON_BLOB_CSV, ON_BLOB);

    private enum Stop { STOPPED_NOW, ALREADY_STOPPED }

    private Listener() {
    }

    /**
     * Initializes the listener: checks the credential, derives the queue endpoint, and builds
     * the one transport its services and Callers share. No call reaches Azure.
     *
     * @param env         the Ballerina runtime environment
     * @param listenerObj the Ballerina listener object
     * @param config      the {@code ListenerConfiguration} record
     * @return {@code null} on success, or the validation error
     */
    @SuppressWarnings("unchecked")
    public static Object initListener(Environment env, BObject listenerObj, BMap<BString, Object> config) {
        try {
            BMap<BString, Object> auth = (BMap<BString, Object>) config.getMapValue(ClientInit.AUTH);
            String sasSignature = Credentials.sasSignature(auth);
            Credentials.checkListenerSas(sasSignature);
            String queueServiceUrl = ValueUtils.optString(config, QUEUE_SERVICE_URL);
            if (queueServiceUrl != null) {
                queueServiceUrl = Credentials.validateUrl(queueServiceUrl, "queueServiceUrl");
            }
            Credentials.Resolved credential = Credentials.resolve(auth);
            String queueEndpoint = QueueEndpoints.derive(queueServiceUrl, credential);
            BMap<BString, Object> clientConfig = (BMap<BString, Object>) listenerObj.get(CLIENT_CONFIG_FIELD);
            BMap<BString, Object> transportConfig = (BMap<BString, Object>) config.get(ClientInit.TRANSPORT_CONFIG);
            TransportConfigMapper.Transport transport = TransportConfigMapper.transport(transportConfig);
            BlobServiceClient serviceClient = ClientInit.buildServiceClient(clientConfig, transport.httpClient());
            BMap<BString, Object> retry = (BMap<BString, Object>) config.get(ClientInit.RETRY_CONFIG);
            RequestRetryOptions retrying = TransportConfigMapper.retryOptions(retry, false);
            RequestRetryOptions retryFree = TransportConfigMapper.singleTry(retry);
            BMap<BString, Object> pool = (BMap<BString, Object>) transportConfig.get(CONNECTION_POOL);
            int maxConnections = Math.toIntExact((Long) pool.get(MAX_CONNECTIONS));
            HoldTiming timing = HoldTiming.fromSystemProperty();
            ListenerContext ctx = new ListenerContext(clientConfig, credential, queueEndpoint, transport,
                    retrying, retryFree, maxConnections, Credentials.listenerAccountName(credential),
                    serviceClient.getAccountUrl(), Boolean.TRUE.equals(config.get(LAX_DATA_BINDING)), timing,
                    env.getRuntime());
            SharedTransports.register(clientConfig, new SharedTransports.Shared(transport.httpClient(),
                    transport.provider(), serviceClient, sasSignature));
            if (timing.scaled()) {
                ctx.log.warn("the message hold is shortened to " + timing.hold().toSeconds() + " seconds by the "
                        + HoldTiming.PROPERTY + " system property, a test setting");
            }
            listenerObj.addNativeData(NATIVE_LISTENER_CONTEXT, ctx);
            BlobMetricsUtil.reportNewConnection(ctx.remoteUrl, ctx.protocol, BlobMetricsUtil.CONTEXT_LISTENER);
            return null;
        } catch (ConfigurationException e) {
            return BlobErrorCreator.clientError(e.getMessage(), null);
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * Attaches a service: validates its annotation and handlers against the rules and the
     * services already attached, builds its queue clients, and starts polling when the listener
     * is running.
     *
     * @param listenerObj the Ballerina listener object
     * @param service     the service being attached
     * @return {@code null} on success, or the validation error
     */
    public static Object attachService(BObject listenerObj, BObject service) {
        ListenerContext ctx = context(listenerObj);
        try {
            synchronized (ctx) {
                if (ctx.state() == State.STOPPED) {
                    throw new ConfigurationException("a service cannot be attached to a stopped listener");
                }
                ServiceContext parsed = ServiceContext.parse(ctx, service);
                List<ServiceSettings> attached = new ArrayList<>();
                for (ServiceContext other : ctx.services()) {
                    attached.add(other.settings);
                }
                parsed.settings.checkAgainst(attached);
                ctx.addService(parsed);
                attached.add(parsed.settings);
                String warning = poolWarning(attached, ctx.maxConnections);
                if (warning != null) {
                    ctx.log.warn(warning);
                }
                if (ctx.state() == State.RUNNING) {
                    parsed.poller = new Poller(ctx, parsed);
                    parsed.poller.start();
                }
            }
            return null;
        } catch (ConfigurationException e) {
            return BlobErrorCreator.clientError(e.getMessage(), null);
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * The warning logged when the attached services' combined concurrency exceeds the pool,
     * or {@code null} when it does not.
     *
     * @param attached       the attached services' settings
     * @param maxConnections the pool size
     * @return the warning text, or {@code null}
     */
    static String poolWarning(List<ServiceSettings> attached, int maxConnections) {
        int sum = 0;
        for (ServiceSettings settings : attached) {
            sum += settings.concurrency();
        }
        if (sum <= maxConnections) {
            return null;
        }
        return "the attached services can have up to " + sum + " blob fetches in flight (the sum of each one's "
                + "batchSize + newBatchThreshold), above transportConfig.connectionPool.maxConnections ("
                + maxConnections + "). The sum is an upper bound, not connections held: a stream read holds no "
                + "connection between chunks, and the queue traffic has its own pool. Raise maxConnections or lower "
                + "batchSize";
    }

    /**
     * Detaches a service: its queue is no longer polled; its running handlers finish and settle.
     *
     * @param listenerObj the Ballerina listener object
     * @param service     the service being detached
     * @return an error if the service is not attached, otherwise {@code null}
     */
    public static Object detachService(BObject listenerObj, BObject service) {
        ListenerContext ctx = context(listenerObj);
        ServiceContext removed = ctx.removeService(service);
        if (removed == null) {
            return BlobErrorCreator.clientError("the given service is not attached to this listener", null);
        }
        Poller poller = removed.poller;
        if (poller != null) {
            poller.stop();
        }
        return null;
    }

    /**
     * Starts polling every attached service's queue. A running listener cannot be started again,
     * and a stopped one cannot be restarted.
     *
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object startListener(BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        synchronized (ctx) {
            if (ctx.state() == State.RUNNING) {
                return BlobErrorCreator.clientError("the listener is already running", null);
            }
            if (ctx.state() == State.STOPPED) {
                return BlobErrorCreator.clientError("a stopped listener cannot be started again", null);
            }
            ctx.state(State.RUNNING);
            for (ServiceContext service : ctx.services()) {
                service.poller = new Poller(ctx, service);
                service.poller.start();
            }
        }
        return null;
    }

    /**
     * Stops polling and waits for the in-flight handlers and onError calls to finish, for at
     * most the graceful bound; what still runs after that is logged and continues.
     *
     * @param env         the Ballerina runtime environment
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object gracefulStopListener(Environment env, BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        Object stopped = stopPolling(ctx);
        if (stopped != Stop.STOPPED_NOW) {
            return stopped == Stop.ALREADY_STOPPED ? null : stopped;
        }
        return BallerinaAzureClient.invoke(env, () -> {
            boolean idle;
            try {
                idle = ctx.awaitIdle(ctx.timing.gracefulBound());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                idle = ctx.inFlight() == 0;
            }
            if (!idle) {
                ctx.log.warn(ctx.inFlight() + " handler(s) still running after the stop wait; they continue and "
                        + "settle their messages when they finish");
            }
            ctx.releaseIfIdle();
            return null;
        });
    }

    /**
     * Stops polling and returns at once; the handlers still running keep their messages hidden
     * and settle normally.
     *
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object immediateStopListener(BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        Object stopped = stopPolling(ctx);
        if (stopped != Stop.STOPPED_NOW) {
            return stopped == Stop.ALREADY_STOPPED ? null : stopped;
        }
        ctx.releaseIfIdle();
        return null;
    }

    // Stopping a listener that never started fails; stopping a stopped one again does nothing.
    private static Object stopPolling(ListenerContext ctx) {
        synchronized (ctx) {
            if (ctx.state() == State.NEW) {
                return BlobErrorCreator.clientError("the listener has not been started", null);
            }
            if (ctx.state() == State.STOPPED) {
                return Stop.ALREADY_STOPPED;
            }
            ctx.state(State.STOPPED);
            for (ServiceContext service : ctx.services()) {
                if (service.poller != null) {
                    service.poller.stop();
                }
            }
        }
        return Stop.STOPPED_NOW;
    }

    private static ListenerContext context(BObject listenerObj) {
        return (ListenerContext) listenerObj.getNativeData(NATIVE_LISTENER_CONTEXT);
    }
}
