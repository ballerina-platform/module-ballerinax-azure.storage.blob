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

import io.ballerina.lib.azure.storage.blob.server.ListenerContext.HandlerConfig;
import io.ballerina.lib.azure.storage.blob.server.ListenerContext.ServiceContext;
import io.ballerina.lib.azure.storage.blob.server.ListenerContext.State;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.types.MethodType;
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Native backing of the queue-consuming {@code Listener}: holds the per-listener context,
 * reads the attached services' handlers and their {@code @blob:FunctionConfig} routing, and
 * drives the lifecycle. The receive loop is the {@link Poller}; the per-message work is the
 * {@link Dispatcher}.
 */
public final class Listener {

    // Native-data key under which the per-listener context is stored on the listener object.
    private static final String NATIVE_LISTENER_CONTEXT = "listenerContext";
    // The listener's private field carrying the client settings its Callers are built from.
    private static final BString CLIENT_CONFIG_FIELD = StringUtils.fromString("clientConfig");

    private static final BString BATCH_SIZE = StringUtils.fromString("batchSize");
    private static final BString NEW_BATCH_THRESHOLD = StringUtils.fromString("newBatchThreshold");
    private static final BString MAX_DELIVERY_COUNT = StringUtils.fromString("maxDeliveryCount");
    private static final BString MAX_POLLING_INTERVAL_SECONDS = StringUtils.fromString("maxPollingIntervalSeconds");
    private static final BString REDELIVERY_DELAY_SECONDS = StringUtils.fromString("redeliveryDelaySeconds");
    private static final BString LAX_DATA_BINDING = StringUtils.fromString("laxDataBinding");

    private static final String FUNCTION_CONFIG_ANNOTATION = "FunctionConfig";
    private static final BString NAME_PATTERN = StringUtils.fromString("namePattern");
    private static final BString CONTENT_TYPE_PATTERN = StringUtils.fromString("contentTypePattern");
    private static final String CALLER_TYPE_NAME = "Caller";

    static final String ON_BLOB = "onBlob";
    static final String ON_BLOB_TEXT = "onBlobText";
    static final String ON_BLOB_JSON = "onBlobJson";
    static final String ON_BLOB_XML = "onBlobXml";
    static final String ON_BLOB_CSV = "onBlobCsv";
    static final String ON_BLOB_DELETED = "onBlobDeleted";
    static final String ON_ERROR = "onError";
    // Routing patterns are checked in this fixed order, so overlapping patterns resolve the
    // same way on every runtime, independent of method enumeration order.
    static final List<String> ROUTING_ORDER = List.of(ON_BLOB_TEXT, ON_BLOB_JSON, ON_BLOB_XML, ON_BLOB_CSV, ON_BLOB);
    private static final Set<String> HANDLER_NAMES = Set.of(ON_BLOB, ON_BLOB_TEXT, ON_BLOB_JSON, ON_BLOB_XML,
            ON_BLOB_CSV, ON_BLOB_DELETED);

    private static final long STOP_POLL_MILLIS = 50;

    private Listener() {
    }

    /**
     * Initializes the listener: validates the queue endpoint and credential locally, and stores
     * the context the poller and dispatcher read. No call reaches Azure.
     *
     * @param env         the Ballerina runtime environment
     * @param listenerObj the Ballerina listener object
     * @param queueName   the queue the Event Grid subscription delivers to
     * @param config      the {@code ListenerConfiguration} record
     * @return {@code null} on success, or the validation error
     */
    @SuppressWarnings("unchecked")
    public static Object initListener(Environment env, BObject listenerObj, BString queueName,
                                      BMap<BString, Object> config) {
        try {
            String queue = queueName.getValue().strip();
            int batchSize = (int) config.getIntValue(BATCH_SIZE).longValue();
            int newBatchThreshold = (int) config.getIntValue(NEW_BATCH_THRESHOLD).longValue();
            int maxDeliveryCount = (int) config.getIntValue(MAX_DELIVERY_COUNT).longValue();
            long maxPollingMillis = Math.max(1, Math.round(
                    ((BDecimal) config.get(MAX_POLLING_INTERVAL_SECONDS)).floatValue() * 1000));
            long redeliverySeconds = (long) Math.ceil(((BDecimal) config.get(REDELIVERY_DELAY_SECONDS)).floatValue());
            boolean laxDataBinding = Boolean.TRUE.equals(config.get(LAX_DATA_BINDING));
            BMap<BString, Object> clientConfig = (BMap<BString, Object>) listenerObj.get(CLIENT_CONFIG_FIELD);
            ListenerContext ctx = new ListenerContext(queue, batchSize, newBatchThreshold, maxDeliveryCount,
                    maxPollingMillis, redeliverySeconds, laxDataBinding, config, clientConfig,
                    QueueClients.build(config, queue));
            ctx.runtime = env.getRuntime();
            listenerObj.addNativeData(NATIVE_LISTENER_CONTEXT, ctx);
            return null;
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * Attaches a service for a container, or as the catch-all when the container is nil.
     *
     * @param listenerObj the Ballerina listener object
     * @param service     the service being attached
     * @param container   the container name, or {@code null} for the catch-all
     * @param caller      the service's {@code Caller}, or {@code null} for the catch-all
     * @return {@code null} on success, or the validation error
     */
    public static Object attachService(BObject listenerObj, BObject service, Object container, Object caller) {
        ListenerContext ctx = context(listenerObj);
        try {
            String name = container == null ? null : ((BString) container).getValue();
            ServiceContext parsed = parseService(service, name, (BObject) caller);
            synchronized (ctx) {
                if (name == null) {
                    if (ctx.catchAll != null) {
                        throw BlobErrorCreator.clientError(
                                "a service with no attach point is already attached to this listener", null);
                    }
                    ctx.catchAll = parsed;
                } else if (ctx.services.putIfAbsent(name, parsed) != null) {
                    throw BlobErrorCreator.clientError(
                            "a service for container '" + name + "' is already attached to this listener", null);
                }
            }
            return null;
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * Detaches a service.
     *
     * @param listenerObj the Ballerina listener object
     * @param service     the service being detached
     * @return an error if the service is not attached, otherwise {@code null}
     */
    public static Object detachService(BObject listenerObj, BObject service) {
        ListenerContext ctx = context(listenerObj);
        synchronized (ctx) {
            if (ctx.catchAll != null && ctx.catchAll.service() == service) {
                ctx.catchAll = null;
                return null;
            }
            if (ctx.services.entrySet().removeIf(entry -> entry.getValue().service() == service)) {
                return null;
            }
        }
        return BlobErrorCreator.clientError("the given service is not attached to this listener", null);
    }

    /**
     * Starts the receive loop. A running listener cannot be started again, and a stopped one
     * cannot be restarted.
     *
     * @param env         the Ballerina runtime environment
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object startListener(Environment env, BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        synchronized (ctx) {
            if (ctx.state == State.RUNNING) {
                return BlobErrorCreator.clientError("the listener is already running", null);
            }
            if (ctx.state == State.STOPPED) {
                return BlobErrorCreator.clientError("a stopped listener cannot be started again", null);
            }
            ctx.runtime = env.getRuntime();
            ctx.poller = new Poller(ctx);
            ctx.state = State.RUNNING;
            ctx.poller.start();
        }
        return null;
    }

    /**
     * Stops polling and waits for the in-flight handlers to finish, for at most the visibility
     * window; what is still running after that is logged and left to redelivery.
     *
     * @param env         the Ballerina runtime environment
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object gracefulStopListener(Environment env, BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        Object stopped = stopPolling(ctx);
        if (stopped != null) {
            return stopped;
        }
        return BallerinaAzureClient.invoke(env, () -> {
            long deadline = System.currentTimeMillis() + MessageHandle.VISIBILITY.toMillis();
            try {
                while (ctx.inFlight.get() > 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(STOP_POLL_MILLIS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (ctx.inFlight.get() > 0) {
                logWarn(ctx, "azure.storage.blob listener: " + ctx.inFlight.get()
                        + " handler(s) still running after the stop wait; their messages will be redelivered");
                releaseActive(ctx);
            }
            return null;
        });
    }

    /**
     * Stops polling and returns at once; the messages of the handlers still running reappear
     * on the queue when their visibility window expires.
     *
     * @param listenerObj the Ballerina listener object
     * @return {@code null} on success, or the error
     */
    public static Object immediateStopListener(BObject listenerObj) {
        ListenerContext ctx = context(listenerObj);
        Object stopped = stopPolling(ctx);
        if (stopped != null) {
            return stopped;
        }
        releaseActive(ctx);
        return null;
    }

    private static Object stopPolling(ListenerContext ctx) {
        synchronized (ctx) {
            if (ctx.state != State.RUNNING) {
                return BlobErrorCreator.clientError("the listener is not running", null);
            }
            ctx.state = State.STOPPED;
            ctx.poller.stop();
        }
        return null;
    }

    private static void releaseActive(ListenerContext ctx) {
        for (MessageHandle handle : ctx.active) {
            handle.release();
        }
    }

    // Diagnostics go through the module's Ballerina log helpers: slf4j has no binding in any
    // Ballerina distribution, so logging from Java directly would be discarded silently.
    private static void logAt(ListenerContext ctx, String function, String message) {
        Runtime runtime = ctx == null ? null : ctx.runtime;
        if (runtime == null) {
            return;
        }
        try {
            runtime.callFunction(ModuleUtils.getModule(), function, new StrandMetadata(true, null),
                    StringUtils.fromString(message));
        } catch (RuntimeException ignored) {
            // Reporting a diagnostic must never take down the path that raised it.
        }
    }

    static void logWarn(ListenerContext ctx, String message) {
        logAt(ctx, "logListenerWarn", message);
    }

    static void logError(ListenerContext ctx, String message) {
        logAt(ctx, "logListenerError", message);
    }

    static void logDebug(ListenerContext ctx, String message) {
        logAt(ctx, "logListenerDebug", message);
    }

    // Reads the service's handlers, their routing annotations and parameter shapes.
    private static ServiceContext parseService(BObject service, String container, BObject caller) {
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        Map<String, HandlerConfig> handlers = new LinkedHashMap<>();
        int onErrorArity = 0;
        for (MethodType method : serviceType.getMethods()) {
            String methodName = method.getName();
            if (ON_ERROR.equals(methodName)) {
                onErrorArity = method.getParameters().length;
                continue;
            }
            if (!HANDLER_NAMES.contains(methodName)) {
                continue;
            }
            Pattern namePattern = null;
            Pattern contentTypePattern = null;
            BMap<BString, Object> functionConfig = annotation(method.getAnnotations(), FUNCTION_CONFIG_ANNOTATION);
            if (functionConfig != null) {
                namePattern = compile(functionConfig.get(NAME_PATTERN));
                contentTypePattern = compile(functionConfig.get(CONTENT_TYPE_PATTERN));
            }
            Parameter[] params = method.getParameters();
            boolean secondIsCaller = params.length >= 2
                    && CALLER_TYPE_NAME.equals(TypeUtils.getReferredType(params[1].type).getName());
            Type contentType = params.length >= 1 ? params[0].type : null;
            handlers.put(methodName, new HandlerConfig(methodName, namePattern, contentTypePattern,
                    params.length, secondIsCaller, contentType));
        }
        return new ServiceContext(service, container, caller, handlers, onErrorArity);
    }

    private static BMap<BString, Object> annotation(BMap<BString, Object> annotations, String suffix) {
        if (annotations == null) {
            return null;
        }
        for (BString key : annotations.getKeys()) {
            if (key.getValue().endsWith(suffix) && annotations.get(key) instanceof BMap) {
                @SuppressWarnings("unchecked")
                BMap<BString, Object> map = (BMap<BString, Object>) annotations.get(key);
                return map;
            }
        }
        return null;
    }

    private static Pattern compile(Object pattern) {
        if (pattern == null) {
            return null;
        }
        String value = ((BString) pattern).getValue();
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw BlobErrorCreator.clientError("invalid regular expression: " + value, e);
        }
    }

    private static ListenerContext context(BObject listenerObj) {
        return (ListenerContext) listenerObj.getNativeData(NATIVE_LISTENER_CONTEXT);
    }
}
