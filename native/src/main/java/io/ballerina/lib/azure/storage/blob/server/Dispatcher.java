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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.queue.models.QueueMessageItem;
import io.ballerina.lib.azure.storage.blob.client.TransferOps;
import io.ballerina.lib.azure.storage.blob.server.ListenerContext.HandlerConfig;
import io.ballerina.lib.azure.storage.blob.server.ListenerContext.ServiceContext;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ContentBinder;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Handles one received message: parses the event, routes it to a service and a handler,
 * fetches and binds the blob's content, invokes the handler, and disposes of the message by
 * the outcome. Every step runs on the message's own virtual thread.
 */
final class Dispatcher {

    private static final BString CALLER_CLIENT_FIELD = StringUtils.fromString("client");
    private static final String NEW_CALLER_FUNCTION = "newCaller";
    private static final String BIND_CSV_CONTENT_FUNCTION = "bindCsvContent";
    private static final String NEW_CONTENT_CSV_STREAM_FUNCTION = "newContentCsvStream";
    private static final String CONTENT_STREAM_GENERATOR_CLASS = "ContentStreamGenerator";
    private static final String NOT_FOUND_ERROR = "NotFoundError";
    private static final String ARCHIVED_BLOB_ERROR = "ArchivedBlobError";

    private static final Map<String, String> EXTENSION_HANDLERS = Map.of(
            "txt", Listener.ON_BLOB_TEXT, "json", Listener.ON_BLOB_JSON,
            "xml", Listener.ON_BLOB_XML, "csv", Listener.ON_BLOB_CSV);
    private static final String JSON_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_JSON + "' handler's declared type";
    private static final String XML_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_XML + "' handler's declared type";
    private static final String XML_PARSE_CONTEXT =
            "content is not valid XML for the '" + Listener.ON_BLOB_XML + "' handler";
    private static final String CSV_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_CSV + "' handler's declared type";
    private static final String LOG_PREFIX = "azure.storage.blob listener: ";

    private Dispatcher() {
    }

    /** Handles one received message end to end; never throws. */
    static void dispatch(ListenerContext ctx, QueueMessageItem message) {
        MessageHandle handle = new MessageHandle(ctx, message);
        ctx.active.add(handle);
        handle.keepInvisible();
        try {
            handleMessage(ctx, handle);
        } catch (Throwable e) {
            Listener.logError(ctx, LOG_PREFIX + "unexpected dispatch failure: " + BallerinaAzureClient.describe(e));
            handle.release();
        }
    }

    private static void handleMessage(ListenerContext ctx, MessageHandle handle) {
        EventParser.Parsed event;
        try {
            event = EventParser.parse(handle.body());
        } catch (BError e) {
            notifyAllOnError(ctx, e);
            fail(ctx, handle, e.getErrorMessage().getValue());
            return;
        }
        if (!event.dispatchable()) {
            Listener.logDebug(ctx, LOG_PREFIX + "acknowledging a " + event.eventType() + " event");
            handle.acknowledge();
            return;
        }
        ServiceContext service = ctx.serviceFor(event.containerName());
        if (service == null) {
            Listener.logDebug(ctx, LOG_PREFIX + "no service for container " + event.containerName()
                    + "; acknowledging the event for " + event.path());
            handle.acknowledge();
            return;
        }
        HandlerConfig handler = event.deleted()
                ? service.handlers().get(Listener.ON_BLOB_DELETED) : resolveHandler(service, event);
        if (handler == null) {
            Listener.logDebug(ctx, LOG_PREFIX + "no handler for " + event.path() + " in container "
                    + event.containerName() + "; acknowledging the event");
            handle.acknowledge();
            return;
        }
        BObject caller;
        try {
            caller = callerFor(ctx, service, event);
        } catch (BError e) {
            // A local configuration failure; nothing about the message itself is wrong.
            notifyOnError(ctx, service, e, null);
            fail(ctx, handle, e.getErrorMessage().getValue());
            return;
        }
        BMap<BString, Object> record = event.toRecord();
        Object[] args;
        if (event.deleted()) {
            args = handler.arity() >= 2 ? new Object[]{record, caller} : new Object[]{record};
        } else {
            Fetch fetch = fetchContent(ctx, caller, handler, event);
            if (fetch.error() != null) {
                notifyOnError(ctx, service, fetch.error(), caller);
                String type = fetch.error().getType().getName();
                if (fetch.bindingFailure() || NOT_FOUND_ERROR.equals(type) || ARCHIVED_BLOB_ERROR.equals(type)) {
                    handle.acknowledge();
                } else {
                    fail(ctx, handle, fetch.error().getErrorMessage().getValue());
                }
                return;
            }
            Object content = fetch.content();
            args = switch (handler.arity()) {
                case 1 -> new Object[]{content};
                case 2 -> handler.secondIsCaller() ? new Object[]{content, caller} : new Object[]{content, record};
                default -> new Object[]{content, record, caller};
            };
        }
        Object result = invoke(ctx, service.service(), handler.methodName(), args);
        if (result instanceof BError error) {
            // The handler already saw its own error; it is printed so the failure stays visible.
            error.printStackTrace();
            fail(ctx, handle, "the " + handler.methodName() + " handler failed for " + event.path()
                    + ": " + error.getErrorMessage());
        } else {
            handle.acknowledge();
        }
    }

    // Redelivers the message, or poisons it once this delivery reached the configured count.
    private static void fail(ListenerContext ctx, MessageHandle handle, String reason) {
        if (handle.dequeueCount() >= ctx.maxDeliveryCount) {
            handle.poison(reason + " (delivery " + handle.dequeueCount() + " of " + ctx.maxDeliveryCount + ")");
        } else {
            handle.redeliver();
        }
    }

    // The Caller a handler or onError receives: the named service's own, or, for the catch-all,
    // the one kept for the event's container, built on its first event.
    private static BObject callerFor(ListenerContext ctx, ServiceContext service, EventParser.Parsed event) {
        if (service.caller() != null) {
            return service.caller();
        }
        BObject cached = ctx.catchAllCallers.get(event.containerName());
        if (cached != null) {
            return cached;
        }
        Object built = ctx.runtime.callFunction(ModuleUtils.getModule(), NEW_CALLER_FUNCTION,
                new StrandMetadata(true, null), StringUtils.fromString(event.containerName()), ctx.clientConfig);
        if (built instanceof BError e) {
            throw e;
        }
        BObject previous = ctx.catchAllCallers.putIfAbsent(event.containerName(), (BObject) built);
        return previous != null ? previous : (BObject) built;
    }

    // Resolves a created event's handler: a routing pattern wins, then the name's extension,
    // then the content type when the name has no extension, then onBlob.
    private static HandlerConfig resolveHandler(ServiceContext service, EventParser.Parsed event) {
        String name = event.blobName();
        String contentType = baseContentType(event.contentType());
        for (String methodName : Listener.ROUTING_ORDER) {
            HandlerConfig handler = service.handlers().get(methodName);
            if (handler == null) {
                continue;
            }
            if (handler.namePattern() != null && handler.namePattern().matcher(name).matches()) {
                return handler;
            }
            if (handler.contentTypePattern() != null && contentType != null
                    && handler.contentTypePattern().matcher(contentType).matches()) {
                return handler;
            }
        }
        String mapped;
        int dot = name.lastIndexOf('.');
        if (dot >= 0) {
            mapped = EXTENSION_HANDLERS.get(name.substring(dot + 1).toLowerCase(Locale.ROOT));
        } else {
            mapped = contentTypeHandler(contentType);
        }
        if (mapped != null && service.handlers().containsKey(mapped)) {
            return service.handlers().get(mapped);
        }
        return service.handlers().get(Listener.ON_BLOB);
    }

    private static String contentTypeHandler(String contentType) {
        if (contentType == null) {
            return null;
        }
        return switch (contentType) {
            case "application/json" -> Listener.ON_BLOB_JSON;
            case "application/xml", "text/xml" -> Listener.ON_BLOB_XML;
            case "text/csv" -> Listener.ON_BLOB_CSV;
            default -> contentType.startsWith("text/") ? Listener.ON_BLOB_TEXT : null;
        };
    }

    // The media type without its parameters, lower-cased.
    private static String baseContentType(String contentType) {
        if (contentType == null) {
            return null;
        }
        int semicolon = contentType.indexOf(';');
        String base = semicolon < 0 ? contentType : contentType.substring(0, semicolon);
        return base.strip().toLowerCase(Locale.ROOT);
    }

    // Fetches the blob through the Caller's client and binds it to the handler's declared type.
    private static Fetch fetchContent(ListenerContext ctx, BObject caller, HandlerConfig handler,
                                      EventParser.Parsed event) {
        BlobClient blob = BallerinaAzureClient.getContainerClient(caller.getObjectValue(CALLER_CLIENT_FIELD))
                .getBlobClient(event.path());
        Type contentType = TypeUtils.getReferredType(handler.contentType());
        if (contentType instanceof StreamType streamType) {
            BlobInputStream stream;
            try {
                stream = blob.openInputStream();
            } catch (RuntimeException e) {
                return Fetch.serviceFailure(BallerinaAzureClient.mapFailure(e));
            }
            BObject generator = ValueCreator.createObjectValue(ModuleUtils.getModule(), CONTENT_STREAM_GENERATOR_CLASS);
            TransferOps.attachContentStream(generator, stream);
            Type constraint = TypeUtils.getReferredType(streamType.getConstrainedType());
            if (!Listener.ON_BLOB_CSV.equals(handler.methodName())) {
                return Fetch.content(ValueCreator.createStreamValue(
                        TypeCreator.createStreamType(constraint, streamType.getCompletionType()), generator));
            }
            BStream byteFeed = ValueCreator.createStreamValue(TypeCreator.createStreamType(
                    TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE),
                    TypeCreator.createUnionType(PredefinedTypes.TYPE_ERROR, PredefinedTypes.TYPE_NULL)), generator);
            Object rows = ctx.runtime.callFunction(ModuleUtils.getModule(), NEW_CONTENT_CSV_STREAM_FUNCTION,
                    new StrandMetadata(true, null), ValueCreator.createTypedescValue(constraint), byteFeed,
                    ctx.laxDataBinding);
            if (rows instanceof BError e) {
                TransferOps.closeQuietly(generator);
                return Fetch.bindingFailure(
                        BlobErrorCreator.clientError(CSV_BIND_CONTEXT + ": " + e.getErrorMessage(), e));
            }
            return Fetch.content(ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(constraint, streamType.getCompletionType()), (BObject) rows));
        }
        byte[] bytes;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.downloadStream(out);
            bytes = out.toByteArray();
        } catch (RuntimeException e) {
            return Fetch.serviceFailure(BallerinaAzureClient.mapFailure(e));
        }
        try {
            return Fetch.content(bind(ctx, handler, bytes));
        } catch (BError e) {
            return Fetch.bindingFailure(e);
        }
    }

    private static Object bind(ListenerContext ctx, HandlerConfig handler, byte[] bytes) {
        return switch (handler.methodName()) {
            case Listener.ON_BLOB_TEXT -> StringUtils.fromString(new String(bytes, StandardCharsets.UTF_8));
            case Listener.ON_BLOB_JSON -> ContentBinder.bindJson(ValueCreator.createArrayValue(bytes),
                    handler.contentType(), ctx.laxDataBinding, JSON_BIND_CONTEXT);
            case Listener.ON_BLOB_XML -> ContentBinder.bindXml(ValueCreator.createArrayValue(bytes),
                    handler.contentType(), ctx.laxDataBinding, XML_BIND_CONTEXT, XML_PARSE_CONTEXT);
            case Listener.ON_BLOB_CSV -> bindCsv(ctx, handler, bytes);
            default -> ValueCreator.createArrayValue(bytes);
        };
    }

    // CSV binds on a Ballerina strand, because the data.csv parser needs one.
    private static Object bindCsv(ListenerContext ctx, HandlerConfig handler, byte[] bytes) {
        Object result = ctx.runtime.callFunction(ModuleUtils.getModule(), BIND_CSV_CONTENT_FUNCTION,
                new StrandMetadata(true, null), ValueCreator.createArrayValue(bytes),
                ValueCreator.createTypedescValue(TypeUtils.getReferredType(handler.contentType())),
                ctx.laxDataBinding);
        if (result instanceof BError e) {
            throw BlobErrorCreator.clientError(CSV_BIND_CONTEXT + ": " + e.getErrorMessage(), e);
        }
        return result;
    }

    // Invokes a service method with the service's isolation; a panic is returned as the error.
    private static Object invoke(ListenerContext ctx, BObject service, String methodName, Object[] args) {
        ObjectType serviceType = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        boolean isConcurrentSafe = serviceType.isIsolated() && serviceType.isIsolated(methodName);
        try {
            return ctx.runtime.callMethod(service, methodName, new StrandMetadata(isConcurrentSafe, null), args);
        } catch (BError panic) {
            return panic;
        } catch (RuntimeException e) {
            return BlobErrorCreator.clientError(methodName + " invocation failed: " + BallerinaAzureClient.describe(e),
                    e);
        }
    }

    /**
     * Notifies every attached service's {@code onError} of a failure that belongs to no event,
     * such as a failed poll. A catch-all {@code onError} that takes a {@code Caller} has no
     * container to bind one to, so it is skipped with a debug log.
     */
    static void notifyAllOnError(ListenerContext ctx, BError error) {
        for (ServiceContext service : ctx.services.values()) {
            notifyOnError(ctx, service, error, service.caller());
        }
        ServiceContext catchAll = ctx.catchAll;
        if (catchAll != null) {
            if (catchAll.onErrorArity() >= 2) {
                Listener.logDebug(ctx, LOG_PREFIX + "the catch-all service's onError takes a Caller, which "
                        + "no container binds for this error; not notified: " + error.getErrorMessage());
            } else {
                notifyOnError(ctx, catchAll, error, null);
            }
        }
    }

    // Notifies one service's onError, if declared, on its own virtual thread.
    private static void notifyOnError(ListenerContext ctx, ServiceContext service, BError error, BObject caller) {
        int arity = service.onErrorArity();
        if (arity == 0 || (arity >= 2 && caller == null)) {
            return;
        }
        Object[] args = arity >= 2 ? new Object[]{error, caller} : new Object[]{error};
        Thread.startVirtualThread(() -> {
            Object result = invoke(ctx, service.service(), Listener.ON_ERROR, args);
            if (result instanceof BError handlerError) {
                handlerError.printStackTrace();
            }
        });
    }

    // The outcome of a content fetch: the bound content, or the failure and whether it was the
    // binding (acknowledged) rather than the service (disposed of by its error type).
    private record Fetch(Object content, BError error, boolean bindingFailure) {

        static Fetch content(Object content) {
            return new Fetch(content, null, false);
        }

        static Fetch serviceFailure(BError error) {
            return new Fetch(null, error, false);
        }

        static Fetch bindingFailure(BError error) {
            return new Fetch(null, error, true);
        }
    }
}
