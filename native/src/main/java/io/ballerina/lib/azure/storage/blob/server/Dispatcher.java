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

import com.azure.storage.queue.models.QueueMessageItem;
import io.ballerina.lib.azure.storage.blob.client.TransferOps;
import io.ballerina.lib.azure.storage.blob.server.ContentFetcher.Fetch;
import io.ballerina.lib.azure.storage.blob.server.MessageHandle.PoisonOutcome;
import io.ballerina.lib.azure.storage.blob.server.ServiceContext.HandlerConfig;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles one received message end to end, in the fixed order of checks: the delivery limit,
 * decoding and parsing, the event type, the account, routing; then the Caller, the content,
 * the handler, and the settlement its return decides. Every step runs on the message's own
 * virtual thread, while the message's visibility is renewed.
 */
final class Dispatcher {

    // The eight poison reasons; the move's error line carries one as its `reason` field.
    static final String REASON_LIMIT = "delivery limit reached";
    static final String REASON_DEAD_LETTERED = "dead-lettered by the handler: ";
    static final String REASON_RETRY_ON_FINAL = "retry requested on the final delivery";
    static final String REASON_WOULD_EXPIRE = "would expire before redelivery";
    static final String REASON_LIMIT_ON_RECEIPT = "delivery limit exceeded on receipt";
    static final String REASON_UNPARSEABLE = "unparseable message";
    static final String REASON_ARCHIVED = "archived blob";
    static final String REASON_BINDING = "content binding failed";

    private static final String CLASSIFY_FUNCTION = "classifyHandlerError";
    private static final BString OUTCOME_KIND = StringUtils.fromString("kind");
    private static final BString OUTCOME_DELAY = StringUtils.fromString("delaySeconds");
    private static final BString OUTCOME_MESSAGE = StringUtils.fromString("message");
    private static final String KIND_DEAD_LETTER = "deadLetter";
    private static final String KIND_RETRY = "retry";
    private static final BString LAST_BINDING_ERROR_FIELD = StringUtils.fromString("lastBindingError");

    // A handler invocation's result: what it returned, or the panic it ended with.
    private record Invocation(Object value, BError panic) {
    }

    private Dispatcher() {
    }

    /** Handles one received message; never throws. */
    static void dispatch(ListenerContext ctx, ServiceContext svc, QueueMessageItem message) {
        OffsetDateTime expiry = message.getExpirationTime();
        MessageHandle handle = new MessageHandle(ctx.log, ctx.timing, ctx.clock, ctx.scheduler, ctx.attempts,
                svc.retrying, svc.retryFree, svc.poison, svc.queueName(), message.getMessageId(),
                message.getPopReceipt(), message.getMessageText(), message.getDequeueCount(),
                expiry == null ? null : expiry.toInstant());
        handle.start();
        try {
            handleMessage(ctx, svc, handle);
        } catch (Throwable e) {
            ctx.log.error("unexpected failure handling message " + handle.messageId() + " on queue "
                    + svc.queueName() + ": " + BallerinaAzureClient.describe(e),
                    ListenerLog.fields("queue", svc.queueName(), "messageId", handle.messageId()));
        } finally {
            handle.dispose();
        }
    }

    private static void handleMessage(ListenerContext ctx, ServiceContext svc, MessageHandle handle) {
        String queue = svc.queueName();
        if (handle.dequeueCount() > svc.settings.maxDeliveryCount()) {
            pastLimit(ctx, svc, handle);
            return;
        }
        EventParser.Parsed event;
        try {
            event = EventParser.parse(handle.body());
        } catch (EventParser.EventParseException parseFailure) {
            BError error = BlobErrorCreator.clientError("message " + handle.messageId() + " on queue " + queue
                    + " is not a storage event: " + parseFailure.getMessage(), parseFailure.getCause());
            notifyOnError(ctx, svc, handle, error, null, null);
            afterPoison(ctx, svc, handle, handle.poison(REASON_UNPARSEABLE, null), null, null);
            return;
        }
        if (!event.dispatchable()) {
            neverOurs(ctx, svc, Routing.typeKey(event.eventType()), Routing.typeWarning(queue, event.eventType()));
            handle.acknowledge();
            return;
        }
        if (event.noContent()) {
            neverOurs(ctx, svc, Routing.noContentKey(event), Routing.noContentWarning(queue, event));
            handle.acknowledge();
            return;
        }
        if (ctx.isForeign(event.accountName(), queue)) {
            handle.acknowledge();
            return;
        }
        HandlerConfig handler = event.deleted() ? svc.handlers.get(Listener.ON_BLOB_DELETED)
                : Routing.resolve(svc.handlers, event.blobName(), event.contentType());
        if (handler == null) {
            if (event.deleted()) {
                neverOurs(ctx, svc, Routing.typeKey(event.eventType()), Routing.deletedWarning(queue));
            } else {
                neverOurs(ctx, svc, Routing.UNROUTED_KEY, Routing.unroutedWarning(queue, event.path(), svc.handlers));
            }
            handle.acknowledge();
            return;
        }
        BMap<BString, Object> record = event.toRecord();
        BObject caller;
        try {
            caller = ctx.callerFor(event.containerName());
        } catch (BError e) {
            notifyOnError(ctx, svc, handle, e, record, null);
            ordinaryFailure(ctx, svc, handle, event, record, null);
            return;
        }
        Object[] args;
        BObject streamGenerator = null;
        BObject csvStream = null;
        if (event.deleted()) {
            args = handler.takesCaller() ? new Object[]{record, caller} : new Object[]{record};
        } else {
            Fetch fetch = ContentFetcher.fetch(ctx, caller, handler, event);
            switch (fetch.disposition()) {
                case NOT_FOUND -> {
                    notifyOnError(ctx, svc, handle, fetch.error(), record, caller);
                    handle.acknowledge();
                    return;
                }
                case ARCHIVED -> {
                    notifyOnError(ctx, svc, handle, fetch.error(), record, caller);
                    afterPoison(ctx, svc, handle, handle.poison(REASON_ARCHIVED, event.path()), record, caller);
                    return;
                }
                case BINDING -> {
                    notifyOnError(ctx, svc, handle, fetch.error(), record, caller);
                    afterPoison(ctx, svc, handle, handle.poison(REASON_BINDING, event.path()), record, caller);
                    return;
                }
                case ORDINARY -> {
                    notifyOnError(ctx, svc, handle, fetch.error(), record, caller);
                    ordinaryFailure(ctx, svc, handle, event, record, caller);
                    return;
                }
                default -> {
                    List<Object> list = new ArrayList<>();
                    list.add(fetch.content());
                    if (handler.takesEvent()) {
                        list.add(record);
                    }
                    if (handler.takesCaller()) {
                        list.add(caller);
                    }
                    args = list.toArray();
                    streamGenerator = fetch.streamGenerator();
                    csvStream = fetch.csvStream();
                }
            }
        }
        Invocation result = invoke(ctx, svc, handler.methodName(), args);
        settle(ctx, svc, handle, event, record, caller, handler, result, streamGenerator, csvStream);
    }

    // Settles the message by the handler's return: nil acknowledges, a DeadLetterError poisons
    // now, a RetryError redelivers after its delay, a stream's own binding error is a binding
    // failure, and any other error or a panic is an ordinary failure.
    private static void settle(ListenerContext ctx, ServiceContext svc, MessageHandle handle,
                               EventParser.Parsed event, BMap<BString, Object> record, BObject caller,
                               HandlerConfig handler, Invocation result, BObject streamGenerator, BObject csvStream) {
        if (result.panic() != null) {
            warnHandlerFailure(ctx, svc, handle, handler, event, "panicked: " + result.panic().getErrorMessage(), null);
            ordinaryFailure(ctx, svc, handle, event, record, caller);
            return;
        }
        if (!(result.value() instanceof BError error)) {
            handle.acknowledge();
            return;
        }
        @SuppressWarnings("unchecked")
        BMap<BString, Object> outcome = (BMap<BString, Object>) ctx.runtime.callFunction(ModuleUtils.getModule(),
                CLASSIFY_FUNCTION, new StrandMetadata(true, null), error);
        String kind = outcome.getStringValue(OUTCOME_KIND).getValue();
        String message = outcome.getStringValue(OUTCOME_MESSAGE).getValue();
        if (KIND_DEAD_LETTER.equals(kind)) {
            afterPoison(ctx, svc, handle, handle.poison(REASON_DEAD_LETTERED + message, event.path()), record, caller);
            return;
        }
        if (KIND_RETRY.equals(kind)) {
            long delay = Delays.seconds(((BDecimal) outcome.get(OUTCOME_DELAY)).decimalValue());
            if (handle.dequeueCount() >= svc.settings.maxDeliveryCount()) {
                warnHandlerFailure(ctx, svc, handle, handler, event, message,
                        "delay not applied: " + REASON_RETRY_ON_FINAL);
                afterPoison(ctx, svc, handle, handle.poison(REASON_RETRY_ON_FINAL, event.path()), record, caller);
            } else if (handle.wouldExpire(delay)) {
                warnHandlerFailure(ctx, svc, handle, handler, event, message,
                        "delay not applied: " + REASON_WOULD_EXPIRE);
                afterPoison(ctx, svc, handle, handle.poison(REASON_WOULD_EXPIRE, event.path()), record, caller);
            } else {
                warnHandlerFailure(ctx, svc, handle, handler, event, message,
                        "delay applied: " + delay + " seconds");
                handle.redeliver(delay);
            }
            return;
        }
        if (csvStream != null && TransferOps.streamFailure(streamGenerator) == null
                && isTheStreamsOwnBindingError(error, csvStream.get(LAST_BINDING_ERROR_FIELD))) {
            BError binding = BlobErrorCreator.contentBindingError(error.getErrorMessage().getValue(), error,
                    event.containerName(), event.path(), null);
            notifyOnError(ctx, svc, handle, binding, record, caller);
            afterPoison(ctx, svc, handle, handle.poison(REASON_BINDING, event.path()), record, caller);
            return;
        }
        warnHandlerFailure(ctx, svc, handle, handler, event, message, null);
        ordinaryFailure(ctx, svc, handle, event, record, caller);
    }

    // Whether the error a CSV stream handler returned is the binding error its stream produced.
    // A query over the stream surfaces a copy of that error (the runtime re-creates the entry
    // with the same message), so the error is recognised by identity or by its message, the
    // wrapped one or the parser's own.
    private static boolean isTheStreamsOwnBindingError(BError returned, Object recorded) {
        if (!(recorded instanceof BError last)) {
            return false;
        }
        if (returned == last) {
            return true;
        }
        String message = returned.getErrorMessage().getValue();
        for (BError current = last; current != null; current = current.getCause()) {
            if (message.equals(current.getErrorMessage().getValue())) {
                return true;
            }
        }
        return false;
    }

    // Redelivers after the service's delay, or poisons the message on its final delivery or when
    // the delay would outlive it.
    private static void ordinaryFailure(ListenerContext ctx, ServiceContext svc, MessageHandle handle,
                                        EventParser.Parsed event, BMap<BString, Object> record, BObject caller) {
        String path = event == null ? null : event.path();
        if (handle.dequeueCount() >= svc.settings.maxDeliveryCount()) {
            afterPoison(ctx, svc, handle, handle.poison(REASON_LIMIT, path), record, caller);
            return;
        }
        long delay = svc.settings.redeliverySeconds();
        if (handle.wouldExpire(delay)) {
            afterPoison(ctx, svc, handle, handle.poison(REASON_WOULD_EXPIRE, path), record, caller);
            return;
        }
        handle.redeliver(delay);
    }

    // A message received past its delivery limit: notified and moved without dispatching; the
    // event and Caller go with the notification only when it parses as ours.
    private static void pastLimit(ListenerContext ctx, ServiceContext svc, MessageHandle handle) {
        EventParser.Parsed event = null;
        try {
            event = EventParser.parse(handle.body());
        } catch (EventParser.EventParseException ignored) {
            // Past the limit, an unparseable message gets no separate parse notification.
        }
        BMap<BString, Object> record = null;
        BObject caller = null;
        if (event != null && event.dispatchable() && !event.noContent() && ctx.sameAccount(event.accountName())) {
            record = event.toRecord();
            try {
                caller = ctx.callerFor(event.containerName());
            } catch (BError ignored) {
                caller = null;
            }
        }
        BError error = BlobErrorCreator.clientError(REASON_LIMIT_ON_RECEIPT + ": message " + handle.messageId()
                + " on queue " + svc.queueName() + " was delivered " + handle.dequeueCount()
                + " times without settling (maxDeliveryCount " + svc.settings.maxDeliveryCount() + ")", null);
        notifyOnError(ctx, svc, handle, error, record, caller);
        afterPoison(ctx, svc, handle, handle.poison(REASON_LIMIT_ON_RECEIPT, event == null ? null : event.path()),
                record, caller);
    }

    // A failed move, or one skipped after a transient failure of its hold check, notifies onError
    // with an error naming the poison queue; the message is disposed of by the caller afterwards.
    private static void afterPoison(ListenerContext ctx, ServiceContext svc, MessageHandle handle,
                                    PoisonOutcome outcome, BMap<BString, Object> record, BObject caller) {
        if (outcome != PoisonOutcome.FAILED && outcome != PoisonOutcome.SKIPPED_TRANSIENT) {
            return;
        }
        String why = outcome == PoisonOutcome.FAILED ? "" : " after a transient failure of its hold check";
        BError error = BlobErrorCreator.clientError("message " + handle.messageId() + " on queue " + svc.queueName()
                + " could not be moved to the poison queue '" + svc.poison.name() + "'" + why
                + "; it returns when its visibility window expires", null);
        notifyOnError(ctx, svc, handle, error, record, caller);
    }

    private static void warnHandlerFailure(ListenerContext ctx, ServiceContext svc, MessageHandle handle,
                                           HandlerConfig handler, EventParser.Parsed event, String message,
                                           String delayNote) {
        String delivery = handle.dequeueCount() + " of " + svc.settings.maxDeliveryCount();
        ctx.log.warn("the " + handler.methodName() + " handler failed for " + event.path() + " (delivery " + delivery
                + "): " + message + (delayNote == null ? "" : "; " + delayNote),
                ListenerLog.fields("queue", svc.queueName(), "handler", handler.methodName(), "path", event.path(),
                        "delivery", delivery, "messageId", handle.messageId()));
    }

    // The first event of a kind no handler takes warns with its filter; repeats log at debug level.
    private static void neverOurs(ListenerContext ctx, ServiceContext svc, String key, String warning) {
        long seen = svc.neverOurs.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
        Map<String, Object> fields = ListenerLog.fields("queue", svc.queueName(), "kind", key, "count", seen);
        if (seen == 1) {
            ctx.log.warn(warning, fields);
        } else {
            ctx.log.debug("another event no handler of queue " + svc.queueName() + " takes was acknowledged (" + key
                    + ", " + seen + " so far)", fields);
        }
    }

    /**
     * Notifies the service's {@code onError}, if declared, and waits for it: with the event and
     * the Caller where the shape takes them, or {@code ()} where the failure carries none. A
     * service without {@code onError} gets a warning instead. The call never settles the
     * message; an error it returns is logged.
     */
    static void notifyOnError(ListenerContext ctx, ServiceContext svc, MessageHandle handle, BError error,
                              BMap<BString, Object> record, BObject caller) {
        Map<String, Object> fields = ListenerLog.fields("queue", svc.queueName(),
                "messageId", handle == null ? null : handle.messageId());
        if (svc.onError == null) {
            ctx.log.warn("the service of queue " + svc.queueName() + " declares no onError; " + error.getErrorMessage(),
                    fields);
            return;
        }
        List<Object> args = new ArrayList<>();
        args.add(error);
        if (svc.onError.takesEvent()) {
            args.add(record);
        }
        if (svc.onError.takesCaller()) {
            args.add(caller);
        }
        if (handle != null) {
            handle.activity("onError call");
        }
        Invocation result = invoke(ctx, svc, Listener.ON_ERROR, args.toArray());
        if (result.panic() != null) {
            ctx.log.warn("onError panicked; the outcome is unchanged: " + result.panic().getErrorMessage(), fields);
        } else if (result.value() instanceof BError returned) {
            ctx.log.warn("onError returned an error; the outcome is unchanged: " + returned.getErrorMessage(), fields);
        }
        if (handle != null) {
            handle.activity("handler");
        }
    }

    // Invokes a service method with the service's isolation; a panic is kept apart from a return.
    private static Invocation invoke(ListenerContext ctx, ServiceContext svc, String methodName, Object[] args) {
        try {
            return new Invocation(ctx.runtime.callMethod(svc.service, methodName,
                    new StrandMetadata(svc.concurrentSafe(methodName), null), args), null);
        } catch (BError panic) {
            // A panic's stack trace is the user's only way to locate it; the warning line that
            // follows carries the handler, the path, and the message.
            panic.printStackTrace();
            return new Invocation(null, panic);
        } catch (RuntimeException e) {
            return new Invocation(null, BlobErrorCreator.clientError(methodName + " invocation failed: "
                    + BallerinaAzureClient.describe(e), e));
        }
    }
}
