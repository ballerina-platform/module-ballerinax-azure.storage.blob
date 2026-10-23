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

import io.ballerina.lib.azure.storage.blob.observability.BlobMetricsUtil;
import io.ballerina.lib.azure.storage.blob.observability.BlobObserverContext;
import io.ballerina.lib.azure.storage.blob.observability.BlobTracingUtil;
import io.ballerina.lib.azure.storage.blob.server.MessageHandle.PoisonOutcome;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.observability.tracer.BSpan;

import java.util.Map;

/**
 * The observability record of one received message: its lifecycle span, the stage counters as
 * the dispatcher moves it from found to cleaned up, the two durations, and the settlement calls,
 * which run through here so that each one is counted and spanned with its outcome. Every call
 * is a no-op while observability is disabled; the settlement calls then only delegate.
 */
final class MessageObservation {

    private static final String CLEANUP_SPAN = "file-cleanup";
    private static final String ERROR_SPAN = "file-error";
    private static final String REASON_MESSAGE_NOT_HELD = "message_not_held";

    private final ListenerContext ctx;
    private final ServiceContext svc;
    private final BlobObserverContext lifecycle;
    private String eventType;
    private String handlerName;
    // The message's outcome so far: the last non-success stage, else success at the end.
    private String outcome;
    private String reason;

    MessageObservation(ListenerContext ctx, ServiceContext svc) {
        this.ctx = ctx;
        this.svc = svc;
        this.lifecycle = BlobTracingUtil.createFileLifecycleContext(ctx.remoteUrl, ctx.protocol, svc.queueName());
    }

    /** Records the parsed event's facts; the event type is known only for a dispatchable event. */
    void event(EventParser.Parsed event) {
        if (event.dispatchable()) {
            eventType = event.deleted() ? BlobMetricsUtil.EVENT_TYPE_DELETE : BlobMetricsUtil.EVENT_TYPE_CREATE;
        }
        BlobTracingUtil.addEventToLifecycleSpan(lifecycle, event.path(), eventType, event.contentLength(),
                event.eventTime() == null ? null : event.eventTime().toString());
    }

    void found(String outcome, String reason) {
        stage(BlobMetricsUtil.FILE_STAGE_FOUND, outcome, reason, eventType, null);
    }

    void dispatched(String handler) {
        handlerName = handler;
        stage(BlobMetricsUtil.FILE_STAGE_DISPATCHED, BlobMetricsUtil.OUTCOME_SUCCESS, null, eventType, null);
    }

    void handled(String outcome, String reason) {
        stage(BlobMetricsUtil.FILE_STAGE_HANDLED, outcome, reason, eventType, null);
    }

    void binding(String outcome, long startNanos) {
        BlobMetricsUtil.reportDatabindingDuration(ctx.remoteUrl, ctx.protocol, svc.queueName(), handlerName, outcome,
                elapsedMillis(startNanos));
    }

    void execution(String outcome, long startNanos) {
        BlobMetricsUtil.reportResourceExecutionDuration(ctx.remoteUrl, ctx.protocol, svc.queueName(), handlerName,
                outcome, elapsedMillis(startNanos));
    }

    /** The strand properties of the handler call, parented to the lifecycle span. */
    Map<String, Object> handlerStrand() {
        return BlobTracingUtil.createHandlerStrandProperties(lifecycle, ctx.remoteUrl, ctx.protocol,
                svc.queueName(), handlerName, eventType);
    }

    void acknowledge(MessageHandle handle) {
        BSpan span = cleanupSpan(BlobMetricsUtil.CLEANUP_ACTION_ACKNOWLEDGE);
        Boolean settled = null;
        try {
            settled = handle.acknowledge();
        } finally {
            cleanedUp(span, BlobMetricsUtil.CLEANUP_ACTION_ACKNOWLEDGE, settled,
                    BlobMetricsUtil.REASON_ACKNOWLEDGE_FAILED);
        }
    }

    void redeliver(MessageHandle handle, long delaySeconds) {
        BSpan span = cleanupSpan(BlobMetricsUtil.CLEANUP_ACTION_REDELIVER);
        Boolean settled = null;
        try {
            settled = handle.redeliver(delaySeconds);
        } finally {
            cleanedUp(span, BlobMetricsUtil.CLEANUP_ACTION_REDELIVER, settled,
                    BlobMetricsUtil.REASON_REDELIVER_FAILED);
        }
    }

    PoisonOutcome poison(MessageHandle handle, String reason, String blobPath) {
        BSpan span = cleanupSpan(BlobMetricsUtil.CLEANUP_ACTION_POISON);
        PoisonOutcome outcome = null;
        try {
            outcome = handle.poison(reason, blobPath);
        } finally {
            if (outcome == PoisonOutcome.SKIPPED_LOST) {
                cleanedUp(span, BlobMetricsUtil.CLEANUP_ACTION_POISON, BlobMetricsUtil.OUTCOME_SKIPPED,
                        REASON_MESSAGE_NOT_HELD);
            } else {
                cleanedUp(span, BlobMetricsUtil.CLEANUP_ACTION_POISON, outcome == PoisonOutcome.MOVED,
                        BlobMetricsUtil.REASON_POISON_MOVE_FAILED);
            }
        }
        return outcome;
    }

    /** Closes the lifecycle span with the message's outcome. */
    void finish() {
        BlobTracingUtil.finishFileLifecycleSpan(lifecycle, outcome == null ? BlobMetricsUtil.OUTCOME_SUCCESS : outcome,
                reason);
    }

    /**
     * The {@code onError} span, tagged with the reported error's type: a child of the message's
     * lifecycle span, or a root span without one.
     */
    static BSpan errorSpan(MessageObservation observation, BError error) {
        return BlobTracingUtil.createChildSpan(observation == null ? null : observation.lifecycle, ERROR_SPAN,
                null, null, Listener.ON_ERROR, BlobTracingUtil.errorTypeName(error));
    }

    /** The strand properties of an {@code onError} call, placing the call under its span. */
    static Map<String, Object> errorStrand(ListenerContext ctx, ServiceContext svc, BSpan errorSpan, BError error) {
        return BlobTracingUtil.createErrorStrandProperties(errorSpan, ctx.remoteUrl, ctx.protocol, svc.queueName(),
                BlobTracingUtil.errorTypeName(error));
    }

    private void stage(String stage, String outcome, String reason, String type, String cleanupAction) {
        if (outcome != null && !BlobMetricsUtil.OUTCOME_SUCCESS.equals(outcome)) {
            this.outcome = outcome;
            this.reason = reason;
        }
        BlobMetricsUtil.reportFileStage(ctx.remoteUrl, ctx.protocol, svc.queueName(), stage, outcome, reason,
                handlerName, type, cleanupAction);
    }

    private BSpan cleanupSpan(String action) {
        return BlobTracingUtil.createChildSpan(lifecycle, CLEANUP_SPAN, BlobMetricsUtil.FILE_STAGE_CLEANED_UP,
                action, handlerName, null);
    }

    private void cleanedUp(BSpan span, String action, Boolean settled, String failureReason) {
        boolean ok = Boolean.TRUE.equals(settled);
        cleanedUp(span, action, ok ? BlobMetricsUtil.OUTCOME_SUCCESS : BlobMetricsUtil.OUTCOME_FAILURE,
                ok ? null : failureReason);
    }

    private void cleanedUp(BSpan span, String action, String outcome, String reason) {
        stage(BlobMetricsUtil.FILE_STAGE_CLEANED_UP, outcome, reason, eventType, action);
        BlobTracingUtil.finishSpan(span, outcome, reason);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
