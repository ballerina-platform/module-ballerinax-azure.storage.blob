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

package io.ballerina.lib.azure.storage.blob.observability;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.ObserverContext;
import io.ballerina.runtime.observability.tracer.BSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNullElse;

/**
 * Puts the connector's tags on traces. Client operations enrich the span the runtime opens for
 * the remote call; the listener opens one {@code file-lifecycle} span per received message,
 * hands it to the handler's strand as the parent of the runtime's call span, and opens
 * {@code file-cleanup} and {@code file-error} children for the settlement and {@code onError}.
 * Every method is a no-op while observability is disabled and never throws.
 */
public final class BlobTracingUtil {

    private static final Logger log = LoggerFactory.getLogger(BlobTracingUtil.class);
    private static final String LIFECYCLE_SPAN = "file-lifecycle";
    private static final String NONE = BlobMetricsUtil.NONE;

    private BlobTracingUtil() {
    }

    /**
     * Opens the parent span of one received message.
     *
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param watchedPath the service's queue name
     * @return the context holding the span, or {@code null} while observability is disabled
     */
    public static BlobObserverContext createFileLifecycleContext(String url, String protocol, String watchedPath) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            BlobObserverContext ctx = new BlobObserverContext(BlobMetricsUtil.CONTEXT_LISTENER, url, protocol);
            ctx.addTag(BlobObserverContext.TAG_ACTION_TYPE, BlobMetricsUtil.ACTION_TYPE_EVENT);
            ctx.addTag(BlobObserverContext.TAG_WATCHED_PATH, requireNonNullElse(watchedPath, NONE));
            BSpan span = BSpan.start(BlobMetricsUtil.MODULE, LIFECYCLE_SPAN, false);
            for (var tag : ctx.getAllTags()) {
                span.addTag(tag.getKey(), tag.getValue());
            }
            ctx.setSpan(span);
            return ctx;
        } catch (Throwable t) {
            log.debug("Failed to create the file lifecycle context", t);
            return null;
        }
    }

    /**
     * Adds the event's facts to the lifecycle span once the message has been parsed.
     *
     * @param parentCtx    the lifecycle context, or {@code null}
     * @param filePath     the blob path
     * @param eventType    {@code create} or {@code delete}
     * @param fileSize     the content length, or {@code null}
     * @param modifiedTime the event time, or {@code null}
     */
    public static void addEventToLifecycleSpan(BlobObserverContext parentCtx, String filePath, String eventType,
                                               Long fileSize, String modifiedTime) {
        if (parentCtx == null || parentCtx.getSpan() == null) {
            return;
        }
        try {
            BSpan span = parentCtx.getSpan();
            if (filePath != null) {
                span.addTag(BlobObserverContext.TAG_FILE_PATH, filePath);
            }
            if (eventType != null) {
                span.addTag(BlobObserverContext.TAG_EVENT_TYPE, eventType);
            }
            if (fileSize != null) {
                span.addTag(BlobObserverContext.TAG_FILE_SIZE, String.valueOf(fileSize));
            }
            if (modifiedTime != null) {
                span.addTag(BlobObserverContext.TAG_FILE_MODIFIED_TIME, modifiedTime);
            }
        } catch (Throwable t) {
            log.debug("Failed to tag the file lifecycle span", t);
        }
    }

    /**
     * Closes the parent span of a received message with the message's outcome.
     *
     * @param parentCtx the lifecycle context, or {@code null}
     * @param outcome   the message's final outcome
     * @param errorType the reason or error type behind a non-success outcome, or {@code null}
     */
    public static void finishFileLifecycleSpan(BlobObserverContext parentCtx, String outcome, String errorType) {
        if (parentCtx == null) {
            return;
        }
        finishSpan(parentCtx.getSpan(), outcome, errorType);
    }

    /**
     * Opens a child span of the lifecycle span, or a root span when the message has no lifecycle
     * span, as for the {@code onError} call after a failed poll.
     *
     * @param parentCtx     the lifecycle context, or {@code null}
     * @param operation     the span name, {@code file-cleanup} or {@code file-error}
     * @param fileStage     the stage the span covers, or {@code null}
     * @param cleanupAction the settlement, or {@code null}
     * @param handlerName   the handler involved, or {@code null}
     * @return the span, or {@code null} while observability is disabled
     */
    public static BSpan createChildSpan(BlobObserverContext parentCtx, String operation, String fileStage,
                                        String cleanupAction, String handlerName) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            BSpan parent = parentCtx == null ? null : parentCtx.getSpan();
            BSpan span = parent == null ? BSpan.start(BlobMetricsUtil.MODULE, operation, false)
                    : BSpan.start(parent.extractContextAsHttpHeaders(), BlobMetricsUtil.MODULE, operation, false);
            span.addTag(BlobObserverContext.TAG_MODULE, BlobMetricsUtil.MODULE);
            span.addTag(BlobObserverContext.TAG_CONTEXT, BlobMetricsUtil.CONTEXT_LISTENER);
            span.addTag(BlobObserverContext.TAG_ACTION_TYPE, BlobMetricsUtil.ACTION_TYPE_EVENT);
            if (parentCtx != null) {
                copyTag(parentCtx, span, BlobObserverContext.TAG_REMOTE_URL);
                copyTag(parentCtx, span, BlobObserverContext.TAG_PROTOCOL);
                copyTag(parentCtx, span, BlobObserverContext.TAG_WATCHED_PATH);
            }
            span.addTag(BlobObserverContext.TAG_INSTANCE_URL,
                    requireNonNullElse(BlobMetricsUtil.getInstanceUrl(), NONE));
            span.addTag(BlobObserverContext.TAG_FILE_STAGE, requireNonNullElse(fileStage, NONE));
            span.addTag(BlobObserverContext.TAG_CLEANUP_ACTION, requireNonNullElse(cleanupAction, NONE));
            span.addTag(BlobObserverContext.TAG_HANDLER_NAME, requireNonNullElse(handlerName, NONE));
            return span;
        } catch (Throwable t) {
            log.debug("Failed to create the {} span", operation, t);
            return null;
        }
    }

    /**
     * Tags a span with its outcome and closes it.
     *
     * @param span      the span, or {@code null}
     * @param outcome   one of the {@code OUTCOME_*} values
     * @param errorType the error type or reason, or {@code null}
     */
    public static void finishSpan(BSpan span, String outcome, String errorType) {
        if (span == null) {
            return;
        }
        try {
            span.addTag(BlobObserverContext.TAG_OUTCOME, outcome);
            if (errorType != null) {
                span.addTag(BlobObserverContext.TAG_ERROR_TYPE, errorType);
            }
        } catch (Throwable t) {
            log.debug("Failed to tag the span outcome", t);
        }
        finishSpan(span);
    }

    private static void finishSpan(BSpan span) {
        if (span == null) {
            return;
        }
        try {
            span.finishSpan();
        } catch (Throwable t) {
            log.debug("Failed to finish the span", t);
        }
    }

    /**
     * Builds the strand properties of a handler call: an observer context the runtime adopts for
     * the call's span, parented to the given context. The runtime copies the context's tags
     * into its request metrics, so only bounded values go here; the per-blob facts stay on the
     * lifecycle span.
     *
     * @param parentCtx   the context whose span becomes the call's parent, or {@code null}
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param watchedPath the service's queue name
     * @param handlerName the handler being called
     * @param eventType   {@code create}, {@code delete} or {@code error}
     * @return the properties, or {@code null} while observability is disabled
     */
    public static Map<String, Object> createHandlerStrandProperties(BlobObserverContext parentCtx, String url,
                                                                    String protocol, String watchedPath,
                                                                    String handlerName, String eventType) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            BlobObserverContext ctx = new BlobObserverContext(BlobMetricsUtil.CONTEXT_LISTENER, url, protocol);
            ctx.addTag(BlobObserverContext.TAG_ACTION_TYPE, BlobMetricsUtil.ACTION_TYPE_EVENT);
            ctx.addTag(BlobObserverContext.TAG_WATCHED_PATH, requireNonNullElse(watchedPath, NONE));
            ctx.addTag(BlobObserverContext.TAG_HANDLER_NAME, requireNonNullElse(handlerName, NONE));
            ctx.addTag(BlobObserverContext.TAG_EVENT_TYPE, requireNonNullElse(eventType, NONE));
            if (parentCtx != null) {
                ctx.setParent(parentCtx);
            }
            Map<String, Object> properties = new HashMap<>();
            properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, ctx);
            return properties;
        } catch (Throwable t) {
            log.debug("Failed to create the handler strand properties", t);
            return null;
        }
    }

    /**
     * Builds the strand properties of an {@code onError} call: an error event naming the error's
     * type, parented to the call's {@code file-error} span, so the call shows under it. The
     * call's own outcome goes on that span once it returns.
     *
     * @param errorSpan   the {@code file-error} span, or {@code null}
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param watchedPath the service's queue name
     * @param errorType   the type name of the error being reported
     * @return the properties, or {@code null} while observability is disabled
     */
    public static Map<String, Object> createErrorStrandProperties(BSpan errorSpan, String url, String protocol,
                                                                  String watchedPath, String errorType) {
        BlobObserverContext parentCtx = null;
        if (errorSpan != null) {
            parentCtx = new BlobObserverContext(BlobMetricsUtil.CONTEXT_LISTENER, url, protocol);
            parentCtx.setSpan(errorSpan);
        }
        Map<String, Object> properties = createHandlerStrandProperties(parentCtx, url, protocol, watchedPath,
                "onError", BlobMetricsUtil.EVENT_TYPE_ERROR);
        if (properties == null) {
            return null;
        }
        try {
            ObserverContext ctx = (ObserverContext) properties.get(ObservabilityConstants.KEY_OBSERVER_CONTEXT);
            ctx.addTag(BlobObserverContext.TAG_ERROR_TYPE, requireNonNullElse(errorType, NONE));
        } catch (Throwable t) {
            log.debug("Failed to create the error strand properties", t);
        }
        return properties;
    }

    /**
     * Tags the span the runtime opened for the running remote call with the operation's identity.
     *
     * @param env             the Ballerina runtime environment
     * @param url             the remote host
     * @param protocol        the wire protocol
     * @param operationType   {@code get}, {@code put} or {@code manage}
     * @param filePath        the blob or container path, or {@code null}
     * @param destinationPath the destination path of a copy, or {@code null}
     */
    public static void sendMetricsData(Environment env, String url, String protocol, String operationType,
                                       String filePath, String destinationPath) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return;
        }
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return;
            }
            ctx.addTag(BlobObserverContext.TAG_MODULE, BlobMetricsUtil.MODULE);
            ctx.addTag(BlobObserverContext.TAG_ACTION_TYPE, BlobMetricsUtil.ACTION_TYPE_OPERATION);
            ctx.addTag(BlobObserverContext.TAG_CONTEXT, BlobMetricsUtil.CONTEXT_CLIENT);
            ctx.addTag(BlobObserverContext.TAG_REMOTE_URL, requireNonNullElse(url, NONE));
            ctx.addTag(BlobObserverContext.TAG_PROTOCOL, requireNonNullElse(protocol, NONE));
            ctx.addTag(BlobObserverContext.TAG_OPERATION_TYPE, operationType);
            ctx.addTag(BlobObserverContext.TAG_INSTANCE_URL,
                    requireNonNullElse(BlobMetricsUtil.getInstanceUrl(), NONE));
            BSpan span = ctx.getSpan();
            if (span != null) {
                if (filePath != null) {
                    span.addTag(BlobObserverContext.TAG_FILE_PATH, filePath);
                }
                if (destinationPath != null) {
                    span.addTag(BlobObserverContext.TAG_DESTINATION_PATH, destinationPath);
                }
            }
        } catch (Throwable t) {
            log.debug("Failed to tag the operation span", t);
        }
    }

    /**
     * Tags the running remote call's span with the operation's outcome and returns the result
     * unchanged.
     *
     * @param result the operation's result
     * @param env    the Ballerina runtime environment
     * @return {@code result}
     */
    public static Object sendTraces(Object result, Environment env) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return result;
        }
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return result;
            }
            if (result instanceof BError error) {
                String errorType = errorTypeName(error);
                ctx.addTag(ObservabilityConstants.TAG_KEY_ERROR, ObservabilityConstants.TAG_TRUE_VALUE);
                ctx.addTag(BlobObserverContext.TAG_ERROR_TYPE, errorType);
                ctx.addTag(BlobObserverContext.TAG_OUTCOME, BlobMetricsUtil.OUTCOME_FAILURE);
            } else {
                ctx.addTag(BlobObserverContext.TAG_OUTCOME, BlobMetricsUtil.OUTCOME_SUCCESS);
            }
        } catch (Throwable t) {
            log.debug("Failed to tag the operation outcome", t);
        }
        return result;
    }

    /**
     * The type name of an error for the {@code error.type} tag.
     *
     * @param error the error
     * @return its type name, or {@code unknown}
     */
    public static String errorTypeName(BError error) {
        return error != null && error.getType() != null ? error.getType().getName() : BlobMetricsUtil.UNKNOWN;
    }

    private static void copyTag(BlobObserverContext source, BSpan target, String key) {
        if (source.getTag(key) != null) {
            target.addTag(key, source.getTag(key).getValue());
        }
    }
}
