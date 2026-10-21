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

import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.metrics.DefaultMetricRegistry;
import io.ballerina.runtime.observability.metrics.MetricId;
import io.ballerina.runtime.observability.metrics.MetricRegistry;
import io.ballerina.runtime.observability.metrics.StatisticConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;

import static java.util.Objects.requireNonNullElse;

/**
 * Records the connector's metrics in the file-module vocabulary: the connection gauge, the bytes
 * counter, the event counter (poll cycles and the four stages of a received message), and the
 * two duration gauges. Every method is a no-op while metrics are disabled and never throws, so a
 * reporting failure cannot change an operation's outcome.
 */
public final class BlobMetricsUtil {

    private static final Logger log = LoggerFactory.getLogger(BlobMetricsUtil.class);

    private static final String METRIC_ACTIVE_CONNECTIONS = "file_active_connections";
    private static final String METRIC_ACTIVE_CONNECTIONS_DESC = "Number of active Azure Blob Storage connections";
    private static final String METRIC_BYTES_TRANSFERRED = "file_bytes_transferred_total";
    private static final String METRIC_BYTES_TRANSFERRED_DESC = "Total bytes read or written across operations";
    private static final String METRIC_FILE_EVENTS = "file_events_total";
    private static final String METRIC_FILE_EVENTS_DESC = "Total blob event lifecycle and poll events";
    private static final String METRIC_DATABINDING_DURATION = "file_databinding_duration_seconds";
    private static final String METRIC_DATABINDING_DURATION_DESC = "Time taken to fetch and bind blob content";
    private static final String METRIC_RESOURCE_EXECUTION_DURATION = "file_resource_execution_duration_seconds";
    private static final String METRIC_RESOURCE_EXECUTION_DURATION_DESC = "Time taken to execute the handler method";

    private static final StatisticConfig DURATION_STATISTIC_CONFIG = StatisticConfig.builder()
            .percentiles(0.5, 0.75, 0.9, 0.95, 0.99)
            .expiry(Duration.ofMinutes(5))
            .buckets(10)
            .build();

    /** The module tag value. */
    public static final String MODULE = "azure_blob";
    /** Tag value when a URL or protocol is unavailable. */
    public static final String UNKNOWN = "unknown";
    /** Tag value for a key that does not apply to an increment, so every series has the same keys. */
    public static final String NONE = "none";

    public static final String CONTEXT_CLIENT = "client";
    public static final String CONTEXT_LISTENER = "listener";

    public static final String OPERATION_TYPE_GET = "get";
    public static final String OPERATION_TYPE_PUT = "put";
    public static final String OPERATION_TYPE_MANAGE = "manage";

    public static final String ACTION_TYPE_OPERATION = "client_operation";
    public static final String ACTION_TYPE_EVENT = "file_event";
    public static final String ACTION_TYPE_POLL = "poll_cycle";

    public static final String FILE_STAGE_FOUND = "found";
    public static final String FILE_STAGE_DISPATCHED = "dispatched";
    public static final String FILE_STAGE_HANDLED = "handled";
    public static final String FILE_STAGE_CLEANED_UP = "cleaned_up";

    public static final String EVENT_TYPE_CREATE = "create";
    public static final String EVENT_TYPE_DELETE = "delete";
    public static final String EVENT_TYPE_ERROR = "error";

    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";
    public static final String OUTCOME_SKIPPED = "skipped";

    // The three settlements a message can end in.
    public static final String CLEANUP_ACTION_ACKNOWLEDGE = "acknowledge";
    public static final String CLEANUP_ACTION_REDELIVER = "redeliver";
    public static final String CLEANUP_ACTION_POISON = "poison";

    // Reasons on the found stage.
    public static final String REASON_DELIVERY_LIMIT_EXCEEDED = "delivery_limit_exceeded";
    public static final String REASON_UNPARSEABLE_MESSAGE = "unparseable_message";
    public static final String REASON_EVENT_TYPE_NOT_DISPATCHED = "event_type_not_dispatched";
    public static final String REASON_NO_CONTENT_EVENT = "no_content_event";
    public static final String REASON_FOREIGN_ACCOUNT = "foreign_account";
    public static final String REASON_NO_HANDLER_MATCHED = "no_handler_matched";
    // Reasons on the handled stage when the content could not be fetched or bound.
    public static final String REASON_BLOB_NOT_FOUND = "blob_not_found";
    public static final String REASON_ARCHIVED_BLOB = "archived_blob";
    public static final String REASON_BINDING_FAILED = "binding_failed";
    // Reasons on the cleaned_up stage.
    public static final String REASON_ACKNOWLEDGE_FAILED = "acknowledge_failed";
    public static final String REASON_REDELIVER_FAILED = "redeliver_failed";
    public static final String REASON_POISON_MOVE_FAILED = "poison_move_failed";

    private static final MetricRegistry metricRegistry = DefaultMetricRegistry.getInstance();

    private static final class Host {
        static final String NAME = resolve();

        private static String resolve() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return null;
            }
        }
    }

    private BlobMetricsUtil() {
    }

    /** The local host name, resolved once; {@code null} when it cannot be resolved. */
    public static String getInstanceUrl() {
        return Host.NAME;
    }

    /**
     * Raises the connection gauge for a new client or listener.
     *
     * @param url      the remote host, with the port when explicit
     * @param protocol the wire protocol
     * @param context  {@link #CONTEXT_CLIENT} or {@link #CONTEXT_LISTENER}
     */
    public static void reportNewConnection(String url, String protocol, String context) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = new BlobObserverContext(context, url, protocol);
            metricRegistry.gauge(new MetricId(METRIC_ACTIVE_CONNECTIONS, METRIC_ACTIVE_CONNECTIONS_DESC,
                    observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report the new connection metric", t);
        }
    }

    /**
     * Lowers the connection gauge when a listener releases its transport.
     *
     * @param url      the remote host
     * @param protocol the wire protocol
     * @param context  {@link #CONTEXT_CLIENT} or {@link #CONTEXT_LISTENER}
     */
    public static void reportConnectionClose(String url, String protocol, String context) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = new BlobObserverContext(context, url, protocol);
            metricRegistry.gauge(new MetricId(METRIC_ACTIVE_CONNECTIONS, METRIC_ACTIVE_CONNECTIONS_DESC,
                    observerContext.getAllTags())).decrement();
        } catch (Throwable t) {
            log.debug("Failed to report the connection close metric", t);
        }
    }

    /**
     * Counts bytes moved through the connector by a content read or write.
     *
     * @param url           the remote host
     * @param protocol      the wire protocol
     * @param context       {@link #CONTEXT_CLIENT} or {@link #CONTEXT_LISTENER}
     * @param operationType {@link #OPERATION_TYPE_GET} or {@link #OPERATION_TYPE_PUT}
     * @param bytes         the byte count; nothing is recorded for zero or less
     */
    public static void reportBytesTransferred(String url, String protocol, String context, String operationType,
                                              long bytes) {
        if (bytes <= 0 || !ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = new BlobObserverContext(context, url, protocol);
            observerContext.addTag(BlobObserverContext.TAG_OPERATION_TYPE, operationType);
            metricRegistry.counter(new MetricId(METRIC_BYTES_TRANSFERRED, METRIC_BYTES_TRANSFERRED_DESC,
                    observerContext.getAllTags())).increment(bytes);
        } catch (Throwable t) {
            log.debug("Failed to report the bytes transferred metric", t);
        }
    }

    /**
     * Counts one stage of a received message's lifecycle.
     *
     * @param url           the remote host
     * @param protocol      the wire protocol
     * @param watchedPath   the service's queue name
     * @param fileStage     one of the {@code FILE_STAGE_*} values
     * @param outcome       one of the {@code OUTCOME_*} values
     * @param errorType     the error type or reason, or {@code null}
     * @param handlerName   the handler the event was routed to, or {@code null}
     * @param eventType     {@link #EVENT_TYPE_CREATE} or {@link #EVENT_TYPE_DELETE}, or {@code null}
     * @param cleanupAction the settlement on the {@code cleaned_up} stage, or {@code null}
     */
    public static void reportFileStage(String url, String protocol, String watchedPath, String fileStage,
                                       String outcome, String errorType, String handlerName, String eventType,
                                       String cleanupAction) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = eventContext(url, protocol, watchedPath, ACTION_TYPE_EVENT);
            observerContext.addTag(BlobObserverContext.TAG_FILE_STAGE, fileStage);
            observerContext.addTag(BlobObserverContext.TAG_EVENT_TYPE, requireNonNullElse(eventType, NONE));
            observerContext.addTag(BlobObserverContext.TAG_OUTCOME, requireNonNullElse(outcome, NONE));
            observerContext.addTag(BlobObserverContext.TAG_ERROR_TYPE, requireNonNullElse(errorType, NONE));
            observerContext.addTag(BlobObserverContext.TAG_HANDLER_NAME, requireNonNullElse(handlerName, NONE));
            observerContext.addTag(BlobObserverContext.TAG_CLEANUP_ACTION, requireNonNullElse(cleanupAction, NONE));
            metricRegistry.counter(new MetricId(METRIC_FILE_EVENTS, METRIC_FILE_EVENTS_DESC,
                    observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report the file stage metric", t);
        }
    }

    /**
     * Counts one receive call on a service's queue.
     *
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param watchedPath the service's queue name
     * @param outcome     {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     */
    public static void reportPollCycle(String url, String protocol, String watchedPath, String outcome) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = eventContext(url, protocol, watchedPath, ACTION_TYPE_POLL);
            observerContext.addTag(BlobObserverContext.TAG_FILE_STAGE, NONE);
            observerContext.addTag(BlobObserverContext.TAG_EVENT_TYPE, NONE);
            observerContext.addTag(BlobObserverContext.TAG_OUTCOME, requireNonNullElse(outcome, NONE));
            observerContext.addTag(BlobObserverContext.TAG_ERROR_TYPE, NONE);
            observerContext.addTag(BlobObserverContext.TAG_HANDLER_NAME, NONE);
            observerContext.addTag(BlobObserverContext.TAG_CLEANUP_ACTION, NONE);
            metricRegistry.counter(new MetricId(METRIC_FILE_EVENTS, METRIC_FILE_EVENTS_DESC,
                    observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report the poll cycle metric", t);
        }
    }

    /**
     * Records how long fetching and binding an event's content took.
     *
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param handlerName the handler whose declared type the content was bound to
     * @param outcome     {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     * @param durationMs  the duration in milliseconds
     */
    public static void reportDatabindingDuration(String url, String protocol, String handlerName, String outcome,
                                                 long durationMs) {
        reportDuration(METRIC_DATABINDING_DURATION, METRIC_DATABINDING_DURATION_DESC, url, protocol, handlerName,
                outcome, durationMs);
    }

    /**
     * Records how long a handler call took.
     *
     * @param url         the remote host
     * @param protocol    the wire protocol
     * @param handlerName the handler that ran
     * @param outcome     {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     * @param durationMs  the duration in milliseconds
     */
    public static void reportResourceExecutionDuration(String url, String protocol, String handlerName,
                                                       String outcome, long durationMs) {
        reportDuration(METRIC_RESOURCE_EXECUTION_DURATION, METRIC_RESOURCE_EXECUTION_DURATION_DESC, url, protocol,
                handlerName, outcome, durationMs);
    }

    private static void reportDuration(String name, String description, String url, String protocol,
                                       String handlerName, String outcome, long durationMs) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            BlobObserverContext observerContext = new BlobObserverContext(CONTEXT_LISTENER, url, protocol);
            observerContext.addTag(BlobObserverContext.TAG_HANDLER_NAME, requireNonNullElse(handlerName, NONE));
            observerContext.addTag(BlobObserverContext.TAG_OUTCOME, requireNonNullElse(outcome, NONE));
            metricRegistry.gauge(new MetricId(name, description, observerContext.getAllTags()),
                    DURATION_STATISTIC_CONFIG).setValue(durationMs / 1000.0);
        } catch (Throwable t) {
            log.debug("Failed to report the {} metric", name, t);
        }
    }

    private static BlobObserverContext eventContext(String url, String protocol, String watchedPath,
                                                    String actionType) {
        BlobObserverContext observerContext = new BlobObserverContext(CONTEXT_LISTENER, url, protocol);
        observerContext.addTag(BlobObserverContext.TAG_ACTION_TYPE, actionType);
        observerContext.addTag(BlobObserverContext.TAG_OPERATION_TYPE, NONE);
        observerContext.addTag(BlobObserverContext.TAG_WATCHED_PATH, requireNonNullElse(watchedPath, NONE));
        return observerContext;
    }
}
