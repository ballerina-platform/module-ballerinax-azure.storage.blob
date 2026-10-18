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

import io.ballerina.lib.azure.storage.blob.server.QueueGateway.QueueCallException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One received message while it is being handled. It keeps the message hidden by renewing the
 * visibility window on the halving schedule, warns at each full window the work still runs,
 * and settles the message by acknowledging, redelivering, or poisoning it. Renewal and
 * settlement are serialised under one lock, so settlement always uses the newest pop receipt
 * and no renewal runs on a settled message.
 */
final class MessageHandle {

    /** The outcome of a move to the poison queue. */
    enum PoisonOutcome { MOVED, SKIPPED_LOST, SKIPPED_TRANSIENT, FAILED }

    static final String MESSAGE_NOT_FOUND = "MessageNotFound";
    // Error codes after which the message is no longer the listener's to hold.
    private static final Set<String> NOT_OURS_CODES = Set.of("PopReceiptMismatch", MESSAGE_NOT_FOUND,
            PoisonQueue.QUEUE_NOT_FOUND, "QueueBeingDeleted");
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    // An expiry further off than this is the service's "never": the expiry rules are inactive.
    private static final Duration INFINITE_TTL = Duration.ofDays(365);

    private final ListenerLog log;
    private final HoldTiming timing;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final Executor attempts;
    private final QueueGateway retrying;
    private final QueueGateway retryFree;
    private final PoisonQueue poison;
    private final String queueName;
    private final String messageId;
    private final String body;
    private final long dequeueCount;
    private final Instant received;
    private final Instant expiry;

    private final ReentrantLock lock = new ReentrantLock();
    private String popReceipt;
    private Instant holdEnd;
    private Duration interval;
    private boolean renewing;
    private volatile boolean disposed;
    private volatile String activity = "handler";
    private ScheduledFuture<?> nextRenewal;
    private ScheduledFuture<?> windowWarning;
    private int windows;

    MessageHandle(ListenerLog log, HoldTiming timing, Clock clock, ScheduledExecutorService scheduler,
                  Executor attempts, QueueGateway retrying, QueueGateway retryFree, PoisonQueue poison,
                  String queueName, String messageId, String popReceipt, String body, long dequeueCount,
                  Instant expiry) {
        this.log = log;
        this.timing = timing;
        this.clock = clock;
        this.scheduler = scheduler;
        this.attempts = attempts;
        this.retrying = retrying;
        this.retryFree = retryFree;
        this.poison = poison;
        this.queueName = queueName;
        this.messageId = messageId;
        this.popReceipt = popReceipt;
        this.body = body;
        this.dequeueCount = dequeueCount;
        this.received = clock.instant();
        this.expiry = expiry != null && Duration.between(received, expiry).compareTo(INFINITE_TTL) > 0 ? null : expiry;
        this.holdEnd = received.plus(timing.hold());
        this.interval = timing.renewal();
    }

    String messageId() {
        return messageId;
    }

    String body() {
        return body;
    }

    long dequeueCount() {
        return dequeueCount;
    }

    Instant expiry() {
        return expiry;
    }

    boolean disposed() {
        return disposed;
    }

    /** Names what runs for the message in the full-window warnings. */
    void activity(String activity) {
        this.activity = activity;
    }

    /** Starts the renewals and the full-window warnings. */
    void start() {
        lock.lock();
        try {
            if (disposed) {
                return;
            }
            renewing = true;
            scheduleRenewal(interval);
            Duration window = timing.window();
            windowWarning = scheduler.scheduleAtFixedRate(this::warnWindow, window.toMillis(), window.toMillis(),
                    TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void warnWindow() {
        if (disposed) {
            return;
        }
        windows++;
        log.warn("the " + activity + " for message " + messageId + " on queue " + queueName
                + " is still running after " + HoldTiming.describe(timing.window().multipliedBy(windows)),
                ListenerLog.fields("queue", queueName, "messageId", messageId, "windows", windows));
    }

    private void scheduleRenewal(Duration delay) {
        if (disposed || !renewing) {
            return;
        }
        nextRenewal = scheduler.schedule(() -> attempts.execute(this::renewAttempt),
                Math.max(0, delay.toMillis()), TimeUnit.MILLISECONDS);
    }

    // One renewal attempt, scheduled from its own start: on success the interval returns to
    // the renewal period, on a transient failure it halves down to the floor, and the message
    // stops being renewed once it is no longer ours or its expiry margin is reached.
    private void renewAttempt() {
        Instant start = clock.instant();
        lock.lock();
        try {
            renewLocked(start);
        } finally {
            lock.unlock();
        }
    }

    private void renewLocked(Instant start) {
        if (disposed || !renewing) {
            return;
        }
        Duration visibility = clampedVisibility(start);
        if (visibility == null) {
            log.error("message " + messageId + " on queue " + queueName + " is bound to expire while its "
                    + activity + " still runs; its visibility is no longer renewed",
                    ListenerLog.fields("queue", queueName, "messageId", messageId));
            renewing = false;
            return;
        }
        try {
            popReceipt = retryFree.update(messageId, popReceipt, visibility,
                    timing.callTimeout(Duration.between(start, holdEnd)));
            holdEnd = start.plus(visibility);
            interval = timing.renewal();
        } catch (QueueCallException e) {
            if (notOurs(e)) {
                log.warn("message " + messageId + " on queue " + queueName + " is no longer held by this "
                        + "listener; its visibility is no longer renewed: " + e.getMessage(),
                        ListenerLog.fields("queue", queueName, "messageId", messageId, "errorCode", e.code));
                renewing = false;
                return;
            }
            Duration halved = interval.dividedBy(2);
            interval = halved.compareTo(timing.floor()) < 0 ? timing.floor() : halved;
            log.warn("could not extend the visibility of message " + messageId + " on queue " + queueName
                    + "; retrying in " + interval.toSeconds() + " seconds: " + e.getMessage(),
                    ListenerLog.fields("queue", queueName, "messageId", messageId, "errorCode", e.code));
        }
        scheduleRenewal(Duration.between(clock.instant(), start.plus(interval)));
    }

    private static boolean notOurs(QueueCallException e) {
        return e.status == BAD_REQUEST || e.status == NOT_FOUND || NOT_OURS_CODES.contains(e.code);
    }

    // The hold window, clamped to end at the expiry margin; null when no positive value remains.
    private Duration clampedVisibility(Instant now) {
        if (expiry == null) {
            return timing.hold();
        }
        Duration remaining = Duration.between(now, expiry).minus(timing.margin());
        if (remaining.isNegative() || remaining.isZero()) {
            return null;
        }
        return remaining.compareTo(timing.hold()) < 0 ? remaining : timing.hold();
    }

    /** Whether a redelivery after the delay would end at or within one window of the expiry. */
    boolean wouldExpire(long delaySeconds) {
        return Delays.wouldExpire(clock.instant(), delaySeconds, expiry, timing.hold());
    }

    /** Acknowledges the message: it is deleted and never delivered again. */
    void acknowledge() {
        lock.lock();
        try {
            if (disposed) {
                return;
            }
            try {
                retrying.delete(messageId, popReceipt);
            } catch (QueueCallException e) {
                if (e.hasCode(MESSAGE_NOT_FOUND) || e.status == NOT_FOUND) {
                    log.debug("message " + messageId + " on queue " + queueName + " was already gone when "
                            + "acknowledged", ListenerLog.fields("queue", queueName, "messageId", messageId));
                } else {
                    log.error("could not acknowledge message " + messageId + " on queue " + queueName
                            + "; it will be redelivered: " + e.getMessage(),
                            ListenerLog.fields("queue", queueName, "messageId", messageId, "errorCode", e.code));
                }
            }
            dispose();
        } finally {
            lock.unlock();
        }
    }

    /** Makes the message visible again after the delay. */
    void redeliver(long delaySeconds) {
        update(Duration.ofSeconds(delaySeconds), "schedule the redelivery of");
    }

    /** Makes the message visible again at once, as after a receive that completed too late. */
    void release() {
        update(Duration.ZERO, "release");
    }

    private void update(Duration visibility, String what) {
        lock.lock();
        try {
            if (disposed) {
                return;
            }
            try {
                retrying.update(messageId, popReceipt, visibility, null);
            } catch (QueueCallException e) {
                log.error("could not " + what + " message " + messageId + " on queue " + queueName
                        + "; the outcome is unknown and the message returns either as requested or when its "
                        + "visibility window expires: " + e.getMessage(),
                        ListenerLog.fields("queue", queueName, "messageId", messageId, "errorCode", e.code));
            }
            dispose();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves the message to the poison queue: proves the hold with a retry-free update (skipped
     * in the last minute before expiry), sends the copy, and deletes the source with the newest
     * pop receipt. The message is disposed of on a completed move and when it is found to be no
     * longer ours; after a transient hold-check failure or a failed move it stays held, so the
     * caller's notification runs under renewal before it disposes of the message.
     */
    PoisonOutcome poison(String reason, String path) {
        lock.lock();
        try {
            if (disposed) {
                return PoisonOutcome.SKIPPED_LOST;
            }
            Instant now = clock.instant();
            Duration visibility = clampedVisibility(now);
            if (visibility != null) {
                try {
                    popReceipt = retryFree.update(messageId, popReceipt, visibility,
                            timing.callTimeout(Duration.between(now, holdEnd)));
                    holdEnd = now.plus(visibility);
                } catch (QueueCallException e) {
                    Map<String, Object> fields = ListenerLog.fields("queue", queueName, "messageId", messageId,
                            "errorCode", e.code, "reason", reason);
                    if (notOurs(e)) {
                        log.warn("the move of message " + messageId + " on queue " + queueName + " to the poison "
                                + "queue was skipped: the message is held by another consumer or is gone: "
                                + e.getMessage(), fields);
                        dispose();
                        return PoisonOutcome.SKIPPED_LOST;
                    }
                    log.error("the move of message " + messageId + " on queue " + queueName + " to the poison "
                            + "queue was skipped after a transient failure of its hold check; the message "
                            + "returns at the end of its visibility window: " + e.getMessage(), fields);
                    return PoisonOutcome.SKIPPED_TRANSIENT;
                }
            }
            try {
                String poisonId = poison.send(body);
                retrying.delete(messageId, popReceipt);
                log.error("message " + messageId + " moved from queue " + queueName + " to the poison queue "
                        + poison.name() + ": " + reason,
                        ListenerLog.fields("reason", reason, "sourceQueue", queueName, "messageId", messageId,
                                "poisonQueue", poison.name(), "poisonMessageId", poisonId, "path", path,
                                "deliveryCount", dequeueCount));
                dispose();
                return PoisonOutcome.MOVED;
            } catch (QueueCallException e) {
                log.error("could not move message " + messageId + " on queue " + queueName + " to the poison queue "
                        + poison.name() + ": " + e.getMessage(),
                        ListenerLog.fields("queue", queueName, "messageId", messageId, "poisonQueue",
                                poison.name(), "errorCode", e.code, "reason", reason));
                return PoisonOutcome.FAILED;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Stops the renewals and the warnings; the message itself is left as it is. */
    void dispose() {
        lock.lock();
        try {
            disposed = true;
            renewing = false;
            if (nextRenewal != null) {
                nextRenewal.cancel(false);
            }
            if (windowWarning != null) {
                windowWarning.cancel(false);
            }
        } finally {
            lock.unlock();
        }
    }
}
