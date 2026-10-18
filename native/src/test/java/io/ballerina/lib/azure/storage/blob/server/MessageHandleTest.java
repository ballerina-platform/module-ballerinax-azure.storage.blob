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

import io.ballerina.lib.azure.storage.blob.server.MessageHandle.PoisonOutcome;
import io.ballerina.lib.azure.storage.blob.server.QueueGateway.QueueCallException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Message visibility and settlement against spec 5.4 and 5.10: the renewal schedule and its
 * halving retry, the stop conditions, the expiry clamp, the call timeout, the full-window
 * warnings, and the poison move protocol with its hold check. The queue calls, the clock and
 * the timer are stubbed, so each rule is driven deterministically.
 */
public class MessageHandleTest {

    private static final HoldTiming TIMING = new HoldTiming(Duration.ofMinutes(10));
    private static final Instant T0 = Instant.parse("2026-10-18T10:00:00Z");

    private MutableClock clock;
    private FakeScheduler scheduler;
    private FakeGateway retrying;
    private FakeGateway retryFree;
    private FakeGateway poisonGateway;
    private RecordingLog log;

    @BeforeMethod
    public void setUp() {
        clock = new MutableClock(T0);
        scheduler = new FakeScheduler();
        retrying = new FakeGateway();
        retryFree = new FakeGateway();
        poisonGateway = new FakeGateway();
        log = new RecordingLog();
    }

    private MessageHandle handle(Instant expiry) {
        MessageHandle handle = new MessageHandle(log, TIMING, clock, scheduler, Runnable::run, retrying, retryFree,
                new PoisonQueue("events-poison", poisonGateway), "events", "m-1", "r-0", "body", 1, expiry);
        handle.start();
        return handle;
    }

    // ---- renewal (spec 5.4)

    @Test
    public void renewalsRunEveryFiveMinutesOnTheRetryFreeClientWithTheNewestReceipt() {
        MessageHandle handle = handle(T0.plus(Duration.ofDays(7)));
        assertEquals(scheduler.nextDelay(), Duration.ofMinutes(5));
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        FakeGateway.Call renewal = retryFree.calls.get(0);
        assertEquals(renewal.op, "update");
        assertEquals(renewal.popReceipt, "r-0");
        assertEquals(renewal.visibility, Duration.ofMinutes(10));
        assertEquals(renewal.timeout, Duration.ofSeconds(30), "the smaller of 30 s and the hold remaining (5 min)");
        assertEquals(scheduler.nextDelay(), Duration.ofMinutes(5), "scheduled from the attempt's start");
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(retryFree.calls.get(1).popReceipt, "r-1", "the rotated receipt");
        handle.acknowledge();
        assertEquals(retrying.calls.get(0).op, "delete");
        assertEquals(retrying.calls.get(0).popReceipt, "r-2", "settlement uses the newest receipt");
        assertTrue(handle.disposed());
        assertNull(scheduler.nextDelay(), "no renewal after disposal");
    }

    @Test
    public void transientFailuresHalveTheIntervalDownToTheFloorAndSuccessResetsIt() {
        handle(null);
        retryFree.failWith(new QueueCallException(503, "ServerBusy", "busy", null));
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofSeconds(150), "5 min halves to 2.5");
        assertEquals(log.warnings().size(), 1);
        clock.advance(Duration.ofSeconds(150));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofSeconds(75), "then 1.25 min");
        clock.advance(Duration.ofSeconds(75));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofSeconds(60), "then the one-minute floor");
        clock.advance(Duration.ofSeconds(60));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofSeconds(60), "and stays at the floor");
        assertEquals(log.warnings().size(), 4, "each failure logs a warning");
        retryFree.failWith(null);
        clock.advance(Duration.ofSeconds(60));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofMinutes(5), "a success returns to five minutes");
        // A timeout and a network failure are transient too.
        retryFree.failWith(new QueueCallException(0, null, "timeout", null));
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(scheduler.nextDelay(), Duration.ofSeconds(150));
    }

    @Test
    public void renewalStopsWhenTheMessageIsNoLongerOurs() {
        for (QueueCallException stop : List.of(
                new QueueCallException(400, "PopReceiptMismatch", "mismatch", null),
                new QueueCallException(404, "MessageNotFound", "gone", null),
                new QueueCallException(404, "QueueNotFound", "gone", null),
                new QueueCallException(409, "QueueBeingDeleted", "deleting", null),
                new QueueCallException(400, "OutOfRangeQueryParameterValue", "past expiry", null))) {
            setUp();
            handle(null);
            retryFree.failWith(stop);
            clock.advance(Duration.ofMinutes(5));
            scheduler.runNext();
            assertNull(scheduler.nextDelay(), stop.code + " stops the renewal");
            assertEquals(log.warnings().size(), 1, stop.code);
        }
    }

    @Test
    public void renewalIsClampedToTheExpiryMarginAndStopsWhenNothingRemains() {
        handle(T0.plus(Duration.ofMinutes(17)));
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(retryFree.calls.get(0).visibility, Duration.ofMinutes(10), "12 minutes left: the full hold");
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(retryFree.calls.get(1).visibility, Duration.ofMinutes(6), "7 minutes left less the margin");
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(retryFree.calls.get(2).visibility, Duration.ofMinutes(1), "2 minutes left less the margin");
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        assertEquals(retryFree.calls.size(), 3, "nothing positive remains: no visibility of zero or less is sent");
        assertNull(scheduler.nextDelay());
        assertEquals(log.errors().size(), 1, "the unavoidable expiry is an error");
        assertTrue(log.errors().get(0).contains("bound to expire"), log.errors().get(0));
    }

    @Test
    public void anExpiryFarInTheFutureIsInfinite() {
        MessageHandle handle = handle(T0.plus(Duration.ofDays(400)));
        assertNull(handle.expiry());
        assertFalse(handle.wouldExpire(604_800));
    }

    @Test
    public void theCallTimeoutShrinksWithTheHoldRemaining() {
        handle(null);
        retryFree.failWith(new QueueCallException(503, "ServerBusy", "busy", null));
        clock.advance(Duration.ofMinutes(5));
        scheduler.runNext();
        clock.advance(Duration.ofSeconds(150));
        scheduler.runNext();
        clock.advance(Duration.ofSeconds(75));
        scheduler.runNext();
        clock.advance(Duration.ofSeconds(60));
        scheduler.runNext();
        // 9 min 45 s into a 10-minute hold: 15 s remain.
        assertEquals(retryFree.calls.get(3).timeout, Duration.ofSeconds(15));
    }

    @Test
    public void aWarningIsLoggedAtEachFullWindow() {
        MessageHandle handle = handle(null);
        assertEquals(scheduler.fixedRatePeriod(), Duration.ofMinutes(10));
        scheduler.runFixedRate();
        scheduler.runFixedRate();
        assertEquals(log.warnings().size(), 2);
        assertTrue(log.warnings().get(0).contains("still running after 10 minutes"), log.warnings().get(0));
        assertTrue(log.warnings().get(1).contains("20 minutes"), log.warnings().get(1));
        handle.activity("onError call");
        scheduler.runFixedRate();
        assertTrue(log.warnings().get(2).contains("onError call"), log.warnings().get(2));
        handle.dispose();
        scheduler.runFixedRate();
        assertEquals(log.warnings().size(), 3, "no warning after disposal");
    }

    // ---- settlement (spec 5.9, 5.10)

    @Test
    public void acknowledgementTreatsNotFoundAsGoneAndOtherFailuresAsRedelivery() {
        MessageHandle gone = handle(null);
        retrying.failWith(new QueueCallException(404, "MessageNotFound", "gone", null));
        gone.acknowledge();
        assertEquals(log.errors().size(), 0);
        assertEquals(log.debugs().size(), 1);
        setUp();
        MessageHandle failed = handle(null);
        retrying.failWith(new QueueCallException(500, "InternalError", "boom", null));
        failed.acknowledge();
        assertEquals(log.errors().size(), 1);
        assertTrue(log.errors().get(0).contains("will be redelivered"), log.errors().get(0));
        assertTrue(failed.disposed());
    }

    @Test
    public void redeliveryAndReleaseUseTheRetryingClientAndReportAnUnknownOutcome() {
        MessageHandle handle = handle(null);
        handle.redeliver(120);
        assertEquals(retrying.calls.get(0).visibility, Duration.ofSeconds(120));
        assertNull(retrying.calls.get(0).timeout, "the SDK's retry options apply");
        setUp();
        MessageHandle released = handle(null);
        retrying.failWith(new QueueCallException(0, null, "connection reset", null));
        released.release();
        assertEquals(retrying.calls.get(0).visibility, Duration.ZERO);
        assertTrue(log.errors().get(0).contains("outcome is unknown"), log.errors().get(0));
    }

    @Test
    public void aPoisonMoveProvesTheHoldThenSendsThenDeletesWithTheNewestReceipt() {
        MessageHandle handle = handle(T0.plus(Duration.ofDays(7)));
        clock.advance(Duration.ofMinutes(2));
        assertEquals(handle.poison(Dispatcher.REASON_LIMIT, "a.txt"), PoisonOutcome.MOVED);
        FakeGateway.Call check = retryFree.calls.get(0);
        assertEquals(check.op, "update");
        assertEquals(check.visibility, Duration.ofMinutes(10), "a renewal's visibility");
        assertEquals(check.timeout, Duration.ofSeconds(30));
        assertEquals(poisonGateway.calls.get(0).op, "send");
        assertEquals(poisonGateway.calls.get(0).body, "body", "copied verbatim");
        assertEquals(retrying.calls.get(0).op, "delete");
        assertEquals(retrying.calls.get(0).popReceipt, "r-1", "the receipt the hold check returned");
        assertTrue(handle.disposed());
        String line = log.errors().get(0);
        assertTrue(line.contains("moved from queue events to the poison queue events-poison"), line);
        Map<String, Object> fields = log.errorFields().get(0);
        assertEquals(fields.get("reason"), Dispatcher.REASON_LIMIT);
        assertEquals(fields.get("sourceQueue"), "events");
        assertEquals(fields.get("messageId"), "m-1");
        assertEquals(fields.get("poisonQueue"), "events-poison");
        assertEquals(fields.get("poisonMessageId"), "p-1");
        assertEquals(fields.get("path"), "a.txt");
        assertEquals(fields.get("deliveryCount"), 1L);
    }

    @Test
    public void thePoisonQueueIsCreatedOnlyWhenTheSendFindsItMissing() {
        MessageHandle handle = handle(null);
        poisonGateway.failOnce(new QueueCallException(404, "QueueNotFound", "no queue", null));
        assertEquals(handle.poison(Dispatcher.REASON_UNPARSEABLE, null), PoisonOutcome.MOVED);
        List<String> ops = poisonGateway.calls.stream().map(call -> call.op).toList();
        assertEquals(ops, List.of("send", "create", "send"), "send first, create on QueueNotFound, send again");
    }

    @Test
    public void aHoldCheckThatFindsTheMessageGoneSkipsTheMoveQuietly() {
        MessageHandle handle = handle(null);
        retryFree.failWith(new QueueCallException(400, "PopReceiptMismatch", "mismatch", null));
        assertEquals(handle.poison(Dispatcher.REASON_LIMIT, "a.txt"), PoisonOutcome.SKIPPED_LOST);
        assertTrue(poisonGateway.calls.isEmpty(), "nothing is sent");
        assertTrue(retrying.calls.isEmpty(), "nothing is deleted");
        assertTrue(handle.disposed(), "no longer ours: disposed of");
        assertEquals(log.warnings().size(), 1);
        assertEquals(log.errors().size(), 0, "logged only, no error line");
    }

    @Test
    public void aTransientHoldCheckFailureSkipsTheMoveAndKeepsTheMessageHeld() {
        MessageHandle handle = handle(null);
        retryFree.failWith(new QueueCallException(500, "InternalError", "boom", null));
        assertEquals(handle.poison(Dispatcher.REASON_LIMIT, "a.txt"), PoisonOutcome.SKIPPED_TRANSIENT);
        assertTrue(poisonGateway.calls.isEmpty());
        assertFalse(handle.disposed(), "held under renewal until the caller disposes of it");
        assertEquals(log.errors().size(), 1);
        assertTrue(log.errors().get(0).contains("transient failure of its hold check"), log.errors().get(0));
    }

    @Test
    public void inTheLastMinuteTheMoveSendsAndDeletesWithoutTheHoldCheck() {
        MessageHandle handle = handle(T0.plus(Duration.ofSeconds(50)));
        assertEquals(handle.poison(Dispatcher.REASON_ARCHIVED, "a.txt"), PoisonOutcome.MOVED);
        assertTrue(retryFree.calls.isEmpty(), "no hold check when no positive visibility remains");
        assertEquals(poisonGateway.calls.get(0).op, "send");
        assertEquals(retrying.calls.get(0).popReceipt, "r-0", "the current receipt");
    }

    @Test
    public void aFailedSendOrDeleteIsAFailedMove() {
        MessageHandle handle = handle(null);
        poisonGateway.failWith(new QueueCallException(403, "AuthorizationPermissionMismatch", "no add", null));
        assertEquals(handle.poison(Dispatcher.REASON_LIMIT, "a.txt"), PoisonOutcome.FAILED);
        assertFalse(handle.disposed());
        assertTrue(log.errors().get(0).contains("could not move message"), log.errors().get(0));
        assertTrue(retrying.calls.isEmpty(), "no delete after a failed send");
    }

    // ---- stubs

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /** Records queue calls; a configured failure is thrown by every call, or by the next one only. */
    private static final class FakeGateway implements QueueGateway {

        record Call(String op, String popReceipt, Duration visibility, Duration timeout, String body) {
        }

        final List<Call> calls = new ArrayList<>();
        private QueueCallException failure;
        private QueueCallException once;
        private int receipts;
        private int sent;

        void failWith(QueueCallException failure) {
            this.failure = failure;
        }

        void failOnce(QueueCallException failure) {
            this.once = failure;
        }

        private void maybeFail() {
            if (once != null) {
                QueueCallException e = once;
                once = null;
                throw e;
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public String update(String messageId, String popReceipt, Duration visibility, Duration timeout) {
            calls.add(new Call("update", popReceipt, visibility, timeout, null));
            maybeFail();
            return "r-" + (++receipts);
        }

        @Override
        public void delete(String messageId, String popReceipt) {
            calls.add(new Call("delete", popReceipt, null, null, null));
            maybeFail();
        }

        @Override
        public String send(String body) {
            calls.add(new Call("send", null, null, null, body));
            maybeFail();
            return "p-" + (++sent);
        }

        @Override
        public void create() {
            calls.add(new Call("create", null, null, null, null));
        }
    }

    /** Captures the scheduled tasks; the test fires them by hand. */
    private static final class FakeScheduler implements ScheduledExecutorService {
        private Runnable next;
        private Duration nextDelay;
        private Runnable fixedRate;
        private Duration fixedRatePeriod;
        private boolean nextCancelled;
        private boolean fixedRateCancelled;

        Duration nextDelay() {
            return next == null || nextCancelled ? null : nextDelay;
        }

        void runNext() {
            Runnable task = next;
            next = null;
            if (task != null && !nextCancelled) {
                task.run();
            }
        }

        Duration fixedRatePeriod() {
            return fixedRatePeriod;
        }

        void runFixedRate() {
            if (fixedRate != null && !fixedRateCancelled) {
                fixedRate.run();
            }
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            next = command;
            nextDelay = Duration.ofMillis(unit.toMillis(delay));
            nextCancelled = false;
            return new Future(() -> nextCancelled = true);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            fixedRate = command;
            fixedRatePeriod = Duration.ofMillis(unit.toMillis(period));
            fixedRateCancelled = false;
            return new Future(() -> fixedRateCancelled = true);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                         TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> java.util.concurrent.Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(
                java.util.Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(
                java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        private static final class Future implements ScheduledFuture<Object> {
            private final Runnable onCancel;

            Future(Runnable onCancel) {
                this.onCancel = onCancel;
            }

            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                onCancel.run();
                return true;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Object get() {
                return null;
            }

            @Override
            public Object get(long timeout, TimeUnit unit) {
                return null;
            }
        }
    }

    /** Keeps the logged lines and fields per level. */
    static final class RecordingLog implements ListenerLog {
        private final List<String> warnings = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();
        private final List<String> debugs = new ArrayList<>();
        private final List<Map<String, Object>> errorFields = new ArrayList<>();

        List<String> warnings() {
            return warnings;
        }

        List<String> errors() {
            return errors;
        }

        List<String> debugs() {
            return debugs;
        }

        List<Map<String, Object>> errorFields() {
            return errorFields;
        }

        @Override
        public void warn(String message, Map<String, Object> fields) {
            warnings.add(message);
        }

        @Override
        public void error(String message, Map<String, Object> fields) {
            errors.add(message);
            errorFields.add(fields);
        }

        @Override
        public void debug(String message, Map<String, Object> fields) {
            debugs.add(message);
        }
    }
}
