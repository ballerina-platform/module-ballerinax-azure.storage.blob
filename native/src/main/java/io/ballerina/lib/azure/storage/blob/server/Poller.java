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

import com.azure.core.util.Context;
import com.azure.storage.queue.models.QueueMessageItem;
import io.ballerina.lib.azure.storage.blob.server.QueueGateway.QueueCallException;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.runtime.api.values.BError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * One service's receive loop, on its own virtual thread. A batch is fetched when the service's
 * in-flight count has dropped to its threshold; each message is handled on its own virtual
 * thread. An empty queue backs off with randomized exponential delays up to the service's
 * ceiling. A failed receive is reported, notified to the service's {@code onError} inline (the
 * queue stays paused until it returns), and retried on the same schedule. A receive that
 * completes after the poller was stopped dispatches nothing and releases its messages.
 */
final class Poller implements Runnable {

    private static final long MIN_BACKOFF_MILLIS = 100;
    private static final long THRESHOLD_WAIT_MILLIS = 20;

    private final ListenerContext ctx;
    private final ServiceContext svc;
    private volatile boolean running = true;
    private Thread thread;

    Poller(ListenerContext ctx, ServiceContext svc) {
        this.ctx = ctx;
        this.svc = svc;
    }

    void start() {
        thread = Thread.ofVirtual().name("azure-blob-listener-" + svc.queueName()).start(this);
    }

    void stop() {
        running = false;
        Thread current = thread;
        if (current != null) {
            current.interrupt();
        }
    }

    @Override
    public void run() {
        long backoff = MIN_BACKOFF_MILLIS;
        try {
            while (running) {
                if (svc.inFlight.get() > svc.settings.newBatchThreshold()) {
                    Thread.sleep(THRESHOLD_WAIT_MILLIS);
                    continue;
                }
                List<QueueMessageItem> batch = new ArrayList<>();
                try {
                    for (QueueMessageItem message : svc.queue.receiveMessages(svc.settings.batchSize(),
                            ctx.timing.hold(), null, Context.NONE)) {
                        batch.add(message);
                    }
                } catch (RuntimeException e) {
                    if (!running) {
                        return;
                    }
                    reportPollFailure(e);
                    backoff = sleepBackoff(backoff);
                    continue;
                }
                if (!running) {
                    releaseLateBatch(batch);
                    return;
                }
                if (batch.isEmpty()) {
                    backoff = sleepBackoff(backoff);
                    continue;
                }
                backoff = MIN_BACKOFF_MILLIS;
                for (QueueMessageItem message : batch) {
                    svc.inFlight.incrementAndGet();
                    ctx.enter();
                    Thread.startVirtualThread(() -> {
                        try {
                            Dispatcher.dispatch(ctx, svc, message);
                        } finally {
                            svc.inFlight.decrementAndGet();
                            ctx.exit();
                        }
                    });
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // The failed receive is logged, and the owning service's onError runs here, on the poller's
    // thread, with a warning at each full window it is still running.
    private void reportPollFailure(RuntimeException e) {
        BError mapped = BallerinaAzureClient.mapFailure(e);
        BError error = BlobErrorCreator.withMessage(mapped, "polling queue '" + svc.queueName() + "' failed: "
                + mapped.getErrorMessage().getValue());
        ctx.log.error("polling queue " + svc.queueName() + " failed: " + mapped.getErrorMessage(),
                ListenerLog.fields("queue", svc.queueName()));
        if (svc.onError == null) {
            return;
        }
        Duration period = ctx.timing.pausedQueueWarning();
        int[] windows = {0};
        ScheduledFuture<?> warning = ctx.scheduler.scheduleAtFixedRate(() -> {
            windows[0]++;
            ctx.log.warn("the onError call for the failed poll of queue " + svc.queueName()
                    + " is still running after " + HoldTiming.describe(period.multipliedBy(windows[0]))
                    + "; the queue is not polled until it returns", ListenerLog.fields("queue", svc.queueName()));
        }, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
        ctx.enter();
        try {
            Dispatcher.notifyOnError(ctx, svc, null, error, null, null);
        } finally {
            warning.cancel(false);
            ctx.exit();
        }
    }

    // Each message of a batch received too late is made visible again at once.
    private void releaseLateBatch(List<QueueMessageItem> batch) {
        for (QueueMessageItem message : batch) {
            try {
                svc.retrying.update(message.getMessageId(), message.getPopReceipt(), Duration.ZERO, null);
            } catch (QueueCallException e) {
                ctx.log.error("could not release message " + message.getMessageId() + " on queue " + svc.queueName()
                        + " received after the stop; the outcome is unknown and the message returns either at "
                        + "once or when its visibility window expires: " + e.getMessage(),
                        ListenerLog.fields("queue", svc.queueName(), "messageId", message.getMessageId(),
                                "errorCode", e.code));
            }
        }
    }

    // Sleeps a random span in the upper half of the current backoff and returns the next one.
    private long sleepBackoff(long backoff) throws InterruptedException {
        long span = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff / 2 + 1);
        Thread.sleep(span);
        return Math.min(backoff * 2, svc.settings.maxPollingMillis());
    }
}
