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
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.runtime.api.values.BError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The receive loop, on one virtual thread per started listener. A batch is fetched when the
 * in-flight count has dropped to the threshold; each message is handled on its own virtual
 * thread. An empty queue backs off with randomized exponential delays up to the configured
 * ceiling, and a failed receive is reported and retried on the same schedule.
 */
final class Poller implements Runnable {

    private static final long MIN_BACKOFF_MILLIS = 100;
    private static final long THRESHOLD_WAIT_MILLIS = 20;

    private final ListenerContext ctx;
    private volatile boolean running = true;
    private Thread thread;

    Poller(ListenerContext ctx) {
        this.ctx = ctx;
    }

    void start() {
        thread = Thread.ofVirtual().name("azure-blob-listener-" + ctx.queueName).start(this);
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
                if (ctx.inFlight.get() > ctx.newBatchThreshold) {
                    Thread.sleep(THRESHOLD_WAIT_MILLIS);
                    continue;
                }
                List<QueueMessageItem> batch = new ArrayList<>();
                try {
                    for (QueueMessageItem message : ctx.queue.receiveMessages(ctx.batchSize,
                            MessageHandle.VISIBILITY, null, Context.NONE)) {
                        batch.add(message);
                    }
                } catch (RuntimeException e) {
                    if (!running) {
                        // The stop interrupted the receive; nothing to report.
                        return;
                    }
                    BError mapped = BallerinaAzureClient.mapFailure(e);
                    Listener.logError(ctx, "azure.storage.blob listener: polling " + ctx.queueName + " failed: "
                            + mapped.getErrorMessage());
                    Dispatcher.notifyAllOnError(ctx, mapped);
                    backoff = sleepBackoff(backoff);
                    continue;
                }
                if (batch.isEmpty()) {
                    backoff = sleepBackoff(backoff);
                    continue;
                }
                backoff = MIN_BACKOFF_MILLIS;
                for (QueueMessageItem message : batch) {
                    ctx.inFlight.incrementAndGet();
                    Thread.startVirtualThread(() -> {
                        try {
                            Dispatcher.dispatch(ctx, message);
                        } finally {
                            ctx.inFlight.decrementAndGet();
                        }
                    });
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Sleeps a random span in the upper half of the current backoff and returns the next one.
    private long sleepBackoff(long backoff) throws InterruptedException {
        long span = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff / 2 + 1);
        Thread.sleep(span);
        return Math.min(backoff * 2, ctx.maxPollingMillis);
    }
}
