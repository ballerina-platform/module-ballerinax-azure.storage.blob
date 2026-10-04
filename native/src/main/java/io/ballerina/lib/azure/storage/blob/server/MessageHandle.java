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
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One received message while it is being handled: keeps it invisible by renewing the
 * visibility window at its half-life, tracks the pop receipt each renewal rotates, and
 * disposes of the message by acknowledging, redelivering, or poisoning it.
 */
final class MessageHandle {

    /** The visibility window a received message is held for, and its renewal period. */
    static final Duration VISIBILITY = Duration.ofMinutes(10);
    private static final long RENEWAL_SECONDS = VISIBILITY.toSeconds() / 2;

    // One daemon scheduler for every listener's renewals; the work is one small request each.
    private static final ScheduledExecutorService RENEWALS = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "azure-blob-listener-visibility");
        thread.setDaemon(true);
        return thread;
    });

    private final ListenerContext ctx;
    private final String messageId;
    private final String body;
    private final long dequeueCount;
    private volatile String popReceipt;
    private volatile ScheduledFuture<?> renewal;
    // Set once, by whichever disposition comes first; the others then do nothing.
    private final AtomicBoolean disposed = new AtomicBoolean();

    MessageHandle(ListenerContext ctx, QueueMessageItem message) {
        this.ctx = ctx;
        this.messageId = message.getMessageId();
        this.popReceipt = message.getPopReceipt();
        this.body = message.getMessageText();
        this.dequeueCount = message.getDequeueCount();
    }

    String body() {
        return body;
    }

    long dequeueCount() {
        return dequeueCount;
    }

    /** Starts renewing the visibility window while the handler runs. */
    void keepInvisible() {
        renewal = RENEWALS.scheduleWithFixedDelay(this::renew, RENEWAL_SECONDS, RENEWAL_SECONDS, TimeUnit.SECONDS);
    }

    private void renew() {
        if (disposed.get()) {
            return;
        }
        try {
            popReceipt = ctx.queue.updateMessage(messageId, popReceipt, body, VISIBILITY).getPopReceipt();
        } catch (RuntimeException e) {
            Listener.logWarn(ctx, "azure.storage.blob listener: could not extend the visibility of message "
                    + messageId + ": " + BallerinaAzureClient.describe(e));
            cancelRenewal();
        }
    }

    /** Acknowledges the message: it is deleted and never delivered again. */
    void acknowledge() {
        if (!dispose()) {
            return;
        }
        try {
            ctx.queue.deleteMessage(messageId, popReceipt);
        } catch (RuntimeException e) {
            Listener.logWarn(ctx, "azure.storage.blob listener: could not delete message " + messageId
                    + "; it will be redelivered: " + BallerinaAzureClient.describe(e));
        }
    }

    /** Makes the message visible again after the configured redelivery delay. */
    void redeliver() {
        if (!dispose()) {
            return;
        }
        try {
            ctx.queue.updateMessage(messageId, popReceipt, body, Duration.ofSeconds(ctx.redeliverySeconds));
        } catch (RuntimeException e) {
            Listener.logWarn(ctx, "azure.storage.blob listener: could not schedule the redelivery of message "
                    + messageId + "; it reappears when its visibility window expires: "
                    + BallerinaAzureClient.describe(e));
        }
    }

    /** Moves the message to the poison queue. */
    void poison(String reason) {
        if (!dispose()) {
            return;
        }
        try {
            PoisonQueue poison = ctx.poison;
            if (poison == null) {
                poison = new PoisonQueue(ctx);
                ctx.poison = poison;
            }
            poison.send(body);
            ctx.queue.deleteMessage(messageId, popReceipt);
            Listener.logError(ctx, "azure.storage.blob listener: message " + messageId + " moved to "
                    + ctx.queueName + PoisonQueue.SUFFIX + ": " + reason);
        } catch (RuntimeException e) {
            Listener.logError(ctx, "azure.storage.blob listener: could not move message " + messageId
                    + " to the poison queue: " + BallerinaAzureClient.describe(e));
        }
    }

    /**
     * Stops the renewals without touching the message (an immediate stop, or a dispatch crash):
     * it reappears on the queue when its visibility window expires.
     */
    void release() {
        dispose();
    }

    // Marks the message disposed of; true for the first caller only.
    private boolean dispose() {
        boolean first = disposed.compareAndSet(false, true);
        if (first) {
            cancelRenewal();
            ctx.active.remove(this);
        }
        return first;
    }

    private void cancelRenewal() {
        ScheduledFuture<?> current = renewal;
        if (current != null) {
            current.cancel(false);
        }
    }
}
