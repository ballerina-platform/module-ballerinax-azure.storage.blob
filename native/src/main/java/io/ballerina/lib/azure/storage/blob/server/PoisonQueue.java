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
import com.azure.storage.queue.QueueClient;

import java.time.Duration;

/**
 * The poison queue, {@code {queue}-poison}: created on first use, and holding its messages
 * for ever (time to live -1), so a poisoned event waits until someone inspects it.
 */
final class PoisonQueue {

    static final String SUFFIX = "-poison";
    private static final Duration NEVER_EXPIRE = Duration.ofSeconds(-1);

    private final QueueClient client;
    private volatile boolean created;

    PoisonQueue(ListenerContext ctx) {
        this.client = QueueClients.build(ctx.listenerConfig, ctx.queueName + SUFFIX);
    }

    /** Copies a message body onto the poison queue. */
    void send(String body) {
        if (!created) {
            client.createIfNotExists();
            created = true;
        }
        client.sendMessageWithResponse(body, null, NEVER_EXPIRE, null, Context.NONE);
    }
}
