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

/**
 * A service's poison queue. The copy is sent first; the queue is created only when that send
 * fails because the queue does not exist, and the send is then repeated. Poison messages never
 * expire.
 */
final class PoisonQueue {

    static final String QUEUE_NOT_FOUND = "QueueNotFound";

    private final String name;
    private final QueueGateway gateway;

    PoisonQueue(String name, QueueGateway gateway) {
        this.name = name;
        this.gateway = gateway;
    }

    String name() {
        return name;
    }

    /** Copies a message body onto the poison queue and returns the copy's message id. */
    String send(String body) {
        try {
            return gateway.send(body);
        } catch (QueueCallException e) {
            if (!e.hasCode(QUEUE_NOT_FOUND)) {
                throw e;
            }
            gateway.create();
            return gateway.send(body);
        }
    }
}
