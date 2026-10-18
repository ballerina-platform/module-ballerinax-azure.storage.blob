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
import com.azure.storage.queue.models.QueueStorageException;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * The four queue calls message handling makes, behind one seam so the handling logic is tested
 * without the SDK. Every failure surfaces as a {@link QueueCallException} carrying the status
 * and the service error code, with the transient cases (a server-side error, a network error,
 * a timeout) marked as such.
 */
interface QueueGateway {

    /** Hides the message for the visibility, without changing its text; returns the new pop receipt. */
    String update(String messageId, String popReceipt, Duration visibility, Duration timeout);

    void delete(String messageId, String popReceipt);

    /** Sends a never-expiring copy and returns its message id. */
    String send(String body);

    void create();

    /** A failed queue call. */
    final class QueueCallException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private static final int REQUEST_TIMEOUT = 408;
        private static final int TOO_MANY_REQUESTS = 429;

        final int status;
        final String code;
        final boolean transientFailure;

        QueueCallException(int status, String code, String message, Throwable cause) {
            super(message, cause);
            this.status = status;
            this.code = code == null ? "" : code;
            this.transientFailure = status == 0 || status >= 500 || status == REQUEST_TIMEOUT
                    || status == TOO_MANY_REQUESTS;
        }

        boolean hasCode(String errorCode) {
            return code.equals(errorCode);
        }
    }

    /** The gateway over an SDK queue client. */
    final class SdkQueueGateway implements QueueGateway {

        private static final Duration NEVER_EXPIRE = Duration.ofSeconds(-1);

        private final QueueClient client;

        SdkQueueGateway(QueueClient client) {
            this.client = client;
        }

        @Override
        public String update(String messageId, String popReceipt, Duration visibility, Duration timeout) {
            try {
                return client.updateMessageWithResponse(messageId, popReceipt, null, visibility, timeout, Context.NONE)
                        .getValue().getPopReceipt();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void delete(String messageId, String popReceipt) {
            try {
                client.deleteMessageWithResponse(messageId, popReceipt, null, Context.NONE);
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public String send(String body) {
            try {
                return client.sendMessageWithResponse(body, null, NEVER_EXPIRE, null, Context.NONE)
                        .getValue().getMessageId();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        @Override
        public void create() {
            try {
                client.createIfNotExists();
            } catch (RuntimeException e) {
                throw translate(e);
            }
        }

        // A timeout or a network failure carries no status; a service failure carries its code.
        static QueueCallException translate(RuntimeException e) {
            Throwable current = e;
            for (int depth = 0; current != null && depth < 8; depth++) {
                if (current instanceof QueueStorageException storage) {
                    String code = storage.getErrorCode() == null ? null : storage.getErrorCode().toString();
                    String message = storage.getServiceMessage() == null ? storage.getMessage()
                            : storage.getServiceMessage();
                    return new QueueCallException(storage.getStatusCode(), code, message, e);
                }
                if (current instanceof TimeoutException || current instanceof IOException) {
                    return new QueueCallException(0, null, BallerinaAzureClient.describe(current), e);
                }
                Throwable cause = current.getCause();
                current = cause == current ? null : cause;
            }
            return new QueueCallException(0, null, BallerinaAzureClient.describe(e), e);
        }
    }
}
