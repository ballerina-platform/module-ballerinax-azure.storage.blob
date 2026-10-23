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

import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.QueueMessageEncoding;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.Credentials;

/**
 * Builds the SDK queue clients of a listener's services on the listener's shared transport and
 * credential, aimed at the queue endpoint derived at {@code init}.
 */
final class QueueClients {

    private QueueClients() {
    }

    /**
     * Builds a queue client.
     *
     * @param ctx       the listener
     * @param queueName the queue
     * @param retry     the retry options
     * @return the client
     */
    static QueueClient build(ListenerContext ctx, String queueName, RequestRetryOptions retry) {
        QueueClientBuilder builder = new QueueClientBuilder()
                .queueName(queueName)
                .messageEncoding(QueueMessageEncoding.NONE)
                .httpClient(ctx.transport.httpClient())
                .retryOptions(retry);
        Credentials.Resolved credential = ctx.credential;
        if (credential.connectionString() != null) {
            // The SDK's parser supplies the credential; the endpoint set after it is the one
            // derived at init, so an explicit blob endpoint is honoured.
            try {
                builder.connectionString(credential.connectionString());
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw BlobErrorCreator.clientError("invalid connection string: " + BallerinaAzureClient.describe(e), e);
            }
        } else if (credential.namedKey() != null) {
            builder.credential(credential.namedKey());
        } else if (credential.sas() != null) {
            builder.credential(credential.sas());
        } else {
            builder.credential(credential.token());
        }
        builder.endpoint(ctx.queueEndpoint);
        HttpPipelinePolicy override = Credentials.portOverride(ctx.queueEndpoint);
        if (override != null) {
            builder.addPolicy(override);
        }
        try {
            return builder.buildClient();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw BlobErrorCreator.clientError(
                    "invalid listener configuration: " + BallerinaAzureClient.describe(e), e);
        }
    }
}
