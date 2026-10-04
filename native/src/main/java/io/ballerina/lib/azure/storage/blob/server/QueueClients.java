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
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.QueueMessageEncoding;
import io.ballerina.lib.azure.storage.blob.client.ClientInit;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.Credentials;
import io.ballerina.lib.azure.storage.blob.util.TransportConfigMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.net.URI;

/**
 * Builds the SDK queue clients from the Ballerina {@code ListenerConfiguration}: the same
 * credential, retry and transport settings as the blob clients, aimed at the queue endpoint.
 * Messages are received undecoded; the listener decides the encoding itself.
 */
final class QueueClients {

    private static final BString QUEUE_SERVICE_URL = StringUtils.fromString("queueServiceUrl");

    private QueueClients() {
    }

    /**
     * Builds a client for one queue of the listener's account.
     *
     * @param config    the {@code ListenerConfiguration} record
     * @param queueName the queue
     * @return the SDK queue client
     */
    @SuppressWarnings("unchecked")
    static QueueClient build(BMap<BString, Object> config, String queueName) {
        BMap<BString, Object> auth = (BMap<BString, Object>) config.getMapValue(ClientInit.AUTH);
        QueueClientBuilder builder = new QueueClientBuilder()
                .queueName(queueName)
                .messageEncoding(QueueMessageEncoding.NONE);
        Object retryConfig = config.get(ClientInit.RETRY_CONFIG);
        if (retryConfig != null) {
            builder.retryOptions(TransportConfigMapper.retryOptions((BMap<BString, Object>) retryConfig));
        }
        builder.httpClient(TransportConfigMapper.httpClient(
                (BMap<BString, Object>) config.get(ClientInit.TRANSPORT_CONFIG)));

        String queueServiceUrl = ValueUtils.optString(config, QUEUE_SERVICE_URL);
        if (queueServiceUrl != null) {
            queueServiceUrl = Credentials.validateUrl(queueServiceUrl, "queueServiceUrl");
        }
        Credentials.Resolved credential = Credentials.resolve(auth);
        String endpoint;
        if (credential.connectionString() != null) {
            if (queueServiceUrl == null) {
                Credentials.requireConnectionStringEndpoint(credential.connectionString(), Credentials.Service.QUEUE);
            }
            try {
                builder.connectionString(credential.connectionString());
            } catch (IllegalArgumentException e) {
                throw BlobErrorCreator.clientError("invalid connection string: " + BallerinaAzureClient.describe(e), e);
            }
            endpoint = queueServiceUrl != null ? queueServiceUrl
                    : Credentials.connectionStringEndpoint(credential.connectionString(), Credentials.Service.QUEUE);
            if (queueServiceUrl != null) {
                builder.endpoint(queueServiceUrl);
            }
        } else {
            endpoint = queueServiceUrl != null ? queueServiceUrl : queueEndpoint(credential);
            builder.endpoint(endpoint);
            if (credential.namedKey() != null) {
                builder.credential(credential.namedKey());
            } else if (credential.sas() != null) {
                builder.credential(credential.sas());
            } else {
                builder.credential(credential.token());
            }
        }
        if (endpoint != null) {
            HttpPipelinePolicy override = Credentials.portOverride(endpoint);
            if (override != null) {
                builder.addPolicy(override);
            }
        }
        try {
            return builder.buildClient();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw BlobErrorCreator.clientError(
                    "invalid listener configuration: " + BallerinaAzureClient.describe(e), e);
        }
    }

    // The queue endpoint derived from the credential: the account's public queue host, or the
    // SAS URL's host with its blob service name swapped for the queue service.
    private static String queueEndpoint(Credentials.Resolved credential) {
        if (credential.sasUrl() == null) {
            return credential.endpoint(Credentials.Service.QUEUE);
        }
        URI uri = URI.create(credential.sasUrl());
        String host = uri.getHost();
        if (host == null || !host.contains(".blob.")) {
            throw BlobErrorCreator.clientError(
                    "queueServiceUrl is required when the SAS URL's host is not an Azure blob endpoint", null);
        }
        return uri.getScheme() + "://" + host.replace(".blob.", ".queue.");
    }
}
