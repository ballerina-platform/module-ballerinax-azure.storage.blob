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

package io.ballerina.lib.azure.storage.blob.client;

import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.Credentials;
import io.ballerina.lib.azure.storage.blob.util.TransportConfigMapper;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Builds the SDK blob clients from the Ballerina {@code ClientConfiguration}. The credential is
 * resolved by {@link Credentials}; every mode is validated locally at {@code init}, with no
 * call to Azure.
 */
public final class ClientInit {

    // Field names of the client configuration record (read only here).
    public static final BString AUTH = StringUtils.fromString("auth");
    public static final BString RETRY_CONFIG = StringUtils.fromString("retryConfig");
    public static final BString TRANSPORT_CONFIG = StringUtils.fromString("transportConfig");

    private ClientInit() {
    }

    /**
     * Initializes the account-level {@code AdminClient}.
     *
     * @param self   the Ballerina client object
     * @param config the {@code ClientConfiguration} record
     * @return {@code null} on success, or the validation error
     */
    public static Object initAdminClient(BObject self, BMap<BString, Object> config) {
        try {
            self.addNativeData(BallerinaAzureClient.NATIVE_SERVICE_CLIENT, buildServiceClient(config));
            return null;
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * Initializes the container-bound {@code Client}.
     *
     * @param self          the Ballerina client object
     * @param containerName the container the client is bound to
     * @param config        the {@code ClientConfiguration} record
     * @return {@code null} on success, or the validation error
     */
    public static Object initClient(BObject self, BString containerName, BMap<BString, Object> config) {
        try {
            String container = containerName.getValue().strip();
            if (container.isEmpty()) {
                throw BlobErrorCreator.clientError("containerName must not be empty", null);
            }
            BlobServiceClient serviceClient = buildServiceClient(config);
            self.addNativeData(BallerinaAzureClient.NATIVE_SERVICE_CLIENT, serviceClient);
            self.addNativeData(BallerinaAzureClient.NATIVE_CONTAINER_CLIENT,
                    serviceClient.getBlobContainerClient(container));
            return null;
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }

    /**
     * Builds the SDK blob service client from a {@code ClientConfiguration} record.
     *
     * @param config the configuration record
     * @return the SDK service client
     */
    @SuppressWarnings("unchecked")
    public static BlobServiceClient buildServiceClient(BMap<BString, Object> config) {
        BMap<BString, Object> auth = (BMap<BString, Object>) config.getMapValue(AUTH);
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
        Object retryConfig = config.get(RETRY_CONFIG);
        if (retryConfig != null) {
            builder.retryOptions(TransportConfigMapper.retryOptions((BMap<BString, Object>) retryConfig));
        }
        // transportConfig is defaulted on the record, so the connector's pool values always apply.
        builder.httpClient(TransportConfigMapper.httpClient((BMap<BString, Object>) config.get(TRANSPORT_CONFIG)));

        Credentials.Resolved credential = Credentials.resolve(auth);
        String endpoint;
        if (credential.connectionString() != null) {
            // The SDK's parser validates the string: it refuses one it cannot derive a blob
            // endpoint from, trims the pairs, and resolves the development-storage shorthand.
            // The endpoint it derived is read back from a first build, so an explicit port (an
            // emulator) keeps the same port policy as an explicit serviceUrl.
            try {
                builder.connectionString(credential.connectionString());
                endpoint = builder.buildClient().getAccountUrl();
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw BlobErrorCreator.clientError("invalid connection string: " + BallerinaAzureClient.describe(e), e);
            }
        } else {
            endpoint = credential.endpoint(Credentials.Service.BLOB);
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
            throw BlobErrorCreator.clientError("invalid client configuration: " + BallerinaAzureClient.describe(e), e);
        }
    }
}
