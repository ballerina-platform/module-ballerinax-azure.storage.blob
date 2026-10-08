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

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.UrlBuilder;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.TransportConfigMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

/**
 * Builds the SDK clients from the Ballerina {@code ClientConfiguration}. The auth union member is
 * selected by the fields present on the auth record; every mode is validated locally at
 * {@code init}, with no call to Azure.
 */
public final class ClientInit {

    // Field names of the auth and client configuration records (read only here).
    private static final BString ACCOUNT_NAME = StringUtils.fromString("accountName");
    private static final BString ACCOUNT_KEY = StringUtils.fromString("accountKey");
    private static final BString SAS_TOKEN = StringUtils.fromString("sasToken");
    private static final BString SAS_URL = StringUtils.fromString("sasUrl");
    private static final BString CONNECTION_STRING = StringUtils.fromString("connectionString");
    private static final BString SERVICE_URL = StringUtils.fromString("serviceUrl");
    private static final BString AUTH = StringUtils.fromString("auth");
    private static final BString RETRY_CONFIG = StringUtils.fromString("retryConfig");
    private static final BString TRANSPORT_CONFIG = StringUtils.fromString("transportConfig");
    private static final BString KIND = StringUtils.fromString("kind");
    // The Entra credential-kind discriminator value selecting managed-identity auth.
    private static final String KIND_MANAGED_IDENTITY = "managed-identity";
    private static final BString TENANT_ID = StringUtils.fromString("tenantId");
    private static final BString CLIENT_ID = StringUtils.fromString("clientId");
    private static final BString CLIENT_SECRET = StringUtils.fromString("clientSecret");
    private static final BString CERTIFICATE_PATH = StringUtils.fromString("certificatePath");
    private static final BString CERTIFICATE_PASSWORD = StringUtils.fromString("certificatePassword");
    private static final BString TOKEN_FILE_PATH = StringUtils.fromString("tokenFilePath");

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
            storeSasSignature(self, config);
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
            bindContainer(self, containerName, config);
            return null;
        } catch (BError e) {
            return e;
        } catch (Exception e) {
            return BlobErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
        }
    }


    private static void bindContainer(BObject self, BString containerName, BMap<BString, Object> config) {
        String container = containerName.getValue().strip();
        if (container.isEmpty()) {
            throw BlobErrorCreator.clientError("containerName must not be empty", null);
        }
        BlobServiceClient serviceClient = buildServiceClient(config);
        self.addNativeData(BallerinaAzureClient.NATIVE_SERVICE_CLIENT, serviceClient);
        self.addNativeData(BallerinaAzureClient.NATIVE_CONTAINER_CLIENT,
                serviceClient.getBlobContainerClient(container));
        storeSasSignature(self, config);
    }

    // A SAS signs the request alone; a copy source in the same account must carry it too, so
    // the query of a SAS credential is kept for the copy operations.
    @SuppressWarnings("unchecked")
    private static void storeSasSignature(BObject self, BMap<BString, Object> config) {
        BMap<BString, Object> auth = (BMap<BString, Object>) config.getMapValue(AUTH);
        String signature = null;
        if (auth.containsKey(SAS_TOKEN)) {
            signature = requireNonEmpty(auth, SAS_TOKEN);
        } else if (auth.containsKey(SAS_URL)) {
            signature = URI.create(requireNonEmpty(auth, SAS_URL)).getRawQuery();
        }
        if (signature != null) {
            self.addNativeData(BallerinaAzureClient.NATIVE_SAS_SIGNATURE,
                    signature.startsWith("?") ? signature.substring(1) : signature);
        }
    }

    /**
     * Builds the SDK service client from a {@code ClientConfiguration} or {@code ListenerConfiguration}
     * record. Both carry the same auth, retry, and transport shape.
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
        if (auth.containsKey(ACCOUNT_KEY)) {
            configureSharedKey(builder, auth);
        } else if (auth.containsKey(SAS_TOKEN)) {
            configureSas(builder, auth);
        } else if (auth.containsKey(SAS_URL)) {
            configureSasUrl(builder, auth);
        } else if (auth.containsKey(CONNECTION_STRING)) {
            configureConnectionString(builder, auth);
        } else {
            configureEntra(builder, auth);
        }
        try {
            return builder.buildClient();
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw BlobErrorCreator.clientError("invalid client configuration: " + BallerinaAzureClient.describe(e), e);
        }
    }

    private static void configureSharedKey(BlobServiceClientBuilder builder, BMap<BString, Object> auth) {
        String accountName = requireNonEmpty(auth, ACCOUNT_NAME);
        String accountKey = requireNonEmpty(auth, ACCOUNT_KEY);
        try {
            Base64.getDecoder().decode(accountKey);
        } catch (IllegalArgumentException e) {
            throw BlobErrorCreator.clientError("accountKey is not a valid base64 string", e);
        }
        String serviceUrl = ValueUtils.optString(auth, SERVICE_URL);
        applyEndpoint(builder, accountName, serviceUrl);
        builder.credential(new AzureNamedKeyCredential(accountName, accountKey));
    }

    /*
     * The SDK's endpoint parsing keeps only the URL's scheme and host, so an endpoint carrying
     * an explicit port (a private endpoint, a tunnel, or a local emulator such as Azurite) would
     * silently lose it. This policy restores the configured authority on requests still aimed at
     * the configured host. A request whose host already differs is a retry the storage retry
     * policy re-targeted at the configured secondary host, and is left untouched (the policy
     * runs per retry, after that swap). One shape stays unguarded: a secondary sharing the
     * primary's host and differing only in port is indistinguishable here and still gets
     * rewritten.
     */
    private static void applyEndpoint(BlobServiceClientBuilder builder, String accountName, String serviceUrl) {
        builder.endpoint(serviceUrl == null ? defaultEndpoint(accountName) : validateUrl(serviceUrl, "serviceUrl"));
        if (serviceUrl != null) {
            addPortOverride(builder, serviceUrl);
        }
    }

    private static void addPortOverride(BlobServiceClientBuilder builder, String url) {
        URI uri = URI.create(url);
        int port = uri.getPort();
        if (port == -1) {
            return;
        }
        String scheme = uri.getScheme();
        String host = uri.getHost();
        HttpPipelinePolicy override = (context, next) -> {
            UrlBuilder requestUrl = UrlBuilder.parse(context.getHttpRequest().getUrl());
            if (!host.equalsIgnoreCase(requestUrl.getHost())) {
                return next.process();
            }
            requestUrl.setScheme(scheme).setHost(host).setPort(port);
            context.getHttpRequest().setUrl(requestUrl.toString());
            return next.process();
        };
        builder.addPolicy(override);
    }

    /*
     * Configures a Microsoft Entra ID credential. The record kind is chosen structurally: a
     * secret, a certificate path, or a token file path names its credential outright; otherwise
     * the `kind` discriminator separates the default chain from a managed identity.
     */
    private static void configureEntra(BlobServiceClientBuilder builder, BMap<BString, Object> auth) {
        TokenCredential credential;
        if (auth.containsKey(CLIENT_SECRET)) {
            credential = new ClientSecretCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID))
                    .clientSecret(requireNonEmpty(auth, CLIENT_SECRET))
                    .build();
        } else if (auth.containsKey(CERTIFICATE_PATH)) {
            String certificatePath = requireNonEmpty(auth, CERTIFICATE_PATH);
            if (!Files.isRegularFile(Path.of(certificatePath))) {
                throw BlobErrorCreator.clientError(
                        "certificatePath does not point to a readable file: " + certificatePath, null);
            }
            ClientCertificateCredentialBuilder certificateBuilder = new ClientCertificateCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID));
            String certificatePassword = ValueUtils.optString(auth, CERTIFICATE_PASSWORD);
            credential = (certificatePassword == null
                    ? certificateBuilder.pemCertificate(certificatePath)
                    : certificateBuilder.pfxCertificate(certificatePath, certificatePassword))
                    .build();
        } else if (auth.containsKey(TOKEN_FILE_PATH)) {
            credential = new WorkloadIdentityCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID))
                    .tokenFilePath(requireNonEmpty(auth, TOKEN_FILE_PATH))
                    .build();
        } else if (KIND_MANAGED_IDENTITY.equals(ValueUtils.optString(auth, KIND))) {
            ManagedIdentityCredentialBuilder managedBuilder = new ManagedIdentityCredentialBuilder();
            String clientId = ValueUtils.optString(auth, CLIENT_ID);
            if (clientId != null) {
                managedBuilder.clientId(clientId);
            }
            credential = managedBuilder.build();
        } else {
            DefaultAzureCredentialBuilder chainBuilder = new DefaultAzureCredentialBuilder();
            String clientId = ValueUtils.optString(auth, CLIENT_ID);
            if (clientId != null) {
                chainBuilder.managedIdentityClientId(clientId);
            }
            credential = chainBuilder.build();
        }
        String accountName = requireNonEmpty(auth, ACCOUNT_NAME);
        String serviceUrl = ValueUtils.optString(auth, SERVICE_URL);
        applyEndpoint(builder, accountName, serviceUrl);
        builder.credential(credential);
    }

    private static void configureSas(BlobServiceClientBuilder builder, BMap<BString, Object> auth) {
        String accountName = requireNonEmpty(auth, ACCOUNT_NAME);
        String sasToken = requireNonEmpty(auth, SAS_TOKEN);
        builder.endpoint(defaultEndpoint(accountName)).credential(new AzureSasCredential(sasToken));
    }

    private static void configureSasUrl(BlobServiceClientBuilder builder, BMap<BString, Object> auth) {
        String sasUrl = requireNonEmpty(auth, SAS_URL);
        URI uri;
        try {
            uri = new URI(sasUrl);
        } catch (URISyntaxException e) {
            throw BlobErrorCreator.clientError("sasUrl is not a valid URL", e);
        }
        if (uri.getScheme() == null || !(uri.getScheme().equals("https") || uri.getScheme().equals("http"))) {
            throw BlobErrorCreator.clientError("sasUrl must use the http or https scheme", null);
        }
        if (uri.getRawQuery() == null || !uri.getRawQuery().contains("sig=")) {
            throw BlobErrorCreator.clientError(
                    "sasUrl carries no SAS token (no `sig=` in its query); for a bare token use SasConfig", null);
        }
        String base = sasUrl.substring(0, sasUrl.indexOf('?'));
        builder.endpoint(base).credential(new AzureSasCredential(uri.getRawQuery()));
        addPortOverride(builder, base);
    }

    // The SDK's parser validates the string: it refuses one it cannot derive a blob endpoint
    // from, trims the pairs, and resolves the development-storage shorthand. The endpoint it
    // derived is read back from a first build, so an explicit port (an emulator) keeps the
    // same port policy as an explicit serviceUrl.
    private static void configureConnectionString(BlobServiceClientBuilder builder, BMap<BString, Object> auth) {
        String connectionString = requireNonEmpty(auth, CONNECTION_STRING);
        try {
            builder.connectionString(connectionString);
            addPortOverride(builder, builder.buildClient().getAccountUrl());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw BlobErrorCreator.clientError("invalid connection string: " + BallerinaAzureClient.describe(e), e);
        }
    }

    private static String requireNonEmpty(BMap<BString, Object> record, BString field) {
        String value = ValueUtils.optString(record, field);
        if (value == null || value.strip().isEmpty()) {
            throw BlobErrorCreator.clientError(field.getValue() + " must not be empty", null);
        }
        return value.strip();
    }

    private static String defaultEndpoint(String accountName) {
        return "https://" + accountName + ".blob.core.windows.net";
    }

    private static String validateUrl(String url, String fieldName) {
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || !(uri.getScheme().equals("https") || uri.getScheme().equals("http"))) {
                throw BlobErrorCreator.clientError(fieldName + " must use the http or https scheme", null);
            }
            return url;
        } catch (URISyntaxException e) {
            throw BlobErrorCreator.clientError(fieldName + " is not a valid URL", e);
        }
    }
}
