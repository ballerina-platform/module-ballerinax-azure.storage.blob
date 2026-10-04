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

package io.ballerina.lib.azure.storage.blob.util;

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
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

/**
 * Resolves the Ballerina {@code AuthConfig} union into the credential and endpoint inputs the
 * blob and queue client builders share. The union member is selected by the fields present;
 * every mode is validated locally, with no call to Azure.
 */
public final class Credentials {

    /** The storage services an endpoint is derived for. */
    public enum Service {
        BLOB("blob", "BlobEndpoint="),
        QUEUE("queue", "QueueEndpoint=");

        private final String host;
        private final String connectionStringKey;

        Service(String host, String connectionStringKey) {
            this.host = host;
            this.connectionStringKey = connectionStringKey;
        }
    }

    // The resolved credential: exactly one of the four credential fields is set.
    public record Resolved(String accountName, String explicitServiceUrl, String sasUrl,
                           AzureNamedKeyCredential namedKey, AzureSasCredential sas, TokenCredential token,
                           String connectionString) {

        /** The endpoint for a service: the explicit URL, else the one derived from the account name. */
        public String endpoint(Service service) {
            if (explicitServiceUrl != null) {
                return explicitServiceUrl;
            }
            if (sasUrl != null) {
                return sasUrl.substring(0, sasUrl.indexOf('?'));
            }
            return "https://" + accountName + "." + service.host + ".core.windows.net";
        }
    }

    // Field names of the auth records (read only here).
    private static final BString ACCOUNT_NAME = StringUtils.fromString("accountName");
    private static final BString ACCOUNT_KEY = StringUtils.fromString("accountKey");
    private static final BString SAS_TOKEN = StringUtils.fromString("sasToken");
    private static final BString SAS_URL = StringUtils.fromString("sasUrl");
    private static final BString CONNECTION_STRING = StringUtils.fromString("connectionString");
    private static final BString SERVICE_URL = StringUtils.fromString("serviceUrl");
    private static final BString KIND = StringUtils.fromString("kind");
    // The Entra credential-kind discriminator value selecting managed-identity auth.
    private static final String KIND_MANAGED_IDENTITY = "managed-identity";
    private static final BString TENANT_ID = StringUtils.fromString("tenantId");
    private static final BString CLIENT_ID = StringUtils.fromString("clientId");
    private static final BString CLIENT_SECRET = StringUtils.fromString("clientSecret");
    private static final BString CERTIFICATE_PATH = StringUtils.fromString("certificatePath");
    private static final BString CERTIFICATE_PASSWORD = StringUtils.fromString("certificatePassword");
    private static final BString TOKEN_FILE_PATH = StringUtils.fromString("tokenFilePath");

    private Credentials() {
    }

    /**
     * Resolves an {@code AuthConfig} record.
     *
     * @param auth the auth record
     * @return the credential and endpoint inputs
     */
    public static Resolved resolve(BMap<BString, Object> auth) {
        if (auth.containsKey(ACCOUNT_KEY)) {
            String accountName = requireNonEmpty(auth, ACCOUNT_NAME);
            String accountKey = requireNonEmpty(auth, ACCOUNT_KEY);
            try {
                Base64.getDecoder().decode(accountKey);
            } catch (IllegalArgumentException e) {
                throw BlobErrorCreator.clientError("accountKey is not a valid base64 string", e);
            }
            return new Resolved(accountName, serviceUrl(auth), null,
                    new AzureNamedKeyCredential(accountName, accountKey), null, null, null);
        }
        if (auth.containsKey(SAS_TOKEN)) {
            return new Resolved(requireNonEmpty(auth, ACCOUNT_NAME), null, null, null,
                    new AzureSasCredential(requireNonEmpty(auth, SAS_TOKEN)), null, null);
        }
        if (auth.containsKey(SAS_URL)) {
            String sasUrl = requireNonEmpty(auth, SAS_URL);
            URI uri;
            try {
                uri = new URI(sasUrl);
            } catch (URISyntaxException e) {
                throw BlobErrorCreator.clientError("sasUrl is not a valid URL", e);
            }
            if (!isHttp(uri)) {
                throw BlobErrorCreator.clientError("sasUrl must use the http or https scheme", null);
            }
            if (uri.getRawQuery() == null || !uri.getRawQuery().contains("sig=")) {
                throw BlobErrorCreator.clientError(
                        "sasUrl carries no SAS token (no `sig=` in its query); for a bare token use SasConfig", null);
            }
            return new Resolved(null, null, sasUrl, null, new AzureSasCredential(uri.getRawQuery()), null, null);
        }
        if (auth.containsKey(CONNECTION_STRING)) {
            return new Resolved(null, null, null, null, null, null, requireNonEmpty(auth, CONNECTION_STRING));
        }
        return new Resolved(requireNonEmpty(auth, ACCOUNT_NAME), serviceUrl(auth), null, null, null,
                entraCredential(auth), null);
    }

    /**
     * Reads the endpoint for a service out of a connection string, when it names one.
     *
     * @param connectionString the connection string
     * @param service          the service
     * @return the endpoint, or {@code null} when the string names none for that service
     */
    public static String connectionStringEndpoint(String connectionString, Service service) {
        for (String pair : connectionString.split(";")) {
            if (pair.startsWith(service.connectionStringKey)) {
                return pair.substring(service.connectionStringKey.length());
            }
        }
        return null;
    }

    /** Checks that a connection string can yield an endpoint for the service. */
    public static void requireConnectionStringEndpoint(String connectionString, Service service) {
        if (connectionStringEndpoint(connectionString, service) == null && !connectionString.contains("AccountName=")) {
            throw BlobErrorCreator.clientError("the connection string must include " + service.connectionStringKey
                    + " or AccountName= so the " + service.host + "-service endpoint can be derived", null);
        }
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
    /**
     * The pipeline policy that keeps an explicit port on requests, or {@code null} when the URL
     * carries none.
     *
     * @param url the configured endpoint
     * @return the policy, or {@code null}
     */
    public static HttpPipelinePolicy portOverride(String url) {
        URI uri = URI.create(url);
        int port = uri.getPort();
        if (port == -1) {
            return null;
        }
        String scheme = uri.getScheme();
        String host = uri.getHost();
        return (context, next) -> {
            UrlBuilder requestUrl = UrlBuilder.parse(context.getHttpRequest().getUrl());
            if (!host.equalsIgnoreCase(requestUrl.getHost())) {
                return next.process();
            }
            requestUrl.setScheme(scheme).setHost(host).setPort(port);
            context.getHttpRequest().setUrl(requestUrl.toString());
            return next.process();
        };
    }

    /**
     * Validates that a URL is absolute with an http or https scheme.
     *
     * @param url       the URL
     * @param fieldName the configuration field it came from, for the message
     * @return the URL
     */
    public static String validateUrl(String url, String fieldName) {
        try {
            URI uri = new URI(url);
            if (!isHttp(uri)) {
                throw BlobErrorCreator.clientError(fieldName + " must use the http or https scheme", null);
            }
            return url;
        } catch (URISyntaxException e) {
            throw BlobErrorCreator.clientError(fieldName + " is not a valid URL", e);
        }
    }

    /*
     * Configures a Microsoft Entra ID credential. The record kind is chosen structurally: a
     * secret, a certificate path, or a token file path names its credential outright; otherwise
     * the `kind` discriminator separates the default chain from a managed identity.
     */
    private static TokenCredential entraCredential(BMap<BString, Object> auth) {
        if (auth.containsKey(CLIENT_SECRET)) {
            return new ClientSecretCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID))
                    .clientSecret(requireNonEmpty(auth, CLIENT_SECRET))
                    .build();
        }
        if (auth.containsKey(CERTIFICATE_PATH)) {
            String certificatePath = requireNonEmpty(auth, CERTIFICATE_PATH);
            if (!Files.isRegularFile(Path.of(certificatePath))) {
                throw BlobErrorCreator.clientError(
                        "certificatePath does not point to a readable file: " + certificatePath, null);
            }
            ClientCertificateCredentialBuilder certificateBuilder = new ClientCertificateCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID));
            String certificatePassword = ValueUtils.optString(auth, CERTIFICATE_PASSWORD);
            return (certificatePassword == null
                    ? certificateBuilder.pemCertificate(certificatePath)
                    : certificateBuilder.pfxCertificate(certificatePath, certificatePassword))
                    .build();
        }
        if (auth.containsKey(TOKEN_FILE_PATH)) {
            return new WorkloadIdentityCredentialBuilder()
                    .tenantId(requireNonEmpty(auth, TENANT_ID))
                    .clientId(requireNonEmpty(auth, CLIENT_ID))
                    .tokenFilePath(requireNonEmpty(auth, TOKEN_FILE_PATH))
                    .build();
        }
        String clientId = ValueUtils.optString(auth, CLIENT_ID);
        if (KIND_MANAGED_IDENTITY.equals(ValueUtils.optString(auth, KIND))) {
            ManagedIdentityCredentialBuilder managedBuilder = new ManagedIdentityCredentialBuilder();
            if (clientId != null) {
                managedBuilder.clientId(clientId);
            }
            return managedBuilder.build();
        }
        DefaultAzureCredentialBuilder chainBuilder = new DefaultAzureCredentialBuilder();
        if (clientId != null) {
            chainBuilder.managedIdentityClientId(clientId);
        }
        return chainBuilder.build();
    }

    private static String serviceUrl(BMap<BString, Object> auth) {
        String serviceUrl = ValueUtils.optString(auth, SERVICE_URL);
        return serviceUrl == null ? null : validateUrl(serviceUrl, "serviceUrl");
    }

    private static boolean isHttp(URI uri) {
        return uri.getScheme() != null && (uri.getScheme().equals("https") || uri.getScheme().equals("http"));
    }

    private static String requireNonEmpty(BMap<BString, Object> record, BString field) {
        String value = ValueUtils.optString(record, field);
        if (value == null || value.strip().isEmpty()) {
            throw BlobErrorCreator.clientError(field.getValue() + " must not be empty", null);
        }
        return value.strip();
    }
}
