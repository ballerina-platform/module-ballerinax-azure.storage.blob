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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Resolves the Ballerina {@code AuthConfig} union into the credential and endpoint inputs the
 * blob and queue client builders share. The union member is selected by the fields present;
 * every mode is validated locally, with no call to Azure.
 */
public final class Credentials {

    /** The storage services an endpoint is derived for. */
    public enum Service {
        BLOB("blob"),
        QUEUE("queue");

        private final String host;

        Service(String host) {
            this.host = host;
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
     * The SAS query of an auth record, in whichever form carries it: a bare token, the query of a
     * SAS URL, or the {@code SharedAccessSignature} pair of a connection string.
     *
     * @param auth the auth record
     * @return the SAS query without a leading {@code ?}, or {@code null} for the other forms
     */
    public static String sasSignature(BMap<BString, Object> auth) {
        String signature = null;
        if (auth.containsKey(SAS_TOKEN)) {
            signature = requireNonEmpty(auth, SAS_TOKEN);
        } else if (auth.containsKey(SAS_URL)) {
            signature = URI.create(requireNonEmpty(auth, SAS_URL)).getRawQuery();
        } else if (auth.containsKey(CONNECTION_STRING)) {
            // Split the way the SDK's own parser splits it.
            for (String pair : requireNonEmpty(auth, CONNECTION_STRING).split(";")) {
                int separator = pair.indexOf('=');
                if (separator > 0 && pair.substring(0, separator).strip().equalsIgnoreCase("SharedAccessSignature")) {
                    signature = pair.substring(separator + 1).strip();
                }
            }
        }
        if (signature == null || signature.isEmpty()) {
            return null;
        }
        return signature.startsWith("?") ? signature.substring(1) : signature;
    }

    /**
     * Checks that a SAS a listener authenticates with is an account SAS covering the blob and the
     * queue services: a service SAS (an {@code sr} parameter) cannot poll a queue, and an account
     * SAS whose {@code ss} lacks {@code b} or {@code q} cannot reach one of the two. Permission
     * letters are not checked; a missing one fails at the first call that needs it.
     *
     * @param sasQuery the SAS query without a leading {@code ?}, or {@code null} for a credential
     *                 that is no SAS
     * @throws ConfigurationException when the SAS is of the wrong kind or covers too little
     */
    public static void checkListenerSas(String sasQuery) {
        if (sasQuery == null) {
            return;
        }
        Map<String, String> parameters = queryParameters(sasQuery);
        if (parameters.containsKey("sr")) {
            throw new ConfigurationException("the listener needs an account SAS covering the blob and queue "
                    + "services; a service SAS (sr) cannot poll a queue");
        }
        String services = parameters.getOrDefault("ss", "");
        if (!services.contains("b")) {
            throw new ConfigurationException("the listener's SAS covers no blob service: ss must include b");
        }
        if (!services.contains("q")) {
            throw new ConfigurationException("the listener's SAS covers no queue service: ss must include q");
        }
    }

    /**
     * The storage account a listener's blob operations act on, as far as the credential tells:
     * an explicit account name, a connection string's {@code AccountName}, the first host label
     * of a blob endpoint that carries the {@code .blob.} service label, or the account segment of
     * an emulator address's path; otherwise unknown.
     *
     * @param credential the resolved credential
     * @return the account name, or {@code null} when it cannot be told
     */
    public static String listenerAccountName(Resolved credential) {
        if (credential.accountName() != null) {
            return credential.accountName();
        }
        if (credential.connectionString() != null) {
            return accountFromConnectionString(credential.connectionString());
        }
        String endpoint = credential.explicitServiceUrl() != null
                ? credential.explicitServiceUrl() : credential.sasUrl();
        return endpoint == null ? null : accountFromEndpoint(endpoint);
    }

    /**
     * The account a connection string names: its {@code AccountName}, or the development account
     * for the emulator shorthand.
     *
     * @param connectionString the connection string
     * @return the account name, or {@code null}
     */
    public static String accountFromConnectionString(String connectionString) {
        Map<String, String> pairs = new LinkedHashMap<>();
        for (String pair : connectionString.split(";")) {
            int separator = pair.indexOf('=');
            if (separator > 0) {
                pairs.put(pair.substring(0, separator).strip().toLowerCase(Locale.ROOT),
                        pair.substring(separator + 1).strip());
            }
        }
        if ("true".equalsIgnoreCase(pairs.get("usedevelopmentstorage"))) {
            return "devstoreaccount1";
        }
        String account = pairs.get("accountname");
        return account == null || account.isBlank() ? null : account;
    }

    /**
     * The account a blob endpoint URL names: the first host label when the host carries the
     * {@code .blob.} service label (standard, sovereign, private-link and DNS-zone hosts), or the
     * first path segment of an emulator address (an IP or {@code localhost}); a custom domain
     * names none.
     *
     * @param endpoint the endpoint URL, with or without a SAS query
     * @return the account name, or {@code null}
     */
    public static String accountFromEndpoint(String endpoint) {
        URI uri;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            return null;
        }
        String host = uri.getHost();
        if (host == null) {
            return null;
        }
        String lower = host.toLowerCase(Locale.ROOT);
        if (lower.contains(".blob.") && !lower.startsWith(".blob.")) {
            return host.substring(0, host.indexOf('.'));
        }
        boolean emulator = lower.equals("localhost") || lower.startsWith("[")
                || lower.matches("\\d+\\.\\d+\\.\\d+\\.\\d+");
        if (emulator && uri.getPath() != null) {
            String[] segments = uri.getPath().split("/");
            for (String segment : segments) {
                if (!segment.isEmpty()) {
                    return segment;
                }
            }
        }
        return null;
    }

    private static Map<String, String> queryParameters(String query) {
        Map<String, String> parameters = new LinkedHashMap<>();
        for (String pair : query.split("&")) {
            int separator = pair.indexOf('=');
            String key = separator < 0 ? pair : pair.substring(0, separator);
            parameters.put(key, separator < 0 ? "" : pair.substring(separator + 1));
        }
        return parameters;
    }

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
