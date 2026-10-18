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

import io.ballerina.lib.azure.storage.blob.util.ConfigurationException;
import io.ballerina.lib.azure.storage.blob.util.Credentials;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Derives the queue service endpoint a listener polls from its credential, in the order the
 * specification fixes: an explicit {@code queueServiceUrl}, else the blob endpoint the credential
 * names with its {@code .blob.} host label swapped for {@code .queue.}, else the connection
 * string's own endpoint rules, else the account's public queue host. Pure: no runtime values, so
 * the rules are unit-tested directly.
 */
final class QueueEndpoints {

    private static final String BLOB_LABEL = ".blob.";
    private static final String QUEUE_LABEL = ".queue.";
    private static final String DEVELOPMENT_ACCOUNT = "devstoreaccount1";
    private static final String DEFAULT_PROXY = "http://127.0.0.1";
    private static final String DEFAULT_SUFFIX = "core.windows.net";
    private static final String DEFAULT_PROTOCOL = "https";

    private QueueEndpoints() {
    }

    /**
     * Derives the queue endpoint.
     *
     * @param queueServiceUrl the configured override, already validated, or {@code null}
     * @param credential      the resolved credential
     * @return the queue endpoint URL
     * @throws ConfigurationException when the credential names a host the rules cannot relabel
     *                                and no {@code queueServiceUrl} is given
     */
    static String derive(String queueServiceUrl, Credentials.Resolved credential) {
        if (queueServiceUrl != null) {
            return queueServiceUrl;
        }
        if (credential.sasUrl() != null) {
            return swapBlobLabel(credential.sasUrl(), "the SAS URL's host");
        }
        if (credential.explicitServiceUrl() != null) {
            return swapBlobLabel(credential.explicitServiceUrl(), "the serviceUrl host");
        }
        if (credential.connectionString() != null) {
            return fromConnectionString(credential.connectionString());
        }
        return DEFAULT_PROTOCOL + "://" + credential.accountName() + QUEUE_LABEL + DEFAULT_SUFFIX;
    }

    /**
     * The queue endpoint a connection string implies: its {@code QueueEndpoint}; else its explicit
     * {@code BlobEndpoint} relabelled; else its account name with its protocol and endpoint suffix;
     * the development-storage shorthand maps to the emulator's queue endpoint.
     *
     * @param connectionString the connection string
     * @return the queue endpoint URL
     * @throws ConfigurationException when no queue endpoint can be derived
     */
    static String fromConnectionString(String connectionString) {
        Map<String, String> pairs = connectionStringPairs(connectionString);
        if ("true".equalsIgnoreCase(pairs.get("usedevelopmentstorage"))) {
            String proxy = pairs.getOrDefault("developmentstorageproxyuri", DEFAULT_PROXY);
            return stripTrailingSlash(proxy) + ":10001/" + DEVELOPMENT_ACCOUNT;
        }
        String queueEndpoint = pairs.get("queueendpoint");
        if (queueEndpoint != null && !queueEndpoint.isBlank()) {
            return stripTrailingSlash(queueEndpoint);
        }
        String blobEndpoint = pairs.get("blobendpoint");
        if (blobEndpoint != null && !blobEndpoint.isBlank()) {
            return swapBlobLabel(blobEndpoint, "the connection string's BlobEndpoint host");
        }
        String account = pairs.get("accountname");
        if (account == null || account.isBlank()) {
            throw new ConfigurationException("the connection string names no QueueEndpoint, BlobEndpoint or "
                    + "AccountName, so no queue endpoint can be derived; set queueServiceUrl");
        }
        String protocol = pairs.getOrDefault("defaultendpointsprotocol", DEFAULT_PROTOCOL);
        String suffix = pairs.getOrDefault("endpointsuffix", DEFAULT_SUFFIX);
        return protocol + "://" + account + QUEUE_LABEL + suffix;
    }

    /**
     * Splits a connection string into its pairs, keyed by the lower-cased name, the way the SDK's
     * own parser splits it (a value may itself contain {@code =}, as a SAS does).
     *
     * @param connectionString the connection string
     * @return the pairs
     */
    static Map<String, String> connectionStringPairs(String connectionString) {
        Map<String, String> pairs = new LinkedHashMap<>();
        for (String pair : connectionString.split(";")) {
            int separator = pair.indexOf('=');
            if (separator > 0) {
                pairs.put(pair.substring(0, separator).strip().toLowerCase(Locale.ROOT),
                        pair.substring(separator + 1).strip());
            }
        }
        return pairs;
    }

    // The URL's scheme and host with the blob service label replaced by the queue one, keeping an
    // explicit port; a host without the label (a custom domain, an IP address, an emulator
    // address) cannot be relabelled.
    private static String swapBlobLabel(String url, String what) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new ConfigurationException(what + " is not a valid URL: " + url);
        }
        String host = uri.getHost();
        if (host == null || !host.toLowerCase(Locale.ROOT).contains(BLOB_LABEL)) {
            throw new ConfigurationException("queueServiceUrl is required: " + what
                    + " carries no blob service label to derive the queue endpoint from");
        }
        int index = host.toLowerCase(Locale.ROOT).indexOf(BLOB_LABEL);
        String queueHost = host.substring(0, index) + QUEUE_LABEL + host.substring(index + BLOB_LABEL.length());
        String port = uri.getPort() == -1 ? "" : ":" + uri.getPort();
        return uri.getScheme() + "://" + queueHost + port;
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
