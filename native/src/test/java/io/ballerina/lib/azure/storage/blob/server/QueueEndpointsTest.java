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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

/**
 * Queue endpoint derivation against spec 5.2: the override, the blob host relabelled, every
 * connection-string form, and the account default.
 */
public class QueueEndpointsTest {

    private static Credentials.Resolved sharedKey(String account, String serviceUrl) {
        return new Credentials.Resolved(account, serviceUrl, null, null, null, null, null);
    }

    private static Credentials.Resolved sasUrl(String url) {
        return new Credentials.Resolved(null, null, url, null, null, null, null);
    }

    private static Credentials.Resolved connectionString(String value) {
        return new Credentials.Resolved(null, null, null, null, null, null, value);
    }

    @Test
    public void theOverrideWinsOverEveryDerivation() {
        String override = "https://queues.contoso.com";
        assertEquals(QueueEndpoints.derive(override, sharedKey("acc", null)), override);
        assertEquals(QueueEndpoints.derive(override, sharedKey("acc", "https://acc.blob.core.windows.net")), override);
        assertEquals(QueueEndpoints.derive(override, sasUrl("https://files.contoso.com/?sv=1&sig=x")), override);
        assertEquals(QueueEndpoints.derive(override, connectionString("AccountName=acc;AccountKey=k")), override);
    }

    @Test
    public void aBareAccountNameGivesThePublicQueueHost() {
        assertEquals(QueueEndpoints.derive(null, sharedKey("acc", null)), "https://acc.queue.core.windows.net");
    }

    @Test
    public void anExplicitServiceUrlIsRelabelled() {
        assertEquals(QueueEndpoints.derive(null, sharedKey("acc", "https://acc.blob.core.windows.net")),
                "https://acc.queue.core.windows.net");
        assertEquals(QueueEndpoints.derive(null, sharedKey("acc", "https://acc.blob.core.chinacloudapi.cn/")),
                "https://acc.queue.core.chinacloudapi.cn");
        assertEquals(QueueEndpoints.derive(null, sharedKey("acc", "https://acc.privatelink.blob.core.windows.net")),
                "https://acc.privatelink.queue.core.windows.net");
        assertEquals(QueueEndpoints.derive(null, sharedKey("acc", "https://acc.z13.blob.storage.azure.net:8443")),
                "https://acc.z13.queue.storage.azure.net:8443");
    }

    @Test
    public void aServiceUrlWithoutTheBlobLabelNeedsTheOverride() {
        for (String host : new String[]{"https://files.contoso.com", "https://10.0.0.5", "http://127.0.0.1:10000"
                + "/devstoreaccount1", "http://localhost:10000/devstoreaccount1"}) {
            ConfigurationException e = expectThrows(ConfigurationException.class,
                    () -> QueueEndpoints.derive(null, sharedKey("acc", host)));
            assertEquals(e.getMessage().contains("queueServiceUrl is required"), true, e.getMessage());
        }
    }

    @Test
    public void aSasUrlIsRelabelledOrNeedsTheOverride() {
        assertEquals(QueueEndpoints.derive(null, sasUrl("https://acc.blob.core.windows.net/?sv=2024&ss=bq&sig=x")),
                "https://acc.queue.core.windows.net");
        assertThrows(ConfigurationException.class,
                () -> QueueEndpoints.derive(null, sasUrl("https://files.contoso.com/?sv=2024&ss=bq&sig=x")));
    }

    @Test
    public void connectionStringsFollowTheirOwnOrder() {
        assertEquals(QueueEndpoints.fromConnectionString(
                "DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=k;"
                        + "BlobEndpoint=https://acc.blob.core.windows.net;"
                        + "QueueEndpoint=https://acc.queue.core.windows.net/"),
                "https://acc.queue.core.windows.net");
        assertEquals(QueueEndpoints.fromConnectionString(
                "AccountName=acc;AccountKey=k;BlobEndpoint=https://acc.privatelink.blob.core.windows.net"),
                "https://acc.privatelink.queue.core.windows.net");
        assertThrows(ConfigurationException.class, () -> QueueEndpoints.fromConnectionString(
                "AccountName=devstoreaccount1;AccountKey=k;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"));
        assertEquals(QueueEndpoints.fromConnectionString(
                "DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=k;EndpointSuffix=core.chinacloudapi.cn"),
                "https://acc.queue.core.chinacloudapi.cn");
        assertEquals(QueueEndpoints.fromConnectionString("AccountName=acc;AccountKey=k"),
                "https://acc.queue.core.windows.net");
        assertEquals(QueueEndpoints.fromConnectionString("DefaultEndpointsProtocol=http;AccountName=acc;AccountKey=k"),
                "http://acc.queue.core.windows.net");
        assertEquals(QueueEndpoints.fromConnectionString("UseDevelopmentStorage=true"),
                "http://127.0.0.1:10001/devstoreaccount1");
        assertEquals(QueueEndpoints.fromConnectionString(
                "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://azurite"),
                "http://azurite:10001/devstoreaccount1");
        assertThrows(ConfigurationException.class, () -> QueueEndpoints.fromConnectionString(
                "BlobEndpoint=https://files.contoso.com;SharedAccessSignature=sv=2024&ss=bq&sig=x"));
        assertThrows(ConfigurationException.class,
                () -> QueueEndpoints.fromConnectionString("SharedAccessSignature=sv=2024&ss=bq&sig=x"));
    }

    @Test
    public void connectionStringPairsKeepValuesWithEqualsSigns() {
        assertEquals(QueueEndpoints.connectionStringPairs("A=1;SharedAccessSignature=sv=2024&sig=a=b; B = 2 ")
                .get("sharedaccesssignature"), "sv=2024&sig=a=b");
        assertEquals(QueueEndpoints.connectionStringPairs("A=1; B = 2 ").get("b"), "2");
    }
}
