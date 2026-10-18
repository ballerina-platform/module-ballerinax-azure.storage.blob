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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * The listener's local credential checks: the kind of SAS it accepts (spec 2.1) and the account
 * it reads from, resolved for the account check (spec 5.8).
 */
public class CredentialsTest {

    @Test
    public void anAccountSasCoveringBothServicesPasses() {
        Credentials.checkListenerSas("sv=2024-11-04&ss=bq&srt=co&sp=racup&se=2026-12-01T00:00:00Z&sig=abc");
        Credentials.checkListenerSas("sv=2024-11-04&ss=bqt&srt=sco&sp=r&sig=abc");
        Credentials.checkListenerSas(null);
    }

    @Test
    public void aServiceSasIsRejected() {
        ConfigurationException e = expectThrows(ConfigurationException.class,
                () -> Credentials.checkListenerSas("sv=2024-11-04&sr=c&sp=rl&sig=abc"));
        assertTrue(e.getMessage().contains("service SAS"), e.getMessage());
    }

    @Test
    public void anAccountSasMissingAServiceIsRejectedNamingIt() {
        ConfigurationException blobOnly = expectThrows(ConfigurationException.class,
                () -> Credentials.checkListenerSas("sv=2024-11-04&ss=b&srt=co&sp=racup&sig=abc"));
        assertTrue(blobOnly.getMessage().contains("ss must include q"), blobOnly.getMessage());
        ConfigurationException queueOnly = expectThrows(ConfigurationException.class,
                () -> Credentials.checkListenerSas("sv=2024-11-04&ss=q&srt=co&sp=racup&sig=abc"));
        assertTrue(queueOnly.getMessage().contains("ss must include b"), queueOnly.getMessage());
        ConfigurationException none = expectThrows(ConfigurationException.class,
                () -> Credentials.checkListenerSas("sv=2024-11-04&sp=racup&sig=abc"));
        assertTrue(none.getMessage().contains("ss must include b"), none.getMessage());
    }

    @Test
    public void permissionLettersAreNotCheckedLocally() {
        Credentials.checkListenerSas("sv=2024-11-04&ss=bq&srt=o&sp=r&sig=abc");
    }

    @Test
    public void theListenerAccountFollowsTheResolutionOrder() {
        assertEquals(Credentials.listenerAccountName(
                new Credentials.Resolved("MyAcct", "https://other.blob.core.windows.net", null, null, null, null,
                        null)),
                "MyAcct");
        assertEquals(Credentials.listenerAccountName(new Credentials.Resolved(null, null, null, null, null, null,
                "DefaultEndpointsProtocol=https;AccountName=csacct;AccountKey=k")), "csacct");
        assertEquals(Credentials.listenerAccountName(new Credentials.Resolved(null, null, null, null, null, null,
                "UseDevelopmentStorage=true")), "devstoreaccount1");
        assertNull(Credentials.listenerAccountName(new Credentials.Resolved(null, null, null, null, null, null,
                "BlobEndpoint=https://files.contoso.com;SharedAccessSignature=sv=1&ss=bq&sig=x")));
        assertEquals(Credentials.listenerAccountName(new Credentials.Resolved(null, null,
                "https://sasacct.blob.core.windows.net/?sv=1&ss=bq&sig=x", null, null, null, null)), "sasacct");
    }

    @Test
    public void theAccountIsReadFromABlobHostOrAnEmulatorPathOnly() {
        assertEquals(Credentials.accountFromEndpoint("https://acc.blob.core.windows.net"), "acc");
        assertEquals(Credentials.accountFromEndpoint("https://acc.blob.core.chinacloudapi.cn/"), "acc");
        assertEquals(Credentials.accountFromEndpoint("https://acc.privatelink.blob.core.windows.net"), "acc");
        assertEquals(Credentials.accountFromEndpoint("https://acc.z13.blob.storage.azure.net"), "acc");
        assertEquals(Credentials.accountFromEndpoint("http://127.0.0.1:10000/devstoreaccount1"), "devstoreaccount1");
        assertEquals(Credentials.accountFromEndpoint("http://localhost:10000/devstoreaccount1/c"), "devstoreaccount1");
        assertNull(Credentials.accountFromEndpoint("https://files.contoso.com"));
        assertNull(Credentials.accountFromEndpoint("https://files.contoso.com/acc"));
        assertNull(Credentials.accountFromEndpoint("not a url"));
    }
}
