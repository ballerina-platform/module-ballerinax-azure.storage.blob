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

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * The event parser against spec 5.7, 5.8 and 5.14: both delivery schemas, the account taken from
 * the topic or source, the dfs URL rewrite, the no-content rules, and what is unparseable.
 */
public class EventParserTest {

    private static final String TOPIC = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg"
            + "/providers/Microsoft.Storage/storageAccounts/myacct";

    private static String eventGrid(String eventType, String api, String blobType, String topic, String url) {
        String blobTypeField = blobType == null ? "" : "\"blobType\": \"" + blobType + "\",";
        String topicField = topic == null ? "" : "\"topic\": \"" + topic + "\",";
        return "{" + topicField
                + "\"subject\": \"/blobServices/default/containers/invoices/blobs/2026/a.json\","
                + "\"eventType\": \"" + eventType + "\","
                + "\"eventTime\": \"2026-10-18T10:00:00.1234567Z\","
                + "\"id\": \"evt-1\","
                + "\"data\": {\"api\": \"" + api + "\", \"contentType\": \"application/json\", \"contentLength\": 42,"
                + blobTypeField + "\"url\": \"" + url + "\", \"sequencer\": \"0000000000000042\","
                + "\"eTag\": \"0x8D4\"},"
                + "\"dataVersion\": \"\", \"metadataVersion\": \"1\"}";
    }

    private static String created(String api, String blobType) {
        return eventGrid("Microsoft.Storage.BlobCreated", api, blobType, TOPIC,
                "https://myacct.blob.core.windows.net/invoices/2026/a.json");
    }

    @Test
    public void parsesTheEventGridSchema() {
        EventParser.Parsed parsed = EventParser.parse(created("PutBlob", "BlockBlob"));
        assertTrue(parsed.dispatchable());
        assertFalse(parsed.deleted());
        assertEquals(parsed.accountName(), "myacct");
        assertEquals(parsed.containerName(), "invoices");
        assertEquals(parsed.path(), "2026/a.json");
        assertEquals(parsed.blobName(), "a.json");
        assertEquals(parsed.api(), "PutBlob");
        assertEquals(parsed.contentLength(), Long.valueOf(42));
        assertEquals(parsed.eTag(), "0x8D4");
        assertEquals(parsed.eventTime().toInstant().getEpochSecond(), 1792317600L);
        assertFalse(parsed.noContent());
    }

    @Test
    public void parsesTheCloudEventsSchemaAndBase64Bodies() {
        String cloudEvent = "{\"specversion\": \"1.0\", \"source\": \"" + TOPIC + "\","
                + "\"subject\": \"/blobServices/default/containers/c/blobs/x.txt\","
                + "\"type\": \"Microsoft.Storage.BlobDeleted\", \"time\": \"2026-10-18T10:00:00Z\", \"id\": \"1\","
                + "\"data\": {\"api\": \"DeleteBlob\", \"url\": \"https://myacct.blob.core.windows.net/c/x.txt\","
                + "\"sequencer\": \"1\"}}";
        String base64 = Base64.getEncoder().encodeToString(cloudEvent.getBytes(StandardCharsets.UTF_8));
        EventParser.Parsed parsed = EventParser.parse(base64);
        assertTrue(parsed.deleted());
        assertEquals(parsed.accountName(), "myacct");
        assertEquals(parsed.containerName(), "c");
        assertNull(parsed.contentLength());
    }

    @Test
    public void acceptsAOneElementArrayAndRejectsOthers() {
        assertTrue(EventParser.parse("[" + created("PutBlob", "BlockBlob") + "]").dispatchable());
        assertThrows(EventParser.EventParseException.class, () -> EventParser.parse("[]"));
        assertThrows(EventParser.EventParseException.class,
                () -> EventParser.parse("[" + created("PutBlob", null) + "," + created("PutBlob", null) + "]"));
    }

    @Test
    public void otherEventTypesAreNotDispatchable() {
        EventParser.Parsed parsed = EventParser.parse(created("SetBlobTier", "BlockBlob")
                .replace("Microsoft.Storage.BlobCreated", "Microsoft.Storage.BlobTierChanged"));
        assertFalse(parsed.dispatchable());
        assertEquals(parsed.eventType(), "Microsoft.Storage.BlobTierChanged");
        assertFalse(parsed.noContent());
    }

    @Test
    public void noContentCreatesAreRecognisedAsADenyList() {
        assertTrue(EventParser.parse(created("CreateFile", null)).noContent());
        assertTrue(EventParser.parse(created("SftpCreate", "BlockBlob")).noContent());
        assertTrue(EventParser.parse(created("SftpWrite", "BlockBlob")).noContent());
        assertTrue(EventParser.parse(created("PutBlob", "AppendBlob")).noContent());
        assertTrue(EventParser.parse(created("PutBlob", "PageBlob")).noContent());
        // Absent or unknown blob types, copies of append blobs, and commits all dispatch.
        assertFalse(EventParser.parse(created("PutBlob", null)).noContent());
        assertFalse(EventParser.parse(created("PutBlob", "Mystery")).noContent());
        assertFalse(EventParser.parse(created("CopyBlob", "AppendBlob")).noContent());
        assertFalse(EventParser.parse(created("PutBlockList", "BlockBlob")).noContent());
        assertFalse(EventParser.parse(created("FlushWithClose", null)).noContent());
        assertFalse(EventParser.parse(created("SftpCommit", "BlockBlob")).noContent());
    }

    @Test
    public void aBlobEventWithoutAStorageAccountIsUnparseable() {
        String relayed = eventGrid("Microsoft.Storage.BlobCreated", "PutBlob", "BlockBlob",
                "/subscriptions/x/resourceGroups/rg/providers/Microsoft.EventGrid/topics/relay",
                "https://myacct.blob.core.windows.net/invoices/2026/a.json");
        assertThrows(EventParser.EventParseException.class, () -> EventParser.parse(relayed));
        String noTopic = eventGrid("Microsoft.Storage.BlobCreated", "PutBlob", "BlockBlob", null,
                "https://myacct.blob.core.windows.net/invoices/2026/a.json");
        assertThrows(EventParser.EventParseException.class, () -> EventParser.parse(noTopic));
        // A non-blob event type needs no account.
        String otherType = eventGrid("Microsoft.Storage.BlobTierChanged", "SetBlobTier", "BlockBlob", null,
                "https://myacct.blob.core.windows.net/invoices/2026/a.json");
        assertFalse(EventParser.parse(otherType).dispatchable());
    }

    @Test
    public void accountNameIsTheSegmentAfterStorageAccounts() {
        assertEquals(EventParser.accountName(TOPIC), "myacct");
        assertEquals(EventParser.accountName(TOPIC + "/"), "myacct");
        assertEquals(EventParser.accountName(TOPIC + "/blobServices/default"), "myacct");
        assertNull(EventParser.accountName("/providers/Microsoft.Storage/storageAccounts/"));
        assertNull(EventParser.accountName("/providers/Microsoft.EventGrid/topics/t"));
        assertNull(EventParser.accountName(null));
    }

    @Test
    public void dfsUrlsAreRewrittenToTheBlobEndpoint() {
        assertEquals(EventParser.blobEndpointUrl("https://myacct.dfs.core.windows.net/fs/dir/a.json"),
                "https://myacct.blob.core.windows.net/fs/dir/a.json");
        assertEquals(EventParser.blobEndpointUrl("https://myacct.dfs.core.chinacloudapi.cn/fs/a"),
                "https://myacct.blob.core.chinacloudapi.cn/fs/a");
        assertEquals(EventParser.blobEndpointUrl("https://myacct.blob.core.windows.net/c/a"),
                "https://myacct.blob.core.windows.net/c/a");
        assertEquals(EventParser.blobEndpointUrl("http://127.0.0.1:10000/devstoreaccount1/c/a"),
                "http://127.0.0.1:10000/devstoreaccount1/c/a");
        assertEquals(EventParser.blobEndpointUrl("not a url"), "not a url");
        EventParser.Parsed parsed = EventParser.parse(eventGrid("Microsoft.Storage.BlobCreated", "FlushWithClose",
                null, TOPIC, "https://myacct.dfs.core.windows.net/invoices/2026/a.json"));
        assertEquals(parsed.url(), "https://myacct.blob.core.windows.net/invoices/2026/a.json");
    }

    @Test
    public void malformedMessagesAreUnparseable() {
        assertThrows(EventParser.EventParseException.class, () -> EventParser.parse("not an event"));
        assertThrows(EventParser.EventParseException.class, () -> EventParser.parse("{\"eventType\": 7}"));
        assertThrows(EventParser.EventParseException.class,
                () -> EventParser.parse(created("PutBlob", null).replace("\"sequencer\": \"0000000000000042\",", "")));
        assertThrows(EventParser.EventParseException.class,
                () -> EventParser.parse(created("PutBlob", null).replace("2026-10-18T10:00:00.1234567Z", "yesterday")));
        assertThrows(EventParser.EventParseException.class,
                () -> EventParser.parse(created("PutBlob", null).replace("/blobServices/default/containers/",
                        "/queueServices/")));
    }
}
