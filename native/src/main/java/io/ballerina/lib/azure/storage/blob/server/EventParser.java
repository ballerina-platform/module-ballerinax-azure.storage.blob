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

import com.azure.json.JsonProviders;
import com.azure.json.JsonReader;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a delivered message into a storage event. The body is base64-decoded when that yields
 * JSON, else read as is; both the Event Grid schema and the CloudEvents schema are accepted,
 * detected per message. The parsing is pure and raises {@link EventParseException}; the
 * {@code parseEvent} extern and the dispatcher turn that into the module's {@code Error}.
 */
public final class EventParser {

    private static final String BLOB_CREATED = "Microsoft.Storage.BlobCreated";
    private static final String BLOB_DELETED = "Microsoft.Storage.BlobDeleted";
    private static final Pattern SUBJECT = Pattern.compile("^/blobServices/default/containers/([^/]+)/blobs/(.*)$");
    private static final String STORAGE_ACCOUNTS_SEGMENT = "/storageAccounts/";
    private static final String DFS_LABEL = ".dfs.";
    private static final String BLOB_LABEL = ".blob.";
    // Created events whose operation commits no content (spec 5.7).
    static final Set<String> NO_CONTENT_APIS = Set.of("CreateFile", "SftpCreate", "SftpWrite");
    private static final String PUT_BLOB = "PutBlob";
    private static final Set<String> NO_CONTENT_BLOB_TYPES = Set.of("AppendBlob", "PageBlob");

    // BlobEvent record and field names.
    private static final String RECORD_BLOB_EVENT = "BlobEvent";
    private static final BString EVENT_TYPE = StringUtils.fromString("eventType");
    private static final BString ACCOUNT_NAME = StringUtils.fromString("accountName");
    private static final BString CONTAINER_NAME = StringUtils.fromString("containerName");
    private static final BString PATH = StringUtils.fromString("path");
    private static final BString URL = StringUtils.fromString("url");
    private static final BString EVENT_TIME = StringUtils.fromString("eventTime");
    private static final BString API = StringUtils.fromString("api");
    private static final BString CONTENT_TYPE = StringUtils.fromString("contentType");
    private static final BString CONTENT_LENGTH = StringUtils.fromString("contentLength");
    private static final BString BLOB_TYPE = StringUtils.fromString("blobType");
    private static final BString E_TAG = StringUtils.fromString("eTag");
    private static final BString SEQUENCER = StringUtils.fromString("sequencer");
    private static final String TYPE_CREATED = "BLOB_CREATED";
    private static final String TYPE_DELETED = "BLOB_DELETED";

    private EventParser() {
    }

    /** A message that is not a storage event the module understands. */
    static final class EventParseException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        EventParseException(String message) {
            super(message);
        }

        EventParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // A parsed message: a blob created or deleted event, or another event type.
    record Parsed(String eventType, boolean deleted, String accountName, String containerName, String path,
                  String url, OffsetDateTime eventTime, String api, String contentType, Long contentLength,
                  String blobType, String eTag, String sequencer) {

        /** Whether this is a blob created or deleted event, the kinds the listener dispatches. */
        boolean dispatchable() {
            return containerName != null;
        }

        /**
         * Whether this is a no-content created event: one from an operation that commits no
         * content, or the creation of an append or page blob (recognised only when the event
         * carries the blob type).
         */
        boolean noContent() {
            if (!dispatchable() || deleted) {
                return false;
            }
            return NO_CONTENT_APIS.contains(api)
                    || (PUT_BLOB.equals(api) && blobType != null && NO_CONTENT_BLOB_TYPES.contains(blobType));
        }

        /** Builds the {@code BlobEvent} record for a dispatchable event. */
        BMap<BString, Object> toRecord() {
            BMap<BString, Object> record = ValueCreator.createRecordValue(ModuleUtils.getModule(), RECORD_BLOB_EVENT);
            record.put(EVENT_TYPE, StringUtils.fromString(deleted ? TYPE_DELETED : TYPE_CREATED));
            record.put(ACCOUNT_NAME, StringUtils.fromString(accountName));
            record.put(CONTAINER_NAME, StringUtils.fromString(containerName));
            record.put(PATH, StringUtils.fromString(path));
            record.put(URL, StringUtils.fromString(url));
            record.put(EVENT_TIME, ValueUtils.toUtc(eventTime));
            record.put(API, StringUtils.fromString(api));
            if (contentType != null) {
                record.put(CONTENT_TYPE, StringUtils.fromString(contentType));
            }
            if (contentLength != null) {
                record.put(CONTENT_LENGTH, contentLength);
            }
            if (blobType != null) {
                record.put(BLOB_TYPE, StringUtils.fromString(blobType));
            }
            if (eTag != null) {
                record.put(E_TAG, StringUtils.fromString(eTag));
            }
            record.put(SEQUENCER, StringUtils.fromString(sequencer));
            return record;
        }

        // The blob name, the last path segment, which the routing matches.
        String blobName() {
            int slash = path.lastIndexOf('/');
            return slash < 0 ? path : path.substring(slash + 1);
        }
    }

    /**
     * The {@code parseEvent} function: turns one delivered blob event into a {@code BlobEvent}.
     *
     * @param message the message body as bytes, a string, or parsed JSON
     * @return the {@code BlobEvent}, {@code null} for a valid event of another type or a
     *         no-content created event, or the module's {@code Error} for anything malformed
     */
    public static Object parseEvent(Object message) {
        String body;
        if (message instanceof BString text) {
            body = text.getValue();
        } else if (message instanceof BArray array
                && TypeUtils.getReferredType(array.getElementType()).getTag() == TypeTags.BYTE_TAG) {
            body = new String(array.getBytes(), StandardCharsets.UTF_8);
        } else if (message instanceof BMap || message instanceof BArray) {
            body = StringUtils.getJsonString(message);
        } else {
            return BlobErrorCreator.clientError("parseEvent takes the message as bytes, a string, or parsed JSON",
                    null);
        }
        try {
            Parsed parsed = parse(body);
            return parsed.dispatchable() && !parsed.noContent() ? parsed.toRecord() : null;
        } catch (EventParseException e) {
            return BlobErrorCreator.clientError(e.getMessage(), e.getCause());
        }
    }

    /**
     * Parses a message body.
     *
     * @param body the message text as received
     * @return the parsed event
     * @throws EventParseException when the body is not a storage event
     */
    static Parsed parse(String body) {
        Object root;
        try (JsonReader reader = JsonProviders.createReader(decode(body))) {
            root = reader.readUntyped();
        } catch (IOException | RuntimeException e) {
            throw new EventParseException("the message is not a JSON storage event", e);
        }
        if (root instanceof List<?> list) {
            if (list.size() != 1) {
                throw new EventParseException("the message carries " + list.size()
                        + " events; one event per message is expected");
            }
            root = list.get(0);
        }
        if (!(root instanceof Map<?, ?> event)) {
            throw new EventParseException("the message is not a JSON storage event");
        }
        // CloudEvents names the type `type`, the time `time` and the origin `source`; the Event
        // Grid schema uses `eventType`, `eventTime` and `topic`.
        boolean cloudEvents = event.containsKey("specversion");
        String eventType = string(event, cloudEvents ? "type" : "eventType");
        if (eventType == null) {
            throw new EventParseException("the message names no event type");
        }
        boolean created = BLOB_CREATED.equals(eventType);
        boolean deleted = BLOB_DELETED.equals(eventType);
        if (!created && !deleted) {
            return new Parsed(eventType, false, null, null, null, null, null, null, null, null, null, null, null);
        }
        String subject = string(event, "subject");
        Matcher matcher = subject == null ? null : SUBJECT.matcher(subject);
        if (matcher == null || !matcher.matches()) {
            throw new EventParseException("the event subject does not name a blob: " + subject);
        }
        String accountName = accountName(string(event, cloudEvents ? "source" : "topic"));
        if (accountName == null) {
            throw new EventParseException("the event names no storage account in its "
                    + (cloudEvents ? "source" : "topic"));
        }
        String time = string(event, cloudEvents ? "time" : "eventTime");
        if (time == null) {
            throw new EventParseException("the event carries no time");
        }
        OffsetDateTime eventTime;
        try {
            eventTime = OffsetDateTime.parse(time);
        } catch (DateTimeParseException e) {
            throw new EventParseException("the event time is not an ISO 8601 timestamp: " + time, e);
        }
        Map<?, ?> data = event.get("data") instanceof Map<?, ?> map ? map : Map.of();
        String url = string(data, "url");
        String sequencer = string(data, "sequencer");
        if (url == null || sequencer == null) {
            throw new EventParseException("the event data carries no url or sequencer");
        }
        Object length = data.get("contentLength");
        String api = string(data, "api");
        return new Parsed(eventType, deleted, accountName, matcher.group(1), matcher.group(2), blobEndpointUrl(url),
                eventTime, api == null ? "" : api, string(data, "contentType"),
                length instanceof Number number ? number.longValue() : null,
                string(data, "blobType"), string(data, "eTag"), sequencer);
    }

    // The storage account named by a topic or source: the segment after `/storageAccounts/`.
    static String accountName(String topicOrSource) {
        if (topicOrSource == null) {
            return null;
        }
        int index = topicOrSource.lastIndexOf(STORAGE_ACCOUNTS_SEGMENT);
        if (index < 0) {
            return null;
        }
        String rest = topicOrSource.substring(index + STORAGE_ACCOUNTS_SEGMENT.length());
        int slash = rest.indexOf('/');
        String name = slash < 0 ? rest : rest.substring(0, slash);
        return name.isEmpty() ? null : name;
    }

    // A Data Lake event carries the blob's URL on the dfs endpoint; the same path on the blob
    // endpoint is what a copy source and a client need.
    static String blobEndpointUrl(String url) {
        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host == null) {
                return url;
            }
            int index = host.toLowerCase(Locale.ROOT).indexOf(DFS_LABEL);
            if (index < 0) {
                return url;
            }
            String blobHost = host.substring(0, index) + BLOB_LABEL + host.substring(index + DFS_LABEL.length());
            return new URI(uri.getScheme(), uri.getRawUserInfo(), blobHost, uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment()).toString();
        } catch (URISyntaxException e) {
            return url;
        }
    }

    // Base64-decodes the body when the decoding yields JSON; otherwise the body is the JSON.
    private static String decode(String body) {
        String trimmed = body.strip();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return trimmed;
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(trimmed), StandardCharsets.UTF_8).strip();
            if (decoded.startsWith("{") || decoded.startsWith("[")) {
                return decoded;
            }
        } catch (IllegalArgumentException e) {
            // Not base64: the raw body is handed to the JSON reader, which reports the failure.
        }
        return trimmed;
    }

    private static String string(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return value instanceof String s ? s : null;
    }
}
