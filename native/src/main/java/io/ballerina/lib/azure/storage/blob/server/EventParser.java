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
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a queue message into a storage event. The body is base64-decoded when that yields
 * JSON, else read as is; both the Event Grid schema and the CloudEvents schema are accepted,
 * detected per message.
 */
final class EventParser {

    private static final String BLOB_CREATED = "Microsoft.Storage.BlobCreated";
    private static final String BLOB_DELETED = "Microsoft.Storage.BlobDeleted";
    private static final Pattern SUBJECT = Pattern.compile("^/blobServices/default/containers/([^/]+)/blobs/(.*)$");

    // BlobEvent record and field names.
    private static final String RECORD_BLOB_EVENT = "BlobEvent";
    private static final BString EVENT_TYPE = StringUtils.fromString("eventType");
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

    // A parsed message: a dispatchable blob event, or another event type to acknowledge.
    record Parsed(String eventType, boolean deleted, String containerName, String path, String url,
                  OffsetDateTime eventTime, String api, String contentType, Long contentLength, String blobType,
                  String eTag, String sequencer) {

        boolean dispatchable() {
            return containerName != null;
        }

        /** Builds the {@code BlobEvent} record for a dispatchable event. */
        BMap<BString, Object> toRecord() {
            BMap<BString, Object> record = ValueCreator.createRecordValue(ModuleUtils.getModule(), RECORD_BLOB_EVENT);
            record.put(EVENT_TYPE, StringUtils.fromString(deleted ? TYPE_DELETED : TYPE_CREATED));
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
     * Parses a message body.
     *
     * @param body the message text as received
     * @return the parsed event
     * @throws io.ballerina.runtime.api.values.BError when the body is not a storage event
     */
    static Parsed parse(String body) {
        Object root;
        try (JsonReader reader = JsonProviders.createReader(decode(body))) {
            root = reader.readUntyped();
        } catch (IOException | RuntimeException e) {
            throw BlobErrorCreator.clientError("the queue message is not a JSON storage event", e);
        }
        if (root instanceof List<?> list) {
            if (list.size() != 1) {
                throw BlobErrorCreator.clientError("the queue message carries " + list.size()
                        + " events; one event per message is expected", null);
            }
            root = list.get(0);
        }
        if (!(root instanceof Map<?, ?> event)) {
            throw BlobErrorCreator.clientError("the queue message is not a JSON storage event", null);
        }
        // CloudEvents names the type `type` and the time `time`; the Event Grid schema uses
        // `eventType` and `eventTime`.
        boolean cloudEvents = event.containsKey("specversion");
        String eventType = string(event, cloudEvents ? "type" : "eventType");
        if (eventType == null) {
            throw BlobErrorCreator.clientError("the queue message names no event type", null);
        }
        boolean created = BLOB_CREATED.equals(eventType);
        boolean deleted = BLOB_DELETED.equals(eventType);
        if (!created && !deleted) {
            return new Parsed(eventType, false, null, null, null, null, null, null, null, null, null, null);
        }
        String subject = string(event, "subject");
        Matcher matcher = subject == null ? null : SUBJECT.matcher(subject);
        if (matcher == null || !matcher.matches()) {
            throw BlobErrorCreator.clientError("the event subject does not name a blob: " + subject, null);
        }
        String time = string(event, cloudEvents ? "time" : "eventTime");
        if (time == null) {
            throw BlobErrorCreator.clientError("the event carries no time", null);
        }
        OffsetDateTime eventTime;
        try {
            eventTime = OffsetDateTime.parse(time);
        } catch (DateTimeParseException e) {
            throw BlobErrorCreator.clientError("the event time is not an ISO 8601 timestamp: " + time, e);
        }
        Map<?, ?> data = event.get("data") instanceof Map<?, ?> map ? map : Map.of();
        String url = string(data, "url");
        String sequencer = string(data, "sequencer");
        if (url == null || sequencer == null) {
            throw BlobErrorCreator.clientError("the event data carries no url or sequencer", null);
        }
        Object length = data.get("contentLength");
        return new Parsed(eventType, deleted, matcher.group(1), matcher.group(2), url, eventTime,
                string(data, "api") == null ? "" : string(data, "api"), string(data, "contentType"),
                length instanceof Number number ? number.longValue() : null,
                string(data, "blobType"), string(data, "eTag"), sequencer);
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
