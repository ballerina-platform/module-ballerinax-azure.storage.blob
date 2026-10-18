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

import io.ballerina.lib.azure.storage.blob.server.ServiceContext.HandlerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Routes a created event to a handler, and words the never-ours warnings: the first event of
 * each kind a queue carries that no handler takes logs a warning naming the subscription filter
 * that keeps such events out, and repeats log at debug level with a running count.
 */
final class Routing {

    static final Map<String, String> EXTENSION_HANDLERS = Map.of(
            "txt", Listener.ON_BLOB_TEXT, "json", Listener.ON_BLOB_JSON,
            "xml", Listener.ON_BLOB_XML, "csv", Listener.ON_BLOB_CSV);
    static final String UNROUTED_KEY = "unrouted";
    private static final String TYPE_KEY = "type:";
    private static final String API_KEY = "api:";
    private static final String BLOB_TYPE_KEY = "blobType:";

    private Routing() {
    }

    /**
     * Resolves a created event's handler: a routing pattern wins, in the fixed handler order;
     * then the name's extension; then the content type when the name has no extension; then
     * {@code onBlob}. Null when the service has no handler for the event.
     */
    static HandlerConfig resolve(Map<String, HandlerConfig> handlers, String blobName, String contentType) {
        String base = baseContentType(contentType);
        for (String methodName : Listener.ROUTING_ORDER) {
            HandlerConfig handler = handlers.get(methodName);
            if (handler == null) {
                continue;
            }
            if (handler.namePattern() != null && handler.namePattern().matcher(blobName).matches()) {
                return handler;
            }
            if (handler.contentTypePattern() != null && base != null
                    && handler.contentTypePattern().matcher(base).matches()) {
                return handler;
            }
        }
        String extension = extension(blobName);
        String mapped = extension.isEmpty() ? contentTypeHandler(base) : EXTENSION_HANDLERS.get(extension);
        if (mapped != null && handlers.containsKey(mapped)) {
            return handlers.get(mapped);
        }
        return handlers.get(Listener.ON_BLOB);
    }

    /** The name's extension, lower-cased, or the empty string. */
    static String extension(String blobName) {
        int dot = blobName.lastIndexOf('.');
        return dot < 0 ? "" : blobName.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    static String contentTypeHandler(String baseContentType) {
        if (baseContentType == null) {
            return null;
        }
        return switch (baseContentType) {
            case "application/json" -> Listener.ON_BLOB_JSON;
            case "application/xml", "text/xml" -> Listener.ON_BLOB_XML;
            case "text/csv" -> Listener.ON_BLOB_CSV;
            default -> baseContentType.startsWith("text/") ? Listener.ON_BLOB_TEXT : null;
        };
    }

    /** The media type without its parameters, lower-cased. */
    static String baseContentType(String contentType) {
        if (contentType == null) {
            return null;
        }
        int semicolon = contentType.indexOf(';');
        String base = semicolon < 0 ? contentType : contentType.substring(0, semicolon);
        return base.strip().toLowerCase(Locale.ROOT);
    }

    // ---- never-ours warnings

    static String typeKey(String eventType) {
        return TYPE_KEY + eventType;
    }

    static String noContentKey(EventParser.Parsed event) {
        return event.api() != null && EventParser.NO_CONTENT_APIS.contains(event.api())
                ? API_KEY + event.api() : BLOB_TYPE_KEY + event.blobType();
    }

    /** The warning for an event type the listener does not dispatch. */
    static String typeWarning(String queueName, String eventType) {
        return "queue " + queueName + " received a " + eventType + " event, which this listener does not dispatch; "
                + "acknowledged. Restrict the subscription's includedEventTypes to Microsoft.Storage.BlobCreated and "
                + "Microsoft.Storage.BlobDeleted";
    }

    /** The warning for a deleted event on a service without {@code onBlobDeleted}. */
    static String deletedWarning(String queueName) {
        return "queue " + queueName + " received a Microsoft.Storage.BlobDeleted event, which its service has no "
                + "onBlobDeleted for; acknowledged. Remove Microsoft.Storage.BlobDeleted from the subscription's "
                + "includedEventTypes";
    }

    /** The warning for a created event that commits no content. */
    static String noContentWarning(String queueName, EventParser.Parsed event) {
        if (event.api() != null && EventParser.NO_CONTENT_APIS.contains(event.api())) {
            return "queue " + queueName + " received a created event from " + event.api() + ", an operation that "
                    + "commits no content; acknowledged. Add the advanced filter StringNotIn on data.api with "
                    + "CreateFile, SftpCreate, and SftpWrite to the subscription";
        }
        return "queue " + queueName + " received the created event of a " + event.blobType() + ", which carries no "
                + "written content; acknowledged. Add the advanced filter StringNotIn on data.blobType with "
                + "AppendBlob and PageBlob to the subscription; it also drops the copies and deletes of such blobs";
    }

    /** The warning for a created event no handler of the service takes. */
    static String unroutedWarning(String queueName, String path, Map<String, HandlerConfig> handlers) {
        String extension = extension(path.substring(path.lastIndexOf('/') + 1));
        List<String> handled = new ArrayList<>();
        for (Map.Entry<String, String> entry : List.of(Map.entry("txt", Listener.ON_BLOB_TEXT),
                Map.entry("json", Listener.ON_BLOB_JSON), Map.entry("xml", Listener.ON_BLOB_XML),
                Map.entry("csv", Listener.ON_BLOB_CSV))) {
            if (handlers.containsKey(entry.getValue())) {
                handled.add("." + entry.getKey());
            }
        }
        String what = extension.isEmpty() ? "a blob without an extension" : "a ." + extension + " blob";
        String keepOut = extension.isEmpty() ? "" : " Add the advanced filter StringNotEndsWith on subject with '."
                + extension + "',";
        String keepIn = handled.size() == 1 ? " or subjectEndsWith with '" + handled.get(0) + "'"
                : " or the advanced filter StringEndsWith on subject with " + String.join(", ", handled);
        return "queue " + queueName + " received the created event of " + what + " (" + path + "), which no handler of "
                + "its service takes and it has no onBlob; acknowledged." + keepOut + keepIn + " to the subscription";
    }
}
