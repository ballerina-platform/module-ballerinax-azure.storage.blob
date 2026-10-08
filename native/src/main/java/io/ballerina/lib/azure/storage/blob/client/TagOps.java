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

import com.azure.core.util.Context;
import com.azure.storage.blob.models.TaggedBlobItem;
import com.azure.storage.blob.options.BlobSetTagsOptions;
import com.azure.storage.blob.options.FindBlobsOptions;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Native implementations of the blob index tag operations. Tags and the tag query are
 * validated locally before any request, against the service's documented limits and grammar.
 */
public final class TagOps {

    // Key under which the native iterator state is stored on a stream generator object.
    private static final String NATIVE_ITERATOR = "taggedBlobIterator";

    // The service's tag limits: at most 10 tags, keys of 1–128 and values of 0–256 characters,
    // both drawn from one character set.
    private static final int MAX_TAGS = 10;
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_VALUE_LENGTH = 256;
    private static final Pattern TAG_CHARACTERS = Pattern.compile("[A-Za-z0-9 +\\-.:=_/]*");
    // The comparison operators the tag query grammar accepts.
    private static final String[] OPERATORS = {">=", "<=", "=", ">", "<"};
    private static final String AND = "AND";

    private TagOps() {
    }

    /** Replaces a blob's index tags. */
    public static Object setTags(Environment env, BObject self, BString path, BMap<BString, BString> tags,
                                 Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            Map<String, String> values = ValueUtils.toStringMap(tags);
            validateTags(values);
            BlobOps.blobClient(self, path).setTagsWithResponse(new BlobSetTagsOptions(values)
                    .setRequestConditions(OptionsReader.leaseConditions(options)), null, null);
            return null;
        });
    }

    /** Reads a blob's index tags. */
    public static Object getTags(Environment env, BObject self, BString path) {
        return BallerinaAzureClient.invoke(env,
                () -> ValueUtils.toBStringMap(BlobOps.blobClient(self, path).getTags()));
    }

    /**
     * Attaches a fresh tag-query iterator to the Ballerina stream generator object. The query
     * is validated locally and scoped to the bound container before the request.
     */
    public static Object newTaggedBlobIterator(Environment env, BObject self, BObject generator, BString query) {
        return BallerinaAzureClient.invoke(env, () -> {
            String scoped = "@container = '" + BallerinaAzureClient.getContainerClient(self).getBlobContainerName()
                    + "' AND " + validateQuery(query.getValue());
            generator.addNativeData(NATIVE_ITERATOR, BallerinaAzureClient.getServiceClient(self)
                    .findBlobsByTags(new FindBlobsOptions(scoped), null, Context.NONE).iterator());
            return null;
        });
    }

    /** Pulls the next match: a {@code TaggedBlobEntry} record, {@code null} at the end, or an error. */
    public static Object nextTaggedBlobEntry(Environment env, BObject generator) {
        return BallerinaAzureClient.invoke(env, () -> {
            @SuppressWarnings("unchecked")
            Iterator<TaggedBlobItem> iterator = (Iterator<TaggedBlobItem>) generator.getNativeData(NATIVE_ITERATOR);
            if (iterator == null || !iterator.hasNext()) {
                return null;
            }
            return RecordMapper.taggedBlobEntry(iterator.next());
        });
    }

    /** Stops an in-progress tag query early. */
    public static Object closeTaggedBlobIterator(BObject generator) {
        generator.addNativeData(NATIVE_ITERATOR, null);
        return null;
    }

    static void validateTags(Map<String, String> tags) {
        if (tags.size() > MAX_TAGS) {
            throw BlobErrorCreator.clientError("a blob carries at most " + MAX_TAGS + " tags", null);
        }
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            String key = tag.getKey();
            if (key.isEmpty() || key.length() > MAX_KEY_LENGTH || !TAG_CHARACTERS.matcher(key).matches()) {
                throw BlobErrorCreator.clientError("invalid tag key '" + key + "': 1 to " + MAX_KEY_LENGTH
                        + " characters from letters, digits, space, and + - . : = _ /", null);
            }
            String value = tag.getValue();
            if (value.length() > MAX_VALUE_LENGTH || !TAG_CHARACTERS.matcher(value).matches()) {
                throw BlobErrorCreator.clientError("invalid value for tag '" + key + "': up to " + MAX_VALUE_LENGTH
                        + " characters from letters, digits, space, and + - . : = _ /", null);
            }
        }
    }

    /*
     * Checks the query against the service grammar before any request: one or more
     * `"key" op 'value'` comparisons joined by AND, where op is =, >, >=, <, or <=. The bound
     * container is added by the connector, so a caller-written @container is refused.
     */
    static String validateQuery(String query) {
        String trimmed = query.strip();
        if (trimmed.startsWith("@")) {
            throw BlobErrorCreator.clientError(
                    "the query must not name @container; it is scoped to the bound container", null);
        }
        if (trimmed.isEmpty()) {
            throw BlobErrorCreator.clientError("the tag query must not be empty", null);
        }
        int position = 0;
        while (true) {
            position = comparison(trimmed, position);
            position = skipSpaces(trimmed, position);
            if (position == trimmed.length()) {
                return trimmed;
            }
            if (!trimmed.regionMatches(true, position, AND, 0, AND.length())) {
                throw invalidQuery("expected AND at position " + position);
            }
            position = skipSpaces(trimmed, position + AND.length());
        }
    }

    // Consumes one `"key" op 'value'` comparison and returns the position after it.
    private static int comparison(String query, int start) {
        int position = skipSpaces(query, start);
        int keyEnd = quoted(query, position, '"', "tag key");
        String key = query.substring(position + 1, keyEnd);
        if (key.isEmpty() || key.length() > MAX_KEY_LENGTH || !TAG_CHARACTERS.matcher(key).matches()) {
            throw invalidQuery("invalid tag key \"" + key + "\"");
        }
        position = skipSpaces(query, keyEnd + 1);
        String operator = null;
        for (String candidate : OPERATORS) {
            if (query.startsWith(candidate, position)) {
                operator = candidate;
                break;
            }
        }
        if (operator == null) {
            throw invalidQuery("expected one of = > >= < <= after the key at position " + position);
        }
        position = skipSpaces(query, position + operator.length());
        int valueEnd = quoted(query, position, '\'', "tag value");
        String value = query.substring(position + 1, valueEnd);
        if (value.length() > MAX_VALUE_LENGTH || !TAG_CHARACTERS.matcher(value).matches()) {
            throw invalidQuery("invalid tag value '" + value + "'");
        }
        return valueEnd + 1;
    }

    // Returns the index of the closing quote of a quoted token starting at `position`.
    private static int quoted(String query, int position, char quote, String what) {
        if (position >= query.length() || query.charAt(position) != quote) {
            throw invalidQuery("expected a " + quote + "-quoted " + what + " at position " + position);
        }
        int end = query.indexOf(quote, position + 1);
        if (end < 0) {
            throw invalidQuery("unterminated " + what + " at position " + position);
        }
        return end;
    }

    private static int skipSpaces(String query, int position) {
        int current = position;
        while (current < query.length() && query.charAt(current) == ' ') {
            current++;
        }
        return current;
    }

    private static RuntimeException invalidQuery(String detail) {
        return BlobErrorCreator.clientError("invalid tag query: " + detail
                + ". Expected \"key\" op 'value' comparisons joined by " + AND.toUpperCase(Locale.ROOT), null);
    }
}
