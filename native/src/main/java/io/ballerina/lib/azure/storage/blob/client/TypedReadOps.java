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

import com.azure.storage.blob.BlobClient;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ContentBinder;
import io.ballerina.lib.azure.storage.blob.util.DataBindingOptions;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * The {@code getBlob} content retrieval: downloads a blob and hands it back in the form the
 * caller-directed target type selects (raw bytes, text, a JSON or XML value, a record shape
 * bound per the resolved format, a lazy byte stream, or a lazy stream of CSV-bound records).
 * Binding is strict and runs through the data.jsondata/xmldata/csv modules.
 */
public final class TypedReadOps {

    // The Caller's private Client field.
    private static final BString CALLER_CLIENT_FIELD = StringUtils.fromString("client");
    // The generator class backing the lazy byte stream, declared in natives.bal.
    private static final String CONTENT_STREAM_GENERATOR_CLASS = "ContentStreamGenerator";

    private static final String FORMAT_JSON = "JSON";
    private static final String FORMAT_XML = "XML";
    private static final String FORMAT_CSV = "CSV";

    private static final String JSON_BIND_CONTEXT = "the blob content does not bind to the target JSON type";
    private static final String XML_BIND_CONTEXT = "the blob content does not bind to the target XML type";
    private static final String XML_PARSE_CONTEXT = "the blob content is not valid XML";
    private static final String CSV_BIND_CONTEXT = "the blob content does not bind to the target CSV type";

    private TypedReadOps() {
    }

    /** Retrieves the blob's content in the form the target typedesc selects. */
    public static Object getBlob(Environment env, BObject self, BString path, Object options, BTypedesc targetType) {
        // A Caller carries no native client data; unwrap it to the Client it holds.
        BObject client = self.getNativeData(BallerinaAzureClient.NATIVE_CONTAINER_CLIENT) == null
                ? self.getObjectValue(CALLER_CLIENT_FIELD) : self;
        return getBlobFrom(env, client, path, options, targetType);
    }

    private static Object getBlobFrom(Environment env, BObject self, BString path, Object options,
                                      BTypedesc targetType) {
        // The declared type drives the binding, so a readonly intersection yields a readonly
        // value; the implied type (references and intersections unwrapped) drives the routing.
        Type described = TypeUtils.getReferredType(targetType.getDescribingType());
        Type implied = TypeUtils.getImpliedType(described);
        if (implied.getTag() == TypeTags.STREAM_TAG) {
            return streamTarget(env, self, path, options, (StreamType) implied);
        }
        Object bytes = readBlobBytes(env, self, path, options);
        if (bytes instanceof BError) {
            return bytes;
        }
        return bindMaterialized(env, (BArray) bytes, described, implied, path, options);
    }

    // Binds materialized content to a non-stream target.
    private static Object bindMaterialized(Environment env, BArray byteArray, Type described, Type implied,
                                           BString path, Object options) {
        switch (implied.getTag()) {
            case TypeTags.STRING_TAG:
                return decodeText(byteArray);
            case TypeTags.ARRAY_TAG:
                return bindArrayTarget(env, byteArray, described, (ArrayType) implied, path, options);
            case TypeTags.RECORD_TYPE_TAG:
            case TypeTags.MAP_TAG:
                return bindRecordTarget(byteArray, described, path, options);
            case TypeTags.UNION_TAG:
                // A union target binds through the JSON parser; a format that resolves to XML
                // or CSV has no union binding.
                String format = resolveFormat(path, options);
                if (format != null && !FORMAT_JSON.equals(format)) {
                    return BlobErrorCreator.clientError("a union target binds from JSON only; use a single "
                            + "record or record array target for " + format + " content", null);
                }
                return bindJson(byteArray, described);
            default:
                if (TypeTags.isXMLTypeTag(implied.getTag())) {
                    return bindXml(byteArray, described);
                }
                return bindJson(byteArray, described);
        }
    }

    // byte[] is the raw content; a record or map array binds per the resolved format (a
    // JSON array or CSV rows; XML has no top-level array). Any other array is a json-shaped
    // target: it binds through the JSON parser, except that CSV content never binds to it.
    private static Object bindArrayTarget(Environment env, BArray byteArray, Type described, ArrayType arrayType,
                                          BString path, Object options) {
        Type element = TypeUtils.getImpliedType(arrayType.getElementType());
        if (element.getTag() == TypeTags.BYTE_TAG) {
            return byteArray;
        }
        String format = resolveFormat(path, options);
        if (element.getTag() != TypeTags.RECORD_TYPE_TAG && element.getTag() != TypeTags.MAP_TAG) {
            if (FORMAT_CSV.equals(format)) {
                return BlobErrorCreator.clientError(
                        "CSV content binds to a record array target; read the content as string "
                                + "or byte[] and bind rows with the data.csv module", null);
            }
            return bindJson(byteArray, described);
        }
        if (format == null) {
            return unresolvableFormat("record array");
        }
        return switch (format) {
            case FORMAT_JSON -> bindJson(byteArray, described);
            case FORMAT_CSV -> parseCsv(env, byteArray, described);
            default -> BlobErrorCreator.clientError(
                    "a record array target does not bind from XML; use a '.json' or '.csv' source, "
                            + "or an explicit fileFormat", null);
        };
    }

    // A single record (or map) binds per the resolved format; a record is never CSV.
    private static Object bindRecordTarget(BArray byteArray, Type described, BString path, Object options) {
        String format = resolveFormat(path, options);
        if (format == null) {
            return unresolvableFormat("record");
        }
        return switch (format) {
            case FORMAT_JSON -> bindJson(byteArray, described);
            case FORMAT_XML -> bindXml(byteArray, described);
            default -> BlobErrorCreator.clientError(
                    "a record target does not bind from CSV; use a record array target for CSV rows", null);
        };
    }

    // A stream target reads lazily: byte streams pull chunks from the open content stream,
    // record streams bind CSV rows through data.csv's own stream parser.
    private static Object streamTarget(Environment env, BObject self, BString path, Object options,
                                       StreamType streamType) {
        BObject generator = ValueCreator.createObjectValue(ModuleUtils.getModule(), CONTENT_STREAM_GENERATOR_CLASS);
        Object opened = TransferOps.openContentStream(env, self, generator, path, options);
        if (opened instanceof BError) {
            return opened;
        }
        Type constraint = TypeUtils.getReferredType(streamType.getConstrainedType());
        Type impliedConstraint = TypeUtils.getImpliedType(constraint);
        boolean byteStream = impliedConstraint.getTag() == TypeTags.ARRAY_TAG
                && TypeUtils.getImpliedType(((ArrayType) impliedConstraint).getElementType()).getTag()
                        == TypeTags.BYTE_TAG;
        if (byteStream) {
            // The stream is typed with the DECLARED constraint and completion, so a target
            // narrowed to the module's Error completion casts cleanly.
            return ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(constraint, streamType.getCompletionType()), generator);
        }
        if (!FORMAT_CSV.equals(resolveFormat(path, options))) {
            TransferOps.closeQuietly(generator);
            return BlobErrorCreator.clientError(
                    "a record stream target reads CSV rows; use a '.csv' source or an explicit fileFormat", null);
        }
        BStream byteFeed = ValueCreator.createStreamValue(TypeCreator.createStreamType(
                TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE), completionType()), generator);
        Object rows = io.ballerina.lib.data.csvdata.csv.Native.parseToStream(env, byteFeed,
                DataBindingOptions.csvParseOptions(false),
                ValueCreator.createTypedescValue(constraint));
        if (rows instanceof BError bError) {
            // The generator already holds an open source stream; release it on a parse failure.
            TransferOps.closeQuietly(generator);
            return csvFailure(bError);
        }
        return rows;
    }

    // Downloads the blob's full content (or a range of it) into a Ballerina byte array.
    private static Object readBlobBytes(Environment env, BObject self, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobClient client = BlobOps.blobClient(self, path, OptionsReader.snapshotId(options));
            BMap<BString, Object> record = OptionsReader.record(options);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            client.downloadStreamWithResponse(out,
                    record == null ? null : OptionsReader.range(record.get(OptionsReader.RANGE)),
                    null, null, false, null, null);
            return ValueCreator.createArrayValue(out.toByteArray());
        });
    }

    private static Object bindJson(BArray content, Type target) {
        try {
            return ContentBinder.bindJson(content, target, false, JSON_BIND_CONTEXT);
        } catch (BError e) {
            return e;
        }
    }

    private static Object bindXml(BArray content, Type target) {
        try {
            return ContentBinder.bindXml(content, target, false, XML_BIND_CONTEXT, XML_PARSE_CONTEXT);
        } catch (BError e) {
            return e;
        }
    }

    private static Object parseCsv(Environment env, BArray byteArray, Type target) {
        try {
            Object result = io.ballerina.lib.data.csvdata.csv.Native.parseBytes(env, byteArray,
                    DataBindingOptions.csvParseOptions(false),
                    ValueCreator.createTypedescValue(target));
            return result instanceof BError bError ? csvFailure(bError) : result;
        } catch (BError e) {
            return csvFailure(e);
        }
    }

    private static Object decodeText(BArray byteArray) {
        try {
            String text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(byteArray.getBytes())).toString();
            return StringUtils.fromString(text);
        } catch (CharacterCodingException e) {
            return BlobErrorCreator.clientError(
                    "the blob content is not valid UTF-8 text: " + BallerinaAzureClient.describe(e), e);
        }
    }

    // Resolves the binding format of a record-shaped target: the explicit fileFormat
    // override wins, else the path's extension decides; null means unresolvable.
    private static String resolveFormat(BString path, Object options) {
        BMap<BString, Object> record = OptionsReader.record(options);
        if (record != null) {
            BString override = record.getStringValue(OptionsReader.FILE_FORMAT);
            if (override != null) {
                return override.getValue();
            }
        }
        String lower = path.getValue().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".json")) {
            return FORMAT_JSON;
        }
        if (lower.endsWith(".xml")) {
            return FORMAT_XML;
        }
        if (lower.endsWith(".csv")) {
            return FORMAT_CSV;
        }
        return null;
    }

    private static Type completionType() {
        return TypeCreator.createUnionType(PredefinedTypes.TYPE_ERROR, PredefinedTypes.TYPE_NULL);
    }

    private static BError unresolvableFormat(String targetKind) {
        return BlobErrorCreator.clientError("a " + targetKind + " target requires a '.json', '.xml', or "
                + "'.csv' extension in the path, or an explicit fileFormat", null);
    }

    private static BError csvFailure(BError cause) {
        return BlobErrorCreator.clientError(CSV_BIND_CONTEXT + ": " + cause.getErrorMessage(), cause);
    }
}
