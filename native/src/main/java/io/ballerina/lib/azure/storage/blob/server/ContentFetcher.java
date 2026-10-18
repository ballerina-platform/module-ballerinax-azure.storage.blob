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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import io.ballerina.lib.azure.storage.blob.client.TransferOps;
import io.ballerina.lib.azure.storage.blob.server.ServiceContext.HandlerConfig;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.ContentBinder;
import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Fetches a created event's blob through the event's {@code Caller} and binds it to the
 * handler's declared type. A failure is classified by its cause for the dispatcher to dispose
 * of the message: the blob not found, archived, content that does not bind, or anything else.
 * Whether a blob is empty is decided by the fetched content.
 */
final class ContentFetcher {

    enum Disposition { CONTENT, NOT_FOUND, ARCHIVED, BINDING, ORDINARY }

    // The fetched content, or the failure; a stream result keeps its generator (and the CSV row
    // stream over it) so the dispatcher can tell a transport failure from a binding error.
    record Fetch(Disposition disposition, Object content, BError error, BObject streamGenerator, BObject csvStream) {

        static Fetch content(Object content) {
            return new Fetch(Disposition.CONTENT, content, null, null, null);
        }

        static Fetch stream(Object content, BObject generator, BObject csvStream) {
            return new Fetch(Disposition.CONTENT, content, null, generator, csvStream);
        }

        static Fetch failure(Disposition disposition, BError error) {
            return new Fetch(disposition, null, error, null, null);
        }
    }

    private static final BString CALLER_CLIENT_FIELD = StringUtils.fromString("client");
    private static final BString ERROR_CODE = StringUtils.fromString("errorCode");
    private static final String BIND_CSV_CONTENT_FUNCTION = "bindCsvContent";
    private static final String NEW_CONTENT_CSV_STREAM_FUNCTION = "newContentCsvStream";
    private static final String CONTENT_STREAM_GENERATOR_CLASS = "ContentStreamGenerator";
    private static final String NOT_FOUND_ERROR = "NotFoundError";
    private static final String BLOB_NOT_FOUND = "BlobNotFound";
    private static final String ARCHIVED_BLOB_ERROR = "ArchivedBlobError";
    private static final String JSON_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_JSON + "' handler's declared type";
    private static final String XML_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_XML + "' handler's declared type";
    private static final String XML_PARSE_CONTEXT =
            "content is not valid XML for the '" + Listener.ON_BLOB_XML + "' handler";
    private static final String CSV_BIND_CONTEXT =
            "content does not bind to the '" + Listener.ON_BLOB_CSV + "' handler's declared type";

    private ContentFetcher() {
    }

    static Fetch fetch(ListenerContext ctx, BObject caller, HandlerConfig handler, EventParser.Parsed event) {
        BlobClient blob = BallerinaAzureClient.getContainerClient(caller.getObjectValue(CALLER_CLIENT_FIELD))
                .getBlobClient(event.path());
        Type contentType = TypeUtils.getReferredType(handler.contentType());
        if (contentType instanceof StreamType streamType) {
            return fetchStream(ctx, blob, handler, event, streamType);
        }
        byte[] bytes;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.downloadStream(out);
            bytes = out.toByteArray();
        } catch (RuntimeException e) {
            return serviceFailure(e);
        }
        boolean typed = !Listener.ON_BLOB.equals(handler.methodName())
                && !Listener.ON_BLOB_TEXT.equals(handler.methodName());
        if (typed && bytes.length == 0) {
            return Fetch.failure(Disposition.BINDING, BlobErrorCreator.contentBindingError("the blob is empty: no "
                    + "content binds to the '" + handler.methodName() + "' handler's declared type", null,
                    event.containerName(), event.path(), bytes));
        }
        try {
            return Fetch.content(bind(ctx, handler, bytes));
        } catch (BError e) {
            return Fetch.failure(Disposition.BINDING, BlobErrorCreator.contentBindingError(
                    e.getErrorMessage().getValue(), e, event.containerName(), event.path(), bytes));
        }
    }

    // The stream forms: the blob's ranged byte stream, wrapped in the CSV row stream for onBlobCsv.
    private static Fetch fetchStream(ListenerContext ctx, BlobClient blob, HandlerConfig handler,
                                     EventParser.Parsed event, StreamType streamType) {
        BlobInputStream stream;
        try {
            stream = blob.openInputStream(new BlobInputStreamOptions());
        } catch (RuntimeException e) {
            return serviceFailure(e);
        }
        BObject generator = ValueCreator.createObjectValue(ModuleUtils.getModule(), CONTENT_STREAM_GENERATOR_CLASS);
        TransferOps.attachContentStream(generator, stream);
        Type constraint = TypeUtils.getReferredType(streamType.getConstrainedType());
        if (!Listener.ON_BLOB_CSV.equals(handler.methodName())) {
            return Fetch.stream(ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(constraint, streamType.getCompletionType()), generator),
                    generator, null);
        }
        BStream byteFeed = ValueCreator.createStreamValue(TypeCreator.createStreamType(
                TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE),
                TypeCreator.createUnionType(PredefinedTypes.TYPE_ERROR, PredefinedTypes.TYPE_NULL)), generator);
        Object rows = ctx.runtime.callFunction(ModuleUtils.getModule(), NEW_CONTENT_CSV_STREAM_FUNCTION,
                new StrandMetadata(true, null), ValueCreator.createTypedescValue(constraint), byteFeed,
                ctx.laxDataBinding);
        if (rows instanceof BError e) {
            TransferOps.closeQuietly(generator);
            return Fetch.failure(Disposition.BINDING, BlobErrorCreator.contentBindingError(
                    CSV_BIND_CONTEXT + ": " + e.getErrorMessage(), e, event.containerName(), event.path(), null));
        }
        return Fetch.stream(ValueCreator.createStreamValue(
                TypeCreator.createStreamType(constraint, streamType.getCompletionType()), (BObject) rows),
                generator, (BObject) rows);
    }

    // A service failure's disposition: BlobNotFound, an archived blob, or an ordinary failure.
    private static Fetch serviceFailure(RuntimeException e) {
        BError mapped = BallerinaAzureClient.mapFailure(e);
        String type = mapped.getType().getName();
        if (NOT_FOUND_ERROR.equals(type) && BLOB_NOT_FOUND.equals(errorCode(mapped))) {
            return Fetch.failure(Disposition.NOT_FOUND, mapped);
        }
        if (ARCHIVED_BLOB_ERROR.equals(type)) {
            return Fetch.failure(Disposition.ARCHIVED, mapped);
        }
        return Fetch.failure(Disposition.ORDINARY, mapped);
    }

    private static String errorCode(BError error) {
        if (error.getDetails() instanceof BMap<?, ?> detail) {
            Object code = detail.get(ERROR_CODE);
            return code == null ? null : code.toString();
        }
        return null;
    }

    private static Object bind(ListenerContext ctx, HandlerConfig handler, byte[] bytes) {
        return switch (handler.methodName()) {
            case Listener.ON_BLOB_TEXT -> StringUtils.fromString(new String(bytes, StandardCharsets.UTF_8));
            case Listener.ON_BLOB_JSON -> ContentBinder.bindJson(ValueCreator.createArrayValue(bytes),
                    handler.contentType(), ctx.laxDataBinding, JSON_BIND_CONTEXT);
            case Listener.ON_BLOB_XML -> ContentBinder.bindXml(ValueCreator.createArrayValue(bytes),
                    handler.contentType(), ctx.laxDataBinding, XML_BIND_CONTEXT, XML_PARSE_CONTEXT);
            case Listener.ON_BLOB_CSV -> bindCsv(ctx, handler, bytes);
            default -> ValueCreator.createArrayValue(bytes);
        };
    }

    // CSV binds on a Ballerina strand, because the data.csv parser needs one.
    private static Object bindCsv(ListenerContext ctx, HandlerConfig handler, byte[] bytes) {
        Object result = ctx.runtime.callFunction(ModuleUtils.getModule(), BIND_CSV_CONTENT_FUNCTION,
                new StrandMetadata(true, null), ValueCreator.createArrayValue(bytes),
                ValueCreator.createTypedescValue(TypeUtils.getReferredType(handler.contentType())),
                ctx.laxDataBinding);
        if (result instanceof BError e) {
            throw BlobErrorCreator.clientError(CSV_BIND_CONTEXT + ": " + e.getErrorMessage(), e);
        }
        return result;
    }
}
