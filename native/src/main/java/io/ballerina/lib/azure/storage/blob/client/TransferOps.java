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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobDownloadToFileOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Native implementations of the {@code Client} transfer operations: local-file upload and
 * download, in-memory content upload, the staged-block stream upload, and the lazy content
 * stream behind {@code getBlob}.
 */
public final class TransferOps {

    // Key under which an open content input stream is stored on a stream generator object.
    private static final String NATIVE_INPUT_STREAM = "inputStream";
    /** The chunk size handed to Ballerina byte-stream consumers. */
    private static final int READ_CHUNK_BYTES = 64 * 1024;
    // Media types applied when the connector itself serialized the content.
    private static final String FORMAT_JSON = "JSON";
    private static final String FORMAT_XML = "XML";
    private static final String MEDIA_TYPE_JSON = "application/json";
    private static final String MEDIA_TYPE_XML = "application/xml";
    private static final String MEDIA_TYPE_CSV = "text/csv";

    private TransferOps() {
    }

    /** Uploads a local file as a block blob, replacing any existing blob at the path. */
    public static Object uploadFromFile(Environment env, BObject self, BString sourcePath,
                                        BString destinationPath, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            Path localPath = Path.of(sourcePath.getValue());
            if (!Files.isRegularFile(localPath)) {
                throw BlobErrorCreator.clientError("local file not found: " + sourcePath.getValue(), null);
            }
            BlobUploadFromFileOptions sdkOptions = new BlobUploadFromFileOptions(localPath.toString())
                    .setHeaders(OptionsReader.contentHeadersOf(options))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setTier(OptionsReader.accessTier(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options));
            // The options form carries no if-none-match, so an existing blob is replaced.
            BlobOps.blobClient(self, destinationPath).uploadFromFileWithResponse(sdkOptions, null, null);
            return null;
        });
    }

    /**
     * Uploads in-memory content (bytes, text, or an XML document) as a block blob. Record content
     * never reaches this call: it is serialized on the Ballerina side first, and the format it
     * was serialized in arrives as {@code appliedFormat} so the content type can be set.
     */
    public static Object upload(Environment env, BObject self, Object content, BString destinationPath,
                                Object options, Object appliedFormat) {
        return BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = contentBytes(content);
            BlobParallelUploadOptions sdkOptions = new BlobParallelUploadOptions(BinaryData.fromBytes(bytes))
                    .setHeaders(headersWithAutoContentType(options, appliedFormat))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setTier(OptionsReader.accessTier(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options));
            BlobOps.blobClient(self, destinationPath).uploadWithResponse(sdkOptions, null, null);
            return null;
        });
    }

    /**
     * Stages one chunk of a stream upload as an uncommitted block. Until the commit no blob
     * exists at the path, and an abandoned upload leaves only blocks the service expires.
     */
    public static Object stageStreamBlock(Environment env, BObject self, BString destinationPath, BString uploadId,
                                          long index, BArray chunk, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = chunk.getBytes();
            BlobOps.blobClient(self, destinationPath).getBlockBlobClient().stageBlockWithResponse(
                    blockId(uploadId.getValue(), index),
                    new ByteArrayInputStream(bytes), bytes.length, null, OptionsReader.leaseId(options), null, null);
            return null;
        });
    }

    /** Commits the staged blocks of a stream upload, creating (or replacing) the blob. */
    public static Object commitStreamBlocks(Environment env, BObject self, BString destinationPath, BString uploadId,
                                            long blockCount, Object options, Object appliedFormat) {
        return BallerinaAzureClient.invoke(env, () -> {
            List<String> blockIds = new ArrayList<>();
            for (long i = 0; i < blockCount; i++) {
                blockIds.add(blockId(uploadId.getValue(), i));
            }
            BlockBlobCommitBlockListOptions sdkOptions = new BlockBlobCommitBlockListOptions(blockIds)
                    .setHeaders(headersWithAutoContentType(options, appliedFormat))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setTier(OptionsReader.accessTier(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options));
            BlobOps.blobClient(self, destinationPath).getBlockBlobClient()
                    .commitBlockListWithResponse(sdkOptions, null, null);
            return null;
        });
    }

    /** Downloads a blob (or a range of it) to a new local file. */
    public static Object download(Environment env, BObject self, BString sourcePath,
                                  BString destinationPath, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            Path localPath = Path.of(destinationPath.getValue());
            if (Files.exists(localPath)) {
                throw BlobErrorCreator.clientError("local file already exists: " + destinationPath.getValue(), null);
            }
            BlobClient client = BlobOps.blobClient(self, sourcePath, OptionsReader.snapshotId(options));
            try {
                client.downloadToFileWithResponse(new BlobDownloadToFileOptions(localPath.toString())
                        .setRange(range(options)), null, null);
            } catch (UncheckedIOException e) {
                throw BlobErrorCreator.clientError("cannot write local file " + destinationPath.getValue() + ": "
                        + BallerinaAzureClient.describe(e.getCause()), e);
            }
            return null;
        });
    }

    /** Opens the blob's content stream and stores it on the Ballerina stream generator object. */
    public static Object openContentStream(Environment env, BObject self, BObject generator,
                                           BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobClient client = BlobOps.blobClient(self, path, OptionsReader.snapshotId(options));
            BlobInputStream stream = client.openInputStream(new BlobInputStreamOptions().setRange(range(options)));
            generator.addNativeData(NATIVE_INPUT_STREAM, stream);
            return null;
        });
    }

    /** Reads the next chunk from an open content stream; {@code null} signals the end. */
    public static Object nextContentChunk(Environment env, BObject generator) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobInputStream stream = (BlobInputStream) generator.getNativeData(NATIVE_INPUT_STREAM);
            if (stream == null) {
                return null;
            }
            try {
                byte[] buffer = new byte[READ_CHUNK_BYTES];
                int read = stream.read(buffer);
                if (read < 0) {
                    closeQuietly(generator);
                    return null;
                }
                byte[] chunk = read == buffer.length ? buffer : Arrays.copyOf(buffer, read);
                return ValueCreator.createArrayValue(chunk);
            } catch (IOException | RuntimeException e) {
                // A service failure on a chunk read arrives wrapped in a RuntimeException; the
                // close and the typed mapping must both still happen.
                closeQuietly(generator);
                throw BallerinaAzureClient.mapFailure(e);
            }
        });
    }

    /** Closes an open content stream early. */
    public static Object closeContentStream(BObject generator) {
        closeQuietly(generator);
        return null;
    }

    static void closeQuietly(BObject generator) {
        BlobInputStream stream = (BlobInputStream) generator.getNativeData(NATIVE_INPUT_STREAM);
        if (stream != null) {
            generator.addNativeData(NATIVE_INPUT_STREAM, null);
            stream.close();
        }
    }

    /**
     * A fresh id for one stream upload, prefixed to its block ids so that concurrent uploads to
     * the same path never stage over each other's blocks (block ids are scoped to the blob).
     */
    public static BString newStreamUploadId() {
        return StringUtils.fromString(UUID.randomUUID().toString().replace("-", ""));
    }

    /**
     * The block id of the {@code index}-th staged block of an upload: the upload id and a
     * zero-padded decimal, base64-encoded. Every id of one upload has the same length, which
     * the commit requires, and stays under the service's 64-byte limit.
     */
    static String blockId(String uploadId, long index) {
        return Base64.getEncoder().encodeToString(
                String.format("%s-%08d", uploadId, index).getBytes(StandardCharsets.US_ASCII));
    }

    // The explicit content headers, with the content type filled from the applied serialization
    // format when the caller set none.
    private static BlobHttpHeaders headersWithAutoContentType(Object options, Object appliedFormat) {
        BlobHttpHeaders headers = OptionsReader.contentHeadersOf(options);
        if (appliedFormat == null) {
            return headers;
        }
        if (headers == null) {
            headers = new BlobHttpHeaders();
        }
        if (headers.getContentType() == null) {
            String format = ((BString) appliedFormat).getValue();
            headers.setContentType(FORMAT_JSON.equals(format) ? MEDIA_TYPE_JSON
                    : FORMAT_XML.equals(format) ? MEDIA_TYPE_XML : MEDIA_TYPE_CSV);
        }
        return headers;
    }

    private static BlobRange range(Object options) {
        BMap<BString, Object> record = OptionsReader.record(options);
        return record == null ? null : OptionsReader.range(record.get(OptionsReader.RANGE));
    }

    private static byte[] contentBytes(Object content) {
        if (content instanceof BArray array) {
            return array.getBytes();
        }
        if (content instanceof BString string) {
            return string.getValue().getBytes(StandardCharsets.UTF_8);
        }
        return ((BXml) content).toString().getBytes(StandardCharsets.UTF_8);
    }
}
