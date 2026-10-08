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

import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.options.BlockBlobStageBlockFromUrlOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Native implementations of the block operations, the advanced path for composing a block
 * blob from separately staged pieces. Block ids are checked locally: each must be valid
 * base64, and every id in one commit must have the same length.
 */
public final class BlockOps {

    private BlockOps() {
    }

    /** Stages a block under a base64 id. */
    public static Object stageBlock(Environment env, BObject self, BString path, BString blockId, BArray content,
                                    Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = content.getBytes();
            blockClient(self, path).stageBlockWithResponse(validBlockId(blockId.getValue()),
                    new ByteArrayInputStream(bytes), bytes.length, null, OptionsReader.leaseId(options), null, null);
            return null;
        });
    }

    /** Stages a block from a readable blob URL, or a range of it. */
    public static Object stageBlockFromUrl(Environment env, BObject self, BString path, BString blockId,
                                           BString sourceUrl, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            BlobRange sourceRange = record == null ? null : OptionsReader.range(record.get(OptionsReader.SOURCE_RANGE));
            blockClient(self, path).stageBlockFromUrlWithResponse(
                    new BlockBlobStageBlockFromUrlOptions(validBlockId(blockId.getValue()), sourceUrl.getValue())
                            .setSourceRange(sourceRange == null ? new BlobRange(0) : sourceRange)
                            .setLeaseId(OptionsReader.leaseId(options)), null, null);
            return null;
        });
    }

    /** Commits staged blocks, in order, as the blob's content, replacing any existing blob. */
    public static Object commitBlockList(Environment env, BObject self, BString path, BArray blockIds, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            List<String> ids = Arrays.asList(blockIds.getStringArray());
            int length = -1;
            for (String id : ids) {
                validBlockId(id);
                if (length >= 0 && id.length() != length) {
                    throw BlobErrorCreator.clientError("every block id in a commit must have the same length", null);
                }
                length = id.length();
            }
            blockClient(self, path).commitBlockListWithResponse(new BlockBlobCommitBlockListOptions(ids)
                    .setHeaders(OptionsReader.contentHeadersOf(options))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setTier(OptionsReader.accessTier(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options)), null, null);
            return null;
        });
    }

    /** Lists the committed and uncommitted blocks of a block blob. */
    public static Object listBlocks(Environment env, BObject self, BString path) {
        return BallerinaAzureClient.invoke(env,
                () -> RecordMapper.blockList(blockClient(self, path).listBlocks(BlockListType.ALL)));
    }

    private static BlockBlobClient blockClient(BObject self, BString path) {
        return BlobOps.blobClient(self, path).getBlockBlobClient();
    }

    private static String validBlockId(String blockId) {
        if (blockId.isEmpty()) {
            throw BlobErrorCreator.clientError("a block id must not be empty", null);
        }
        try {
            Base64.getDecoder().decode(blockId);
        } catch (IllegalArgumentException e) {
            throw BlobErrorCreator.clientError("block id '" + blockId + "' is not valid base64", e);
        }
        return blockId;
    }
}
