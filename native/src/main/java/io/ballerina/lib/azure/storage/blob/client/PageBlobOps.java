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
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.PageBlobRequestConditions;
import com.azure.storage.blob.models.PageRange;
import com.azure.storage.blob.models.PageRangeItem;
import com.azure.storage.blob.options.ListPageRangesOptions;
import com.azure.storage.blob.options.PageBlobCreateOptions;
import com.azure.storage.blob.specialized.PageBlobClient;
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

/**
 * Native implementations of the page blob operations. Offsets and lengths are checked locally
 * against the 512-byte page alignment the service requires.
 */
public final class PageBlobOps {

    private static final int PAGE_SIZE = 512;

    private PageBlobOps() {
    }

    /** Creates a page blob of the given size, replacing any existing blob at the path. */
    public static Object createPageBlob(Environment env, BObject self, BString path, long sizeInBytes, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            requireAligned(sizeInBytes, "sizeInBytes");
            pageClient(self, path, null).createWithResponse(new PageBlobCreateOptions(sizeInBytes)
                    .setHeaders(OptionsReader.contentHeadersOf(options))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options)), null, null);
            return null;
        });
    }

    /** Writes pages at an offset. */
    public static Object uploadPages(Environment env, BObject self, BString path, long offset, BArray content,
                                     Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = content.getBytes();
            requireAligned(offset, "offset");
            requireAligned(bytes.length, "the content length");
            pageClient(self, path, null).uploadPagesWithResponse(pageRange(offset, bytes.length),
                    new ByteArrayInputStream(bytes), null, conditions(options), null, null);
            return null;
        });
    }

    /** Clears a page range. */
    public static Object clearPages(Environment env, BObject self, BString path, long offset, long length,
                                    Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            requireAligned(offset, "offset");
            requireAligned(length, "length");
            pageClient(self, path, null).clearPagesWithResponse(pageRange(offset, length), conditions(options),
                    null, null);
            return null;
        });
    }

    /** Lists the written page ranges of a page blob (or of one of its snapshots). */
    public static Object listPageRanges(Environment env, BObject self, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            BlobRange range = record == null ? null : OptionsReader.range(record.get(OptionsReader.RANGE));
            BArray ranges = RecordMapper.recordArray(RecordMapper.RECORD_PAGE_RANGE);
            for (PageRangeItem item : pageClient(self, path, OptionsReader.snapshotId(options))
                    .listPageRanges(new ListPageRangesOptions(range == null ? new BlobRange(0) : range),
                            null, Context.NONE)) {
                ranges.append(RecordMapper.pageRange(item));
            }
            return ranges;
        });
    }

    private static PageBlobClient pageClient(BObject self, BString path, String snapshotId) {
        return BlobOps.blobClient(self, path, snapshotId).getPageBlobClient();
    }

    private static PageRange pageRange(long offset, long length) {
        if (length <= 0) {
            throw BlobErrorCreator.clientError("the page range length must be positive", null);
        }
        return new PageRange().setStart(offset).setEnd(offset + length - 1);
    }

    private static void requireAligned(long value, String what) {
        if (value < 0 || value % PAGE_SIZE != 0) {
            throw BlobErrorCreator.clientError(what + " must be a non-negative multiple of " + PAGE_SIZE, null);
        }
    }

    private static PageBlobRequestConditions conditions(Object options) {
        String leaseId = OptionsReader.leaseId(options);
        return leaseId == null ? null : new PageBlobRequestConditions().setLeaseId(leaseId);
    }
}
