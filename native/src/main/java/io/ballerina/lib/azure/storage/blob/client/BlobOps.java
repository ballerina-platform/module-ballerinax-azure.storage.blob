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
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the single-blob operations: existence, deletion and restore,
 * properties, metadata, and content headers.
 */
public final class BlobOps {

    // The Ballerina DeleteSnapshotsOption enum value that keeps the blob and deletes its snapshots.
    private static final String DELETE_SNAPSHOTS_ONLY = "only";

    private BlobOps() {
    }

    /** Checks whether the blob exists; {@code false} only on a confirmed 404. */
    public static Object hasBlob(Environment env, BObject self, BString path) {
        return BallerinaAzureClient.invoke(env, () -> blobClient(self, path).exists());
    }

    /** Deletes a blob, one of its snapshots, or the blob together with (or only) its snapshots. */
    public static Object deleteBlob(Environment env, BObject self, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            DeleteSnapshotsOptionType snapshots = null;
            if (record != null) {
                String option = ValueUtils.optString(record, OptionsReader.DELETE_SNAPSHOTS);
                if (option != null) {
                    snapshots = DELETE_SNAPSHOTS_ONLY.equals(option)
                            ? DeleteSnapshotsOptionType.ONLY : DeleteSnapshotsOptionType.INCLUDE;
                }
            }
            blobClient(self, path, OptionsReader.snapshotId(options))
                    .deleteWithResponse(snapshots, OptionsReader.leaseConditions(options), null, null);
            return null;
        });
    }

    /** Restores a soft-deleted blob along with its soft-deleted snapshots. */
    public static Object undeleteBlob(Environment env, BObject self, BString path) {
        return BallerinaAzureClient.invoke(env, () -> {
            blobClient(self, path).undelete();
            return null;
        });
    }

    /** Fetches a blob's properties, metadata, and copy state as a {@code BlobProperties} record. */
    public static Object getBlobProperties(Environment env, BObject self, BString path) {
        return BallerinaAzureClient.invoke(env,
                () -> RecordMapper.blobProperties(blobClient(self, path).getProperties()));
    }

    /** Replaces a blob's user-defined metadata. */
    public static Object setBlobMetadata(Environment env, BObject self, BString path, BMap<BString, BString> metadata,
                                         Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            blobClient(self, path).setMetadataWithResponse(ValueUtils.toStringMap(metadata),
                    OptionsReader.leaseConditions(options), null, null);
            return null;
        });
    }

    /** Replaces a blob's content headers as a whole set. */
    public static Object setContentHeaders(Environment env, BObject self, BString path, BMap<BString, Object> headers,
                                           Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            blobClient(self, path).setHttpHeadersWithResponse(OptionsReader.contentHeaders(headers),
                    OptionsReader.leaseConditions(options), null, null);
            return null;
        });
    }

    /** Returns the SDK blob client for a container-relative path. */
    static BlobClient blobClient(BObject self, BString path) {
        return BallerinaAzureClient.getContainerClient(self).getBlobClient(BallerinaAzureClient.blobPath(path));
    }

    /** Returns the SDK blob client for a path, bound to a snapshot when an id is given. */
    static BlobClient blobClient(BObject self, BString path, String snapshotId) {
        BlobClient client = blobClient(self, path);
        return snapshotId == null ? client : client.getSnapshotClient(snapshotId);
    }
}
