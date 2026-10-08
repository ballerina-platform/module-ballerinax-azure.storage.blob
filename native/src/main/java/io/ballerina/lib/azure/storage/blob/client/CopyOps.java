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

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.options.BlobBeginCopyOptions;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the copy operations. A copy is asynchronous on the service: the
 * call returns as soon as the service accepts it, and the copy state is read later through
 * {@code getBlobProperties}.
 */
public final class CopyOps {

    private CopyOps() {
    }

    /**
     * Starts a copy of a blob in the bound container to another path in it. The service
     * authorizes the copy source separately from the request, so a SAS credential is attached
     * to the source URL; a shared key authorizes a same-account source on its own.
     */
    public static Object copyBlob(Environment env, BObject self, BString sourcePath, BString destinationPath,
                                  Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            String sourceUrl = BlobOps.blobClient(self, sourcePath).getBlobUrl();
            String signature = (String) self.getNativeData(BallerinaAzureClient.NATIVE_SAS_SIGNATURE);
            if (signature != null) {
                sourceUrl = sourceUrl + (sourceUrl.contains("?") ? "&" : "?") + signature;
            }
            return beginCopy(self, sourceUrl, destinationPath, options);
        });
    }

    /** Starts a copy from any readable blob URL to a path in the bound container. */
    public static Object copyBlobFromUrl(Environment env, BObject self, BString sourceUrl, BString destinationPath,
                                         Object options) {
        return BallerinaAzureClient.invoke(env,
                () -> beginCopy(self, sourceUrl.getValue(), destinationPath, options));
    }

    /** Aborts a pending copy by its id. */
    public static Object abortCopy(Environment env, BObject self, BString path, BString copyId, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobOps.blobClient(self, path).abortCopyFromUrlWithResponse(copyId.getValue(),
                    OptionsReader.leaseId(options), null, null);
            return null;
        });
    }

    // Issues the copy request and reports the id and status at acceptance. The poller is never
    // waited on: the connector does not block a strand on a service-side copy.
    private static Object beginCopy(BObject self, String sourceUrl, BString destinationPath, Object options) {
        BlobBeginCopyOptions sdkOptions = new BlobBeginCopyOptions(sourceUrl)
                .setMetadata(OptionsReader.metadata(options))
                .setTags(OptionsReader.tags(options))
                .setTier(OptionsReader.accessTier(options))
                .setDestinationRequestConditions(OptionsReader.leaseConditions(options));
        SyncPoller<BlobCopyInfo, Void> poller = BlobOps.blobClient(self, destinationPath).beginCopy(sdkOptions);
        return RecordMapper.copyInfo(poller.poll().getValue());
    }
}
