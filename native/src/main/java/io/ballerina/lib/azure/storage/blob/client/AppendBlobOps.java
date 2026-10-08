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

import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.AppendBlobAppendBlockFromUrlOptions;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.io.ByteArrayInputStream;

/**
 * Native implementations of the append blob operations.
 */
public final class AppendBlobOps {

    private AppendBlobOps() {
    }

    /** Creates an empty append blob, replacing any existing blob at the path. */
    public static Object createAppendBlob(Environment env, BObject self, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            appendClient(self, path).createWithResponse(new AppendBlobCreateOptions()
                    .setHeaders(OptionsReader.contentHeadersOf(options))
                    .setMetadata(OptionsReader.metadata(options))
                    .setTags(OptionsReader.tags(options))
                    .setRequestConditions(OptionsReader.leaseConditions(options)), null, null);
            return null;
        });
    }

    /** Appends a block of bytes to an append blob. */
    public static Object appendBlock(Environment env, BObject self, BString path, BArray content, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = content.getBytes();
            appendClient(self, path).appendBlockWithResponse(new ByteArrayInputStream(bytes), bytes.length, null,
                    conditions(options), null, null);
            return null;
        });
    }

    /** Appends the content of a readable blob URL to an append blob. */
    public static Object appendBlockFromUrl(Environment env, BObject self, BString path, BString sourceUrl,
                                            Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            appendClient(self, path).appendBlockFromUrlWithResponse(
                    new AppendBlobAppendBlockFromUrlOptions(sourceUrl.getValue())
                            .setSourceRange(new BlobRange(0))
                            .setDestinationRequestConditions(conditions(options)), null, null);
            return null;
        });
    }

    private static AppendBlobClient appendClient(BObject self, BString path) {
        return BlobOps.blobClient(self, path).getAppendBlobClient();
    }

    private static AppendBlobRequestConditions conditions(Object options) {
        String leaseId = OptionsReader.leaseId(options);
        return leaseId == null ? null : new AppendBlobRequestConditions().setLeaseId(leaseId);
    }
}
