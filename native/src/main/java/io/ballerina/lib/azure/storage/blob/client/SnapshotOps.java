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

import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementation of the snapshot operation.
 */
public final class SnapshotOps {

    private SnapshotOps() {
    }

    /** Creates a read-only snapshot of a blob and returns its id. */
    public static Object createSnapshot(Environment env, BObject self, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> StringUtils.fromString(BlobOps.blobClient(self, path)
                .createSnapshotWithResponse(OptionsReader.metadata(options), OptionsReader.leaseConditions(options),
                        null, null)
                .getValue().getSnapshotId()));
    }
}
