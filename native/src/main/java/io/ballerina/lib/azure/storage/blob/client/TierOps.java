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

import com.azure.storage.blob.models.AccessTier;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementation of the access tier operation.
 */
public final class TierOps {

    private TierOps() {
    }

    /** Moves a blob to an access tier, with an optional rehydration priority when leaving Archive. */
    public static Object setAccessTier(Environment env, BObject self, BString path, BString accessTier,
                                       Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobOps.blobClient(self, path).setAccessTierWithResponse(AccessTier.fromString(accessTier.getValue()),
                    OptionsReader.rehydratePriority(options), OptionsReader.leaseId(options), null, null);
            return null;
        });
    }
}
