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

import com.azure.storage.blob.BlobServiceClient;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the account-scoped {@code AdminClient} operations: container
 * lifecycle, service configuration, account information, and the user delegation key.
 */
public final class AdminOps {

    private AdminOps() {
    }

    /** Checks whether a container exists; {@code false} only on a confirmed 404. */
    public static Object hasContainer(Environment env, BObject self, BString containerName) {
        return BallerinaAzureClient.invoke(env, () -> BallerinaAzureClient.getServiceClient(self)
                .getBlobContainerClient(containerName(containerName)).exists());
    }

    /** Creates a container with optional metadata and anonymous access level. */
    public static Object createContainer(Environment env, BObject self, BString containerName, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            BallerinaAzureClient.getServiceClient(self).createBlobContainerWithResponse(containerName(containerName),
                    OptionsReader.metadata(options),
                    record == null ? null : OptionsReader.publicAccess(record.get(OptionsReader.PUBLIC_ACCESS)),
                    null);
            return null;
        });
    }

    /** Deletes a container and every blob in it, honouring an active lease. */
    public static Object deleteContainer(Environment env, BObject self, BString containerName, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BallerinaAzureClient.getServiceClient(self).getBlobContainerClient(containerName(containerName))
                    .deleteWithResponse(OptionsReader.leaseConditions(options), null, null);
            return null;
        });
    }

    /** Restores a soft-deleted container by name and deleted version. */
    public static Object undeleteContainer(Environment env, BObject self, BString containerName,
                                           BString deletedContainerVersion) {
        return BallerinaAzureClient.invoke(env, () -> {
            BallerinaAzureClient.getServiceClient(self).undeleteBlobContainer(containerName(containerName),
                    deletedContainerVersion.getValue());
            return null;
        });
    }

    /** Reads the blob service configuration as a {@code ServiceProperties} record. */
    public static Object getServiceProperties(Environment env, BObject self) {
        return BallerinaAzureClient.invoke(env,
                () -> RecordMapper.serviceProperties(BallerinaAzureClient.getServiceClient(self).getProperties()));
    }

    /** Writes the blob service configuration; groups absent from the record are left unchanged. */
    public static Object setServiceProperties(Environment env, BObject self, BMap<BString, Object> properties) {
        return BallerinaAzureClient.invoke(env, () -> {
            BallerinaAzureClient.getServiceClient(self).setProperties(OptionsReader.serviceProperties(properties));
            return null;
        });
    }

    /** Reads the account's SKU, kind, and namespace type. */
    public static Object getAccountInfo(Environment env, BObject self) {
        return BallerinaAzureClient.invoke(env,
                () -> RecordMapper.accountInfo(BallerinaAzureClient.getServiceClient(self).getAccountInfo()));
    }

    /** Obtains a user delegation key for the given validity window. */
    public static Object getUserDelegationKey(Environment env, BObject self, BArray startTime, BArray expiryTime) {
        return BallerinaAzureClient.invoke(env, () -> {
            BlobServiceClient client = BallerinaAzureClient.getServiceClient(self);
            return RecordMapper.userDelegationKey(
                    client.getUserDelegationKey(ValueUtils.fromUtc(startTime), ValueUtils.fromUtc(expiryTime)));
        });
    }

    private static String containerName(BString name) {
        String value = name.getValue().strip();
        if (value.isEmpty()) {
            throw BlobErrorCreator.clientError("containerName must not be empty", null);
        }
        return value;
    }
}
