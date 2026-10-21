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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerAccessPolicies;
import com.azure.storage.blob.models.BlobRequestConditions;
import io.ballerina.lib.azure.storage.blob.observability.BlobMetricsUtil;
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

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Native implementations of the bound container's own operations: properties, metadata, and
 * the access policy. The wire sets the anonymous access level and the stored access policies
 * together, so each setter reads the current half it does not change and resends both. The
 * write is conditioned on the container being unmodified since that read (the service offers
 * no ETag condition on container operations), so a concurrent change surfaces as a
 * precondition failure instead of being overwritten.
 */
public final class ContainerOps {

    // The service accepts at most five stored access policies per container.
    private static final int MAX_SIGNED_IDENTIFIERS = 5;

    private ContainerOps() {
    }

    /** Reads the bound container's properties and metadata. */
    public static Object getContainerProperties(Environment env, BObject self) {
        return BallerinaAzureClient.invoke(env, self, BlobMetricsUtil.OPERATION_TYPE_GET, null, null,
                () -> RecordMapper.containerProperties(BallerinaAzureClient.getContainerClient(self).getProperties()));
    }

    /** Replaces the bound container's metadata. */
    public static Object setContainerMetadata(Environment env, BObject self, BMap<BString, BString> metadata,
                                              Object options) {
        return BallerinaAzureClient.invoke(env, self, BlobMetricsUtil.OPERATION_TYPE_MANAGE,
                null, null, () -> {
            BallerinaAzureClient.getContainerClient(self).setMetadataWithResponse(
                    ValueUtils.toStringMap(metadata), OptionsReader.leaseConditions(options), null, null);
            return null;
        });
    }

    /** Reads the anonymous access level together with the stored access policies. */
    public static Object getContainerAccessPolicy(Environment env, BObject self) {
        return BallerinaAzureClient.invoke(env, self, BlobMetricsUtil.OPERATION_TYPE_GET,
                null, null, () -> RecordMapper.containerAccessPolicy(
                BallerinaAzureClient.getContainerClient(self).getAccessPolicy()));
    }

    /** Sets the anonymous access level, keeping the current stored access policies. */
    public static Object setPublicAccess(Environment env, BObject self, BString access, Object options) {
        return BallerinaAzureClient.invoke(env, self, BlobMetricsUtil.OPERATION_TYPE_MANAGE,
                null, null, () -> {
            BlobContainerClient container = BallerinaAzureClient.getContainerClient(self);
            BlobRequestConditions conditions = writeConditions(options);
            Response<BlobContainerAccessPolicies> current =
                    container.getAccessPolicyWithResponse(conditions.getLeaseId(), null, null);
            container.setAccessPolicyWithResponse(OptionsReader.publicAccess(access),
                    current.getValue().getIdentifiers(), unmodifiedSince(conditions, current), null, null);
            return null;
        });
    }

    /** Replaces the stored access policies, keeping the current anonymous access level. */
    public static Object setContainerAccessPolicy(Environment env, BObject self, BArray identifiers, Object options) {
        return BallerinaAzureClient.invoke(env, self, BlobMetricsUtil.OPERATION_TYPE_MANAGE,
                null, null, () -> {
            if (identifiers.size() > MAX_SIGNED_IDENTIFIERS) {
                throw BlobErrorCreator.clientError(
                        "a container holds at most " + MAX_SIGNED_IDENTIFIERS + " stored access policies", null);
            }
            BlobContainerClient container = BallerinaAzureClient.getContainerClient(self);
            BlobRequestConditions conditions = writeConditions(options);
            Response<BlobContainerAccessPolicies> current =
                    container.getAccessPolicyWithResponse(conditions.getLeaseId(), null, null);
            container.setAccessPolicyWithResponse(current.getValue().getBlobAccessType(),
                    OptionsReader.signedIdentifiers(identifiers), unmodifiedSince(conditions, current), null, null);
            return null;
        });
    }

    private static BlobRequestConditions writeConditions(Object options) {
        BlobRequestConditions conditions = OptionsReader.leaseConditions(options);
        return conditions == null ? new BlobRequestConditions() : conditions;
    }

    // Conditions the write on the Last-Modified the read reported; absent (an emulator that
    // omits the header), the write goes unconditioned as before.
    private static BlobRequestConditions unmodifiedSince(BlobRequestConditions conditions, Response<?> read) {
        String lastModified = read.getHeaders().getValue(HttpHeaderName.LAST_MODIFIED);
        if (lastModified != null) {
            conditions.setIfUnmodifiedSince(OffsetDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME));
        }
        return conditions;
    }
}
