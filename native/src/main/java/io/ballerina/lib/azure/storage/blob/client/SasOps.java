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

import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import com.azure.storage.common.sas.SasIpRange;
import com.azure.storage.common.sas.SasProtocol;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.time.OffsetDateTime;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The SAS signers. Signing is local (the account key or a user delegation key), so these are
 * ordinary methods that never leave the strand; what is knowable at signing time is validated
 * first.
 */
public final class SasOps {

    // The Ballerina SasProtocol enum value selecting HTTPS-only access.
    private static final String SAS_PROTOCOL_HTTPS = "https";
    // Field names of the SAS signature-values and permission records (read only here).
    private static final BString IP_RANGE = StringUtils.fromString("ipRange");
    private static final BString IDENTIFIER = StringUtils.fromString("identifier");
    private static final BString PROTOCOL = StringUtils.fromString("protocol");
    private static final BString SERVICES = StringUtils.fromString("services");
    private static final BString RESOURCE_TYPES = StringUtils.fromString("resourceTypes");
    private static final BString PERMISSION_READ = StringUtils.fromString("read");
    private static final BString PERMISSION_ADD = StringUtils.fromString("add");
    private static final BString PERMISSION_CREATE = StringUtils.fromString("create");
    private static final BString PERMISSION_WRITE = StringUtils.fromString("write");
    private static final BString PERMISSION_DELETE = StringUtils.fromString("delete");
    private static final BString PERMISSION_LIST = StringUtils.fromString("list");
    private static final BString PERMISSION_TAG = StringUtils.fromString("tag");
    private static final BString PERMISSION_FILTER = StringUtils.fromString("filter");
    private static final BString PERMISSION_UPDATE = StringUtils.fromString("update");
    private static final BString PERMISSION_PROCESS = StringUtils.fromString("process");
    private static final BString SERVICE_BLOB = StringUtils.fromString("blob");
    private static final BString SERVICE_QUEUE = StringUtils.fromString("queue");
    private static final BString RESOURCE_SERVICE = StringUtils.fromString("service");
    private static final BString RESOURCE_CONTAINER = StringUtils.fromString("container");
    private static final BString RESOURCE_OBJECT = StringUtils.fromString("object");

    private SasOps() {
    }

    /** Signs an account SAS with the account key. */
    public static Object generateAccountSas(BObject self, BMap<BString, Object> values) {
        return sign(() -> {
            BMap<BString, Object> permissions = record(values.get(RecordMapper.PERMISSIONS));
            AccountSasPermission sasPermission = new AccountSasPermission()
                    .setReadPermission(permissions.getBooleanValue(PERMISSION_READ))
                    .setWritePermission(permissions.getBooleanValue(PERMISSION_WRITE))
                    .setDeletePermission(permissions.getBooleanValue(PERMISSION_DELETE))
                    .setListPermission(permissions.getBooleanValue(PERMISSION_LIST))
                    .setAddPermission(permissions.getBooleanValue(PERMISSION_ADD))
                    .setCreatePermission(permissions.getBooleanValue(PERMISSION_CREATE))
                    .setUpdatePermission(permissions.getBooleanValue(PERMISSION_UPDATE))
                    .setProcessMessages(permissions.getBooleanValue(PERMISSION_PROCESS))
                    .setTagsPermission(permissions.getBooleanValue(PERMISSION_TAG))
                    .setFilterTagsPermission(permissions.getBooleanValue(PERMISSION_FILTER));
            BMap<BString, Object> services = record(values.get(SERVICES));
            AccountSasService sasServices = new AccountSasService()
                    .setBlobAccess(services.getBooleanValue(SERVICE_BLOB))
                    .setQueueAccess(services.getBooleanValue(SERVICE_QUEUE));
            BMap<BString, Object> resourceTypes = record(values.get(RESOURCE_TYPES));
            AccountSasResourceType sasResourceTypes = new AccountSasResourceType()
                    .setService(resourceTypes.getBooleanValue(RESOURCE_SERVICE))
                    .setContainer(resourceTypes.getBooleanValue(RESOURCE_CONTAINER))
                    .setObject(resourceTypes.getBooleanValue(RESOURCE_OBJECT));
            AccountSasSignatureValues sdkValues = new AccountSasSignatureValues(
                    ValueUtils.fromUtc((BArray) values.get(RecordMapper.EXPIRY_TIME)),
                    sasPermission, sasServices, sasResourceTypes);
            applyCommon(values, sdkValues::setStartTime, sdkValues::setProtocol, sdkValues::setSasIpRange);
            return BallerinaAzureClient.getServiceClient(self).generateAccountSas(sdkValues);
        });
    }

    /** Signs a container SAS with the account key. */
    public static Object generateContainerSas(BObject self, BMap<BString, Object> values) {
        return sign(() -> BallerinaAzureClient.getContainerClient(self).generateSas(sasValues(values, true, false)));
    }

    /** Signs a blob SAS with the account key. */
    public static Object generateSas(BObject self, BString path, BMap<BString, Object> values) {
        return sign(() -> BlobOps.blobClient(self, path).generateSas(sasValues(values, false, false)));
    }

    /** Signs a container SAS with a user delegation key. */
    public static Object generateContainerUserDelegationSas(BObject self, BMap<BString, Object> values,
                                                            BMap<BString, Object> key) {
        return sign(() -> BallerinaAzureClient.getContainerClient(self)
                .generateUserDelegationSas(sasValues(values, true, true), delegationKey(key)));
    }

    /** Signs a blob SAS with a user delegation key. */
    public static Object generateUserDelegationSas(BObject self, BString path, BMap<BString, Object> values,
                                                   BMap<BString, Object> key) {
        return sign(() -> BlobOps.blobClient(self, path)
                .generateUserDelegationSas(sasValues(values, false, true), delegationKey(key)));
    }

    // Runs a signer on the strand, mapping every failure to the module's error.
    private static Object sign(Supplier<String> signer) {
        try {
            return StringUtils.fromString(signer.get());
        } catch (BError e) {
            return e;
        } catch (RuntimeException e) {
            return BlobErrorCreator.clientError(
                    "cannot generate the SAS token: " + BallerinaAzureClient.describe(e), e);
        }
    }

    private static BlobServiceSasSignatureValues sasValues(BMap<BString, Object> values, boolean containerScope,
                                                           boolean userDelegation) {
        BMap<BString, Object> permissions = record(values.get(RecordMapper.PERMISSIONS));
        Object expiryValue = values.get(RecordMapper.EXPIRY_TIME);
        String identifier = ValueUtils.optString(values, IDENTIFIER);
        if (userDelegation) {
            if (identifier != null) {
                throw BlobErrorCreator.clientError(
                        "a user delegation SAS cannot use a stored access policy identifier", null);
            }
            if (expiryValue == null || permissions == null) {
                throw BlobErrorCreator.clientError(
                        "expiryTime and permissions must be set for a user delegation SAS", null);
            }
        } else if (identifier == null && (expiryValue == null || permissions == null)) {
            throw BlobErrorCreator.clientError("either identifier, or expiryTime and permissions, must be set", null);
        }
        BlobServiceSasSignatureValues sdkValues;
        if (identifier != null) {
            sdkValues = new BlobServiceSasSignatureValues(identifier);
            if (expiryValue != null) {
                sdkValues.setExpiryTime(ValueUtils.fromUtc((BArray) expiryValue));
            }
            if (permissions != null) {
                if (containerScope) {
                    sdkValues.setPermissions(containerPermissions(permissions));
                } else {
                    sdkValues.setPermissions(blobPermissions(permissions));
                }
            }
        } else {
            OffsetDateTime expiry = ValueUtils.fromUtc((BArray) expiryValue);
            sdkValues = containerScope
                    ? new BlobServiceSasSignatureValues(expiry, containerPermissions(permissions))
                    : new BlobServiceSasSignatureValues(expiry, blobPermissions(permissions));
        }
        applyCommon(values, sdkValues::setStartTime, sdkValues::setProtocol, sdkValues::setSasIpRange);
        return sdkValues;
    }

    private static BlobContainerSasPermission containerPermissions(BMap<BString, Object> permissions) {
        return new BlobContainerSasPermission()
                .setReadPermission(permissions.getBooleanValue(PERMISSION_READ))
                .setAddPermission(permissions.getBooleanValue(PERMISSION_ADD))
                .setCreatePermission(permissions.getBooleanValue(PERMISSION_CREATE))
                .setWritePermission(permissions.getBooleanValue(PERMISSION_WRITE))
                .setDeletePermission(permissions.getBooleanValue(PERMISSION_DELETE))
                .setListPermission(permissions.getBooleanValue(PERMISSION_LIST))
                .setTagsPermission(permissions.getBooleanValue(PERMISSION_TAG))
                .setFilterPermission(permissions.getBooleanValue(PERMISSION_FILTER));
    }

    private static BlobSasPermission blobPermissions(BMap<BString, Object> permissions) {
        return new BlobSasPermission()
                .setReadPermission(permissions.getBooleanValue(PERMISSION_READ))
                .setAddPermission(permissions.getBooleanValue(PERMISSION_ADD))
                .setCreatePermission(permissions.getBooleanValue(PERMISSION_CREATE))
                .setWritePermission(permissions.getBooleanValue(PERMISSION_WRITE))
                .setDeletePermission(permissions.getBooleanValue(PERMISSION_DELETE))
                .setTagsPermission(permissions.getBooleanValue(PERMISSION_TAG));
    }

    private static void applyCommon(BMap<BString, Object> values,
            Function<OffsetDateTime, ?> setStartTime,
            Function<SasProtocol, ?> setProtocol,
            Function<SasIpRange, ?> setIpRange) {
        Object startTime = values.get(RecordMapper.START_TIME);
        if (startTime != null) {
            setStartTime.apply(ValueUtils.fromUtc((BArray) startTime));
        }
        String protocol = ValueUtils.optString(values, PROTOCOL);
        if (protocol != null) {
            setProtocol.apply(SAS_PROTOCOL_HTTPS.equals(protocol) ? SasProtocol.HTTPS_ONLY : SasProtocol.HTTPS_HTTP);
        }
        String ipRange = ValueUtils.optString(values, IP_RANGE);
        if (ipRange != null) {
            setIpRange.apply(SasIpRange.parse(ipRange));
        }
    }

    private static UserDelegationKey delegationKey(BMap<BString, Object> key) {
        return new UserDelegationKey()
                .setSignedObjectId(key.getStringValue(RecordMapper.SIGNED_OBJECT_ID).getValue())
                .setSignedTenantId(key.getStringValue(RecordMapper.SIGNED_TENANT_ID).getValue())
                .setSignedStart(ValueUtils.fromUtc((BArray) key.get(RecordMapper.SIGNED_START)))
                .setSignedExpiry(ValueUtils.fromUtc((BArray) key.get(RecordMapper.SIGNED_EXPIRY)))
                .setSignedService(key.getStringValue(RecordMapper.SIGNED_SERVICE).getValue())
                .setSignedVersion(key.getStringValue(RecordMapper.SIGNED_VERSION).getValue())
                .setValue(key.getStringValue(RecordMapper.VALUE).getValue());
    }

    @SuppressWarnings("unchecked")
    private static BMap<BString, Object> record(Object value) {
        return (BMap<BString, Object>) value;
    }
}
