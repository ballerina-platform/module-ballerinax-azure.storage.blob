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

package io.ballerina.lib.azure.storage.blob.util;

import com.azure.core.http.HttpRange;
import com.azure.storage.blob.models.BlobAnalyticsLogging;
import com.azure.storage.blob.models.BlobContainerAccessPolicies;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobCorsRule;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobMetrics;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRetentionPolicy;
import com.azure.storage.blob.models.BlobServiceProperties;
import com.azure.storage.blob.models.BlobSignedIdentifier;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.LeaseDurationType;
import com.azure.storage.blob.models.LeaseStateType;
import com.azure.storage.blob.models.LeaseStatusType;
import com.azure.storage.blob.models.PageRangeItem;
import com.azure.storage.blob.models.PublicAccessType;
import com.azure.storage.blob.models.StaticWebsite;
import com.azure.storage.blob.models.StorageAccountInfo;
import com.azure.storage.blob.models.TaggedBlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps the SDK model classes to the Ballerina result records declared in {@code types.bal}.
 * Optional record fields are set only when the service supplied a value.
 */
public final class RecordMapper {

    // The Ballerina record type names the mapper materializes.
    public static final String RECORD_CONTAINER_INFO = "ContainerInfo";
    public static final String RECORD_CONTAINER_LIST = "ContainerList";
    public static final String RECORD_CONTAINER_PROPERTIES = "ContainerProperties";
    public static final String RECORD_CONTAINER_ACCESS_POLICY = "ContainerAccessPolicy";
    public static final String RECORD_SIGNED_IDENTIFIER = "SignedIdentifier";
    public static final String RECORD_BLOB_ENTRY = "BlobEntry";
    public static final String RECORD_BLOB_LIST = "BlobList";
    public static final String RECORD_BLOB_PROPERTIES = "BlobProperties";
    public static final String RECORD_CONTENT_HEADERS = "ContentHeaders";
    public static final String RECORD_COPY_STATUS_INFO = "CopyStatusInfo";
    public static final String RECORD_COPY_INFO = "CopyInfo";
    public static final String RECORD_TAGGED_BLOB_ENTRY = "TaggedBlobEntry";
    public static final String RECORD_PAGE_RANGE = "PageRange";
    public static final String RECORD_BLOCK_INFO = "BlockInfo";
    public static final String RECORD_BLOCK_LIST = "BlockList";
    public static final String RECORD_SERVICE_PROPERTIES = "ServiceProperties";
    public static final String RECORD_METRICS_PROPERTIES = "MetricsProperties";
    public static final String RECORD_LOGGING_PROPERTIES = "LoggingProperties";
    public static final String RECORD_RETENTION_POLICY = "RetentionPolicy";
    public static final String RECORD_CORS_RULE = "CorsRule";
    public static final String RECORD_STATIC_WEBSITE_PROPERTIES = "StaticWebsiteProperties";
    public static final String RECORD_ACCOUNT_INFO = "AccountInfo";
    public static final String RECORD_USER_DELEGATION_KEY = "UserDelegationKey";

    // Result-record field names.
    public static final BString NAME = StringUtils.fromString("name");
    public static final BString PATH = StringUtils.fromString("path");
    public static final BString E_TAG = StringUtils.fromString("eTag");
    public static final BString LAST_MODIFIED = StringUtils.fromString("lastModified");
    public static final BString CREATED_TIME = StringUtils.fromString("createdTime");
    public static final BString PUBLIC_ACCESS = StringUtils.fromString("publicAccess");
    public static final BString LEASE_STATE = StringUtils.fromString("leaseState");
    public static final BString LEASE_STATUS = StringUtils.fromString("leaseStatus");
    public static final BString LEASE_DURATION = StringUtils.fromString("leaseDuration");
    public static final BString IS_DELETED = StringUtils.fromString("isDeleted");
    public static final BString DELETED_VERSION = StringUtils.fromString("deletedVersion");
    public static final BString DELETED_TIME = StringUtils.fromString("deletedTime");
    public static final BString REMAINING_RETENTION_DAYS = StringUtils.fromString("remainingRetentionDays");
    public static final BString CONTAINERS = StringUtils.fromString("containers");
    public static final BString BLOBS = StringUtils.fromString("blobs");
    public static final BString NEXT_MARKER = StringUtils.fromString("nextMarker");
    public static final BString HAS_IMMUTABILITY_POLICY = StringUtils.fromString("hasImmutabilityPolicy");
    public static final BString HAS_LEGAL_HOLD = StringUtils.fromString("hasLegalHold");
    public static final BString ACCESS = StringUtils.fromString("access");
    public static final BString IDENTIFIERS = StringUtils.fromString("identifiers");
    public static final BString ID = StringUtils.fromString("id");
    public static final BString START_TIME = StringUtils.fromString("startTime");
    public static final BString EXPIRY_TIME = StringUtils.fromString("expiryTime");
    public static final BString PERMISSIONS = StringUtils.fromString("permissions");
    public static final BString IS_PREFIX = StringUtils.fromString("isPrefix");
    public static final BString CONTENT_LENGTH = StringUtils.fromString("contentLength");
    public static final BString BLOB_TYPE = StringUtils.fromString("blobType");
    public static final BString ACCESS_TIER_INFERRED = StringUtils.fromString("accessTierInferred");
    public static final BString ARCHIVE_STATUS = StringUtils.fromString("archiveStatus");
    public static final BString COPY_STATUS = StringUtils.fromString("copyStatus");
    public static final BString COPY_ID = StringUtils.fromString("copyId");
    public static final BString COPY_SOURCE = StringUtils.fromString("copySource");
    public static final BString COPY_PROGRESS = StringUtils.fromString("copyProgress");
    public static final BString COPY_COMPLETION_TIME = StringUtils.fromString("copyCompletionTime");
    public static final BString COPY_STATUS_DESCRIPTION = StringUtils.fromString("copyStatusDescription");
    public static final BString BLOB_SEQUENCE_NUMBER = StringUtils.fromString("blobSequenceNumber");
    public static final BString COMMITTED_BLOCK_COUNT = StringUtils.fromString("committedBlockCount");
    public static final BString OFFSET = StringUtils.fromString("offset");
    public static final BString LENGTH = StringUtils.fromString("length");
    public static final BString BLOCK_ID = StringUtils.fromString("blockId");
    public static final BString SIZE_BYTES = StringUtils.fromString("sizeBytes");
    public static final BString COMMITTED_BLOCKS = StringUtils.fromString("committedBlocks");
    public static final BString UNCOMMITTED_BLOCKS = StringUtils.fromString("uncommittedBlocks");
    public static final BString HOUR_METRICS = StringUtils.fromString("hourMetrics");
    public static final BString MINUTE_METRICS = StringUtils.fromString("minuteMetrics");
    public static final BString LOGGING = StringUtils.fromString("logging");
    public static final BString CORS = StringUtils.fromString("cors");
    public static final BString DELETE_RETENTION_POLICY = StringUtils.fromString("deleteRetentionPolicy");
    public static final BString STATIC_WEBSITE = StringUtils.fromString("staticWebsite");
    public static final BString DEFAULT_SERVICE_VERSION = StringUtils.fromString("defaultServiceVersion");
    public static final BString VERSION = StringUtils.fromString("version");
    public static final BString ENABLED = StringUtils.fromString("enabled");
    public static final BString INCLUDE_APIS = StringUtils.fromString("includeApis");
    public static final BString RETENTION_POLICY = StringUtils.fromString("retentionPolicy");
    public static final BString DAYS = StringUtils.fromString("days");
    public static final BString READ = StringUtils.fromString("read");
    public static final BString WRITE = StringUtils.fromString("write");
    public static final BString DELETE = StringUtils.fromString("delete");
    public static final BString ALLOWED_ORIGINS = StringUtils.fromString("allowedOrigins");
    public static final BString ALLOWED_METHODS = StringUtils.fromString("allowedMethods");
    public static final BString ALLOWED_HEADERS = StringUtils.fromString("allowedHeaders");
    public static final BString EXPOSED_HEADERS = StringUtils.fromString("exposedHeaders");
    public static final BString MAX_AGE_IN_SECONDS = StringUtils.fromString("maxAgeInSeconds");
    public static final BString INDEX_DOCUMENT = StringUtils.fromString("indexDocument");
    public static final BString ERROR_DOCUMENT_404_PATH = StringUtils.fromString("errorDocument404Path");
    public static final BString DEFAULT_INDEX_DOCUMENT_PATH = StringUtils.fromString("defaultIndexDocumentPath");
    public static final BString SKU_NAME = StringUtils.fromString("skuName");
    public static final BString ACCOUNT_KIND = StringUtils.fromString("accountKind");
    public static final BString IS_HIERARCHICAL_NAMESPACE_ENABLED =
            StringUtils.fromString("isHierarchicalNamespaceEnabled");
    public static final BString SIGNED_OBJECT_ID = StringUtils.fromString("signedObjectId");
    public static final BString SIGNED_TENANT_ID = StringUtils.fromString("signedTenantId");
    public static final BString SIGNED_START = StringUtils.fromString("signedStart");
    public static final BString SIGNED_EXPIRY = StringUtils.fromString("signedExpiry");
    public static final BString SIGNED_SERVICE = StringUtils.fromString("signedService");
    public static final BString SIGNED_VERSION = StringUtils.fromString("signedVersion");
    public static final BString VALUE = StringUtils.fromString("value");

    // Array types are cached per record name: building one otherwise costs a throwaway
    // record value on every list-shaped call.
    private static final Map<String, ArrayType> ARRAY_TYPES = new ConcurrentHashMap<>();

    private RecordMapper() {
    }

    /** Maps one listed container to a {@code ContainerInfo} record. */
    public static BMap<BString, Object> containerInfo(BlobContainerItem item) {
        BMap<BString, Object> record = newRecord(RECORD_CONTAINER_INFO);
        BlobContainerItemProperties p = item.getProperties();
        record.put(NAME, StringUtils.fromString(item.getName()));
        record.put(LAST_MODIFIED, ValueUtils.toUtc(p.getLastModified()));
        record.put(E_TAG, StringUtils.fromString(p.getETag()));
        record.put(PUBLIC_ACCESS, publicAccess(p.getPublicAccess()));
        putLeaseFields(record, p.getLeaseState(), p.getLeaseStatus(), p.getLeaseDuration());
        if (item.getMetadata() != null) {
            record.put(OptionsReader.METADATA, ValueUtils.toBStringMap(item.getMetadata()));
        }
        if (Boolean.TRUE.equals(item.isDeleted())) {
            record.put(IS_DELETED, true);
        }
        if (item.getVersion() != null) {
            record.put(DELETED_VERSION, StringUtils.fromString(item.getVersion()));
        }
        if (p.getDeletedTime() != null) {
            record.put(DELETED_TIME, ValueUtils.toUtc(p.getDeletedTime()));
        }
        if (p.getRemainingRetentionDays() != null) {
            record.put(REMAINING_RETENTION_DAYS, p.getRemainingRetentionDays().longValue());
        }
        return record;
    }

    /** Builds a {@code ContainerList} record; the marker is set only when more remain. */
    public static BMap<BString, Object> containerList(BArray containers, String nextMarker) {
        BMap<BString, Object> record = newRecord(RECORD_CONTAINER_LIST);
        record.put(CONTAINERS, containers);
        if (nextMarker != null && !nextMarker.isEmpty()) {
            record.put(NEXT_MARKER, StringUtils.fromString(nextMarker));
        }
        return record;
    }

    /** Maps SDK container properties to a {@code ContainerProperties} record. */
    public static BMap<BString, Object> containerProperties(BlobContainerProperties p) {
        BMap<BString, Object> record = newRecord(RECORD_CONTAINER_PROPERTIES);
        record.put(LAST_MODIFIED, ValueUtils.toUtc(p.getLastModified()));
        record.put(E_TAG, StringUtils.fromString(p.getETag()));
        record.put(PUBLIC_ACCESS, publicAccess(p.getBlobPublicAccess()));
        record.put(OptionsReader.METADATA,
                ValueUtils.toBStringMap(p.getMetadata() == null ? Map.of() : p.getMetadata()));
        putLeaseFields(record, p.getLeaseState(), p.getLeaseStatus(), p.getLeaseDuration());
        record.put(HAS_IMMUTABILITY_POLICY, p.hasImmutabilityPolicy());
        record.put(HAS_LEGAL_HOLD, p.hasLegalHold());
        return record;
    }

    /** Maps the SDK access-policy pair to a {@code ContainerAccessPolicy} record. */
    public static BMap<BString, Object> containerAccessPolicy(BlobContainerAccessPolicies policies) {
        BMap<BString, Object> record = newRecord(RECORD_CONTAINER_ACCESS_POLICY);
        record.put(ACCESS, publicAccess(policies.getBlobAccessType()));
        BArray identifiers = recordArray(RECORD_SIGNED_IDENTIFIER);
        if (policies.getIdentifiers() != null) {
            for (BlobSignedIdentifier identifier : policies.getIdentifiers()) {
                BMap<BString, Object> entry = newRecord(RECORD_SIGNED_IDENTIFIER);
                entry.put(ID, StringUtils.fromString(identifier.getId()));
                if (identifier.getAccessPolicy() != null) {
                    if (identifier.getAccessPolicy().getStartsOn() != null) {
                        entry.put(START_TIME, ValueUtils.toUtc(identifier.getAccessPolicy().getStartsOn()));
                    }
                    if (identifier.getAccessPolicy().getExpiresOn() != null) {
                        entry.put(EXPIRY_TIME, ValueUtils.toUtc(identifier.getAccessPolicy().getExpiresOn()));
                    }
                    if (identifier.getAccessPolicy().getPermissions() != null) {
                        entry.put(PERMISSIONS, StringUtils.fromString(identifier.getAccessPolicy().getPermissions()));
                    }
                }
                identifiers.append(entry);
            }
        }
        record.put(IDENTIFIERS, identifiers);
        return record;
    }

    /** Maps one listed blob (or collapsed prefix) to a {@code BlobEntry} record. */
    public static BMap<BString, Object> blobEntry(BlobItem item) {
        BMap<BString, Object> record = newRecord(RECORD_BLOB_ENTRY);
        record.put(PATH, StringUtils.fromString(item.getName()));
        if (Boolean.TRUE.equals(item.isPrefix())) {
            record.put(IS_PREFIX, true);
            return record;
        }
        BlobItemProperties p = item.getProperties();
        if (p != null) {
            if (p.getContentLength() != null) {
                record.put(CONTENT_LENGTH, p.getContentLength());
            }
            if (p.getLastModified() != null) {
                record.put(LAST_MODIFIED, ValueUtils.toUtc(p.getLastModified()));
            }
            if (p.getETag() != null) {
                record.put(E_TAG, StringUtils.fromString(p.getETag()));
            }
            if (p.getBlobType() != null) {
                record.put(BLOB_TYPE, StringUtils.fromString(p.getBlobType().toString()));
            }
            if (p.getAccessTier() != null) {
                record.put(OptionsReader.ACCESS_TIER, StringUtils.fromString(p.getAccessTier().toString()));
            }
        }
        if (item.getMetadata() != null) {
            record.put(OptionsReader.METADATA, ValueUtils.toBStringMap(item.getMetadata()));
        }
        if (item.getTags() != null) {
            record.put(OptionsReader.TAGS, ValueUtils.toBStringMap(item.getTags()));
        }
        if (item.getSnapshot() != null) {
            record.put(OptionsReader.SNAPSHOT_ID, StringUtils.fromString(item.getSnapshot()));
        }
        if (item.isDeleted()) {
            record.put(IS_DELETED, true);
        }
        return record;
    }

    /** Builds a {@code BlobList} record; the marker is set only when more remain. */
    public static BMap<BString, Object> blobList(BArray blobs, String nextMarker) {
        BMap<BString, Object> record = newRecord(RECORD_BLOB_LIST);
        record.put(BLOBS, blobs);
        if (nextMarker != null && !nextMarker.isEmpty()) {
            record.put(NEXT_MARKER, StringUtils.fromString(nextMarker));
        }
        return record;
    }

    /** Maps SDK blob properties to a {@code BlobProperties} record. */
    public static BMap<BString, Object> blobProperties(BlobProperties p) {
        BMap<BString, Object> record = newRecord(RECORD_BLOB_PROPERTIES);
        record.put(LAST_MODIFIED, ValueUtils.toUtc(p.getLastModified()));
        record.put(CREATED_TIME, ValueUtils.toUtc(p.getCreationTime()));
        record.put(E_TAG, StringUtils.fromString(p.getETag()));
        record.put(CONTENT_LENGTH, p.getBlobSize());
        record.put(OptionsReader.CONTENT_HEADERS, contentHeaders(p));
        record.put(OptionsReader.METADATA,
                ValueUtils.toBStringMap(p.getMetadata() == null ? Map.of() : p.getMetadata()));
        record.put(BLOB_TYPE, StringUtils.fromString(p.getBlobType().toString()));
        if (p.getAccessTier() != null) {
            record.put(OptionsReader.ACCESS_TIER, StringUtils.fromString(p.getAccessTier().toString()));
            // The service sends the inferred flag only when it is true, so its absence on a
            // tiered blob means the tier was set explicitly.
            record.put(ACCESS_TIER_INFERRED, Boolean.TRUE.equals(p.isAccessTierInferred()));
        }
        if (p.getArchiveStatus() != null) {
            record.put(ARCHIVE_STATUS, StringUtils.fromString(p.getArchiveStatus().toString()));
        }
        putLeaseFields(record, p.getLeaseState(), p.getLeaseStatus(), p.getLeaseDuration());
        BMap<BString, Object> copy = copyStatusInfo(p);
        if (copy != null) {
            record.put(COPY_STATUS, copy);
        }
        if (p.getBlobSequenceNumber() != null) {
            record.put(BLOB_SEQUENCE_NUMBER, p.getBlobSequenceNumber());
        }
        if (p.getCommittedBlockCount() != null) {
            record.put(COMMITTED_BLOCK_COUNT, p.getCommittedBlockCount().longValue());
        }
        return record;
    }

    /**
     * Builds a {@code CopyStatusInfo} record from fetched blob properties, or {@code null} when the
     * blob has never been a copy destination.
     */
    public static BMap<BString, Object> copyStatusInfo(BlobProperties p) {
        if (p.getCopyId() == null || p.getCopyStatus() == null) {
            return null;
        }
        BMap<BString, Object> record = newRecord(RECORD_COPY_STATUS_INFO);
        record.put(COPY_ID, StringUtils.fromString(p.getCopyId()));
        record.put(COPY_STATUS, StringUtils.fromString(p.getCopyStatus().toString()));
        record.put(COPY_SOURCE, StringUtils.fromString(p.getCopySource() == null ? "" : p.getCopySource()));
        if (p.getCopyProgress() != null) {
            record.put(COPY_PROGRESS, StringUtils.fromString(p.getCopyProgress()));
        }
        if (p.getCopyCompletionTime() != null) {
            record.put(COPY_COMPLETION_TIME, ValueUtils.toUtc(p.getCopyCompletionTime()));
        }
        if (p.getCopyStatusDescription() != null) {
            record.put(COPY_STATUS_DESCRIPTION, StringUtils.fromString(p.getCopyStatusDescription()));
        }
        return record;
    }

    /** Maps the copy-acceptance response to a {@code CopyInfo} record. */
    public static BMap<BString, Object> copyInfo(BlobCopyInfo info) {
        BMap<BString, Object> record = newRecord(RECORD_COPY_INFO);
        record.put(COPY_ID, StringUtils.fromString(info.getCopyId()));
        record.put(COPY_STATUS, StringUtils.fromString(info.getCopyStatus().toString()));
        return record;
    }

    /** Maps one tag-query match to a {@code TaggedBlobEntry} record. */
    public static BMap<BString, Object> taggedBlobEntry(TaggedBlobItem item) {
        BMap<BString, Object> record = newRecord(RECORD_TAGGED_BLOB_ENTRY);
        record.put(PATH, StringUtils.fromString(item.getName()));
        record.put(OptionsReader.TAGS,
                ValueUtils.toBStringMap(item.getTags() == null ? Map.of() : item.getTags()));
        return record;
    }

    /** Maps one listed page range to a {@code PageRange} record. */
    public static BMap<BString, Object> pageRange(PageRangeItem item) {
        HttpRange range = item.getRange();
        BMap<BString, Object> record = newRecord(RECORD_PAGE_RANGE);
        record.put(OFFSET, range.getOffset());
        Long length = range.getLength();
        record.put(LENGTH, length == null ? Long.valueOf(0L) : length);
        return record;
    }

    /** Maps the SDK block listing to a {@code BlockList} record. */
    public static BMap<BString, Object> blockList(BlockList list) {
        BMap<BString, Object> record = newRecord(RECORD_BLOCK_LIST);
        record.put(COMMITTED_BLOCKS, blocks(list.getCommittedBlocks()));
        record.put(UNCOMMITTED_BLOCKS, blocks(list.getUncommittedBlocks()));
        return record;
    }

    private static BArray blocks(List<Block> blocks) {
        BArray array = recordArray(RECORD_BLOCK_INFO);
        if (blocks != null) {
            for (Block block : blocks) {
                BMap<BString, Object> entry = newRecord(RECORD_BLOCK_INFO);
                entry.put(BLOCK_ID, StringUtils.fromString(block.getName()));
                entry.put(SIZE_BYTES, block.getSizeLong());
                array.append(entry);
            }
        }
        return array;
    }

    /** Maps the SDK blob-service configuration to a {@code ServiceProperties} record. */
    public static BMap<BString, Object> serviceProperties(BlobServiceProperties sdk) {
        BMap<BString, Object> record = newRecord(RECORD_SERVICE_PROPERTIES);
        if (sdk.getHourMetrics() != null) {
            record.put(HOUR_METRICS, metrics(sdk.getHourMetrics()));
        }
        if (sdk.getMinuteMetrics() != null) {
            record.put(MINUTE_METRICS, metrics(sdk.getMinuteMetrics()));
        }
        BlobAnalyticsLogging logging = sdk.getLogging();
        if (logging != null) {
            BMap<BString, Object> loggingRecord = newRecord(RECORD_LOGGING_PROPERTIES);
            loggingRecord.put(VERSION,
                    StringUtils.fromString(logging.getVersion() == null ? "" : logging.getVersion()));
            loggingRecord.put(READ, logging.isRead());
            loggingRecord.put(WRITE, logging.isWrite());
            loggingRecord.put(DELETE, logging.isDelete());
            putRetentionPolicy(loggingRecord, RETENTION_POLICY, logging.getRetentionPolicy());
            record.put(LOGGING, loggingRecord);
        }
        if (sdk.getCors() != null) {
            BArray rules = recordArray(RECORD_CORS_RULE);
            for (BlobCorsRule rule : sdk.getCors()) {
                BMap<BString, Object> ruleRecord = newRecord(RECORD_CORS_RULE);
                ruleRecord.put(ALLOWED_ORIGINS, split(rule.getAllowedOrigins()));
                ruleRecord.put(ALLOWED_METHODS, split(rule.getAllowedMethods()));
                ruleRecord.put(ALLOWED_HEADERS, split(rule.getAllowedHeaders()));
                ruleRecord.put(EXPOSED_HEADERS, split(rule.getExposedHeaders()));
                ruleRecord.put(MAX_AGE_IN_SECONDS, (long) rule.getMaxAgeInSeconds());
                rules.append(ruleRecord);
            }
            record.put(CORS, rules);
        }
        putRetentionPolicy(record, DELETE_RETENTION_POLICY, sdk.getDeleteRetentionPolicy());
        StaticWebsite site = sdk.getStaticWebsite();
        if (site != null) {
            BMap<BString, Object> siteRecord = newRecord(RECORD_STATIC_WEBSITE_PROPERTIES);
            siteRecord.put(ENABLED, site.isEnabled());
            putString(siteRecord, INDEX_DOCUMENT, site.getIndexDocument());
            putString(siteRecord, ERROR_DOCUMENT_404_PATH, site.getErrorDocument404Path());
            putString(siteRecord, DEFAULT_INDEX_DOCUMENT_PATH, site.getDefaultIndexDocumentPath());
            record.put(STATIC_WEBSITE, siteRecord);
        }
        putString(record, DEFAULT_SERVICE_VERSION, sdk.getDefaultServiceVersion());
        return record;
    }

    private static BMap<BString, Object> metrics(BlobMetrics sdk) {
        BMap<BString, Object> record = newRecord(RECORD_METRICS_PROPERTIES);
        record.put(VERSION, StringUtils.fromString(sdk.getVersion() == null ? "" : sdk.getVersion()));
        record.put(ENABLED, sdk.isEnabled());
        if (sdk.isIncludeApis() != null) {
            record.put(INCLUDE_APIS, sdk.isIncludeApis());
        }
        putRetentionPolicy(record, RETENTION_POLICY, sdk.getRetentionPolicy());
        return record;
    }

    private static void putRetentionPolicy(BMap<BString, Object> record, BString field, BlobRetentionPolicy policy) {
        if (policy == null) {
            return;
        }
        BMap<BString, Object> policyRecord = newRecord(RECORD_RETENTION_POLICY);
        policyRecord.put(ENABLED, policy.isEnabled());
        if (policy.getDays() != null) {
            policyRecord.put(DAYS, policy.getDays().longValue());
        }
        record.put(field, policyRecord);
    }

    /** Maps the SDK account information to an {@code AccountInfo} record. */
    public static BMap<BString, Object> accountInfo(StorageAccountInfo info) {
        BMap<BString, Object> record = newRecord(RECORD_ACCOUNT_INFO);
        record.put(SKU_NAME, StringUtils.fromString(info.getSkuName() == null ? "" : info.getSkuName().toString()));
        record.put(ACCOUNT_KIND,
                StringUtils.fromString(info.getAccountKind() == null ? "" : info.getAccountKind().toString()));
        record.put(IS_HIERARCHICAL_NAMESPACE_ENABLED, info.isHierarchicalNamespaceEnabled());
        return record;
    }

    /** Maps the SDK user-delegation key to a {@code UserDelegationKey} record. */
    public static BMap<BString, Object> userDelegationKey(UserDelegationKey key) {
        BMap<BString, Object> record = newRecord(RECORD_USER_DELEGATION_KEY);
        record.put(SIGNED_OBJECT_ID, StringUtils.fromString(key.getSignedObjectId()));
        record.put(SIGNED_TENANT_ID, StringUtils.fromString(key.getSignedTenantId()));
        record.put(SIGNED_START, ValueUtils.toUtc(key.getSignedStart()));
        record.put(SIGNED_EXPIRY, ValueUtils.toUtc(key.getSignedExpiry()));
        record.put(SIGNED_SERVICE, StringUtils.fromString(key.getSignedService()));
        record.put(SIGNED_VERSION, StringUtils.fromString(key.getSignedVersion()));
        record.put(VALUE, StringUtils.fromString(key.getValue()));
        return record;
    }

    /** Creates an array value typed to the named module record. */
    public static BArray recordArray(String recordTypeName) {
        ArrayType arrayType = ARRAY_TYPES.computeIfAbsent(recordTypeName,
                name -> TypeCreator.createArrayType(TypeUtils.getType(newRecord(name))));
        return ValueCreator.createArrayValue(arrayType);
    }

    /** Creates an empty value of the named module record. */
    public static BMap<BString, Object> newRecord(String typeName) {
        return ValueCreator.createRecordValue(ModuleUtils.getModule(), typeName);
    }

    private static BMap<BString, Object> contentHeaders(BlobProperties p) {
        BMap<BString, Object> record = newRecord(RECORD_CONTENT_HEADERS);
        putString(record, OptionsReader.CONTENT_TYPE, p.getContentType());
        putString(record, OptionsReader.CONTENT_ENCODING, p.getContentEncoding());
        putString(record, OptionsReader.CONTENT_LANGUAGE, p.getContentLanguage());
        putString(record, OptionsReader.CONTENT_DISPOSITION, p.getContentDisposition());
        putString(record, OptionsReader.CACHE_CONTROL, p.getCacheControl());
        if (p.getContentMd5() != null) {
            record.put(OptionsReader.CONTENT_MD5,
                    StringUtils.fromString(Base64.getEncoder().encodeToString(p.getContentMd5())));
        }
        return record;
    }

    // The wire's absent public-access header is the PRIVATE enum member.
    private static BString publicAccess(PublicAccessType access) {
        return StringUtils.fromString(access == null ? OptionsReader.PUBLIC_ACCESS_PRIVATE : access.toString());
    }

    // The three lease fields are always present; the duration is nil while no lease is held.
    private static void putLeaseFields(BMap<BString, Object> record, LeaseStateType state,
                                       LeaseStatusType status, LeaseDurationType duration) {
        record.put(LEASE_STATE, StringUtils.fromString(
                state == null ? LeaseStateType.AVAILABLE.toString() : state.toString()));
        record.put(LEASE_STATUS, StringUtils.fromString(
                status == null ? LeaseStatusType.UNLOCKED.toString() : status.toString()));
        record.put(LEASE_DURATION, duration == null ? null : StringUtils.fromString(duration.toString()));
    }

    private static void putString(BMap<BString, Object> record, BString field, String value) {
        if (value != null) {
            record.put(field, StringUtils.fromString(value));
        }
    }

    // The wire carries the CORS lists as comma-separated strings; an empty string is no entries.
    private static BArray split(String joined) {
        BArray array = ValueCreator.createArrayValue(TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING));
        if (joined != null && !joined.isEmpty()) {
            for (String part : joined.split(",")) {
                array.append(StringUtils.fromString(part.trim()));
            }
        }
        return array;
    }
}
