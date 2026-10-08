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

import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobAccessPolicy;
import com.azure.storage.blob.models.BlobAnalyticsLogging;
import com.azure.storage.blob.models.BlobCorsRule;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobMetrics;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobRetentionPolicy;
import com.azure.storage.blob.models.BlobServiceProperties;
import com.azure.storage.blob.models.BlobSignedIdentifier;
import com.azure.storage.blob.models.PublicAccessType;
import com.azure.storage.blob.models.RehydratePriority;
import com.azure.storage.blob.models.StaticWebsite;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Reads the Ballerina option records into the SDK's option and property classes. Every reader
 * accepts the record as a {@code BMap} (or {@code null} for an omitted record) and tolerates
 * absent optional fields.
 */
public final class OptionsReader {

    // The Ballerina PublicAccess enum value that the wire carries as an absent header.
    public static final String PUBLIC_ACCESS_PRIVATE = "private";

    // Field names of the option and content-header records; the options schema other
    // classes reference.
    public static final BString PREFIX = StringUtils.fromString("prefix");
    public static final BString DELIMITER = StringUtils.fromString("delimiter");
    public static final BString INCLUDE_METADATA = StringUtils.fromString("includeMetadata");
    public static final BString INCLUDE_TAGS = StringUtils.fromString("includeTags");
    public static final BString INCLUDE_SNAPSHOTS = StringUtils.fromString("includeSnapshots");
    public static final BString INCLUDE_DELETED = StringUtils.fromString("includeDeleted");
    public static final BString PAGE_SIZE = StringUtils.fromString("pageSize");
    public static final BString LIMIT = StringUtils.fromString("limit");
    public static final BString MARKER = StringUtils.fromString("marker");
    public static final BString METADATA = StringUtils.fromString("metadata");
    public static final BString TAGS = StringUtils.fromString("tags");
    public static final BString PUBLIC_ACCESS = StringUtils.fromString("publicAccess");
    public static final BString LEASE_ID = StringUtils.fromString("leaseId");
    public static final BString DELETE_SNAPSHOTS = StringUtils.fromString("deleteSnapshots");
    public static final BString SNAPSHOT_ID = StringUtils.fromString("snapshotId");
    public static final BString ACCESS_TIER = StringUtils.fromString("accessTier");
    public static final BString REHYDRATE_PRIORITY = StringUtils.fromString("rehydratePriority");
    public static final BString CONTENT_HEADERS = StringUtils.fromString("contentHeaders");
    public static final BString RANGE = StringUtils.fromString("range");
    public static final BString SOURCE_RANGE = StringUtils.fromString("sourceRange");
    public static final BString FILE_FORMAT = StringUtils.fromString("fileFormat");
    public static final BString CONTENT_TYPE = StringUtils.fromString("contentType");
    public static final BString CONTENT_ENCODING = StringUtils.fromString("contentEncoding");
    public static final BString CONTENT_LANGUAGE = StringUtils.fromString("contentLanguage");
    public static final BString CONTENT_DISPOSITION = StringUtils.fromString("contentDisposition");
    public static final BString CACHE_CONTROL = StringUtils.fromString("cacheControl");
    public static final BString CONTENT_MD5 = StringUtils.fromString("contentMd5");
    public static final BString START_BYTE = StringUtils.fromString("startByte");
    public static final BString END_BYTE = StringUtils.fromString("endByte");

    private OptionsReader() {
    }

    /** Casts an options argument to its record, or {@code null} when the record was omitted. */
    @SuppressWarnings("unchecked")
    public static BMap<BString, Object> record(Object options) {
        return (BMap<BString, Object>) options;
    }

    /** Reads {@code leaseId} off an options record; {@code null} when absent or the record is omitted. */
    public static String leaseId(Object options) {
        return options == null ? null : ValueUtils.optString(record(options), LEASE_ID);
    }

    /** Builds the request conditions carrying {@code leaseId}; {@code null} when no lease is passed. */
    public static BlobRequestConditions leaseConditions(Object options) {
        String leaseId = leaseId(options);
        return leaseId == null ? null : new BlobRequestConditions().setLeaseId(leaseId);
    }

    /** Reads {@code metadata} off an options record; {@code null} when absent. */
    public static Map<String, String> metadata(Object options) {
        return options == null ? null : ValueUtils.optStringMap(record(options), METADATA);
    }

    /** Reads {@code tags} off an options record; {@code null} when absent. */
    public static Map<String, String> tags(Object options) {
        return options == null ? null : ValueUtils.optStringMap(record(options), TAGS);
    }

    /** Reads {@code snapshotId} off an options record; {@code null} when absent. */
    public static String snapshotId(Object options) {
        return options == null ? null : ValueUtils.optString(record(options), SNAPSHOT_ID);
    }

    /** Reads {@code accessTier} off an options record; {@code null} when absent. */
    public static AccessTier accessTier(Object options) {
        String tier = options == null ? null : ValueUtils.optString(record(options), ACCESS_TIER);
        return tier == null ? null : AccessTier.fromString(tier);
    }

    /** Reads {@code rehydratePriority} off an options record; {@code null} when absent. */
    public static RehydratePriority rehydratePriority(Object options) {
        String priority = options == null ? null : ValueUtils.optString(record(options), REHYDRATE_PRIORITY);
        return priority == null ? null : RehydratePriority.fromString(priority);
    }

    /** Reads {@code contentHeaders} off an options record; {@code null} when absent. */
    public static BlobHttpHeaders contentHeadersOf(Object options) {
        return options == null ? null : contentHeaders(record(options).get(CONTENT_HEADERS));
    }

    /**
     * Converts a {@code PublicAccess} enum value to the SDK type; {@code PRIVATE} (and an absent
     * value) is the wire's absent header, {@code null}.
     */
    public static PublicAccessType publicAccess(Object value) {
        if (value == null) {
            return null;
        }
        String access = ((BString) value).getValue();
        return PUBLIC_ACCESS_PRIVATE.equals(access) ? null : PublicAccessType.fromString(access);
    }

    /** Converts a {@code ContentHeaders} record to the SDK header class; {@code null} when absent. */
    public static BlobHttpHeaders contentHeaders(Object value) {
        if (value == null) {
            return null;
        }
        BMap<BString, Object> record = record(value);
        BlobHttpHeaders headers = new BlobHttpHeaders()
                .setContentType(ValueUtils.optString(record, CONTENT_TYPE))
                .setContentEncoding(ValueUtils.optString(record, CONTENT_ENCODING))
                .setContentLanguage(ValueUtils.optString(record, CONTENT_LANGUAGE))
                .setContentDisposition(ValueUtils.optString(record, CONTENT_DISPOSITION))
                .setCacheControl(ValueUtils.optString(record, CACHE_CONTROL));
        String md5 = ValueUtils.optString(record, CONTENT_MD5);
        if (md5 != null) {
            try {
                headers.setContentMd5(Base64.getDecoder().decode(md5));
            } catch (IllegalArgumentException e) {
                throw BlobErrorCreator.clientError("contentMd5 must be base64-encoded", e);
            }
        }
        return headers;
    }

    /**
     * Converts a {@code ByteRange} record (inclusive bounds) to the SDK range (offset + count);
     * {@code null} when absent.
     */
    public static BlobRange range(Object value) {
        if (value == null) {
            return null;
        }
        BMap<BString, Object> record = record(value);
        long start = (Long) record.get(START_BYTE);
        long end = (Long) record.get(END_BYTE);
        if (start < 0 || end < start) {
            throw BlobErrorCreator.clientError(
                    "invalid byte range: startByte must be non-negative and endByte at least startByte", null);
        }
        return new BlobRange(start, end - start + 1);
    }

    /** Converts a {@code SignedIdentifier[]} array to the SDK stored access policies. */
    public static List<BlobSignedIdentifier> signedIdentifiers(BArray identifiers) {
        List<BlobSignedIdentifier> result = new ArrayList<>();
        for (int i = 0; i < identifiers.size(); i++) {
            BMap<BString, Object> record = record(identifiers.get(i));
            BlobAccessPolicy policy = new BlobAccessPolicy()
                    .setPermissions(ValueUtils.optString(record, RecordMapper.PERMISSIONS));
            Object startTime = record.get(RecordMapper.START_TIME);
            if (startTime != null) {
                policy.setStartsOn(ValueUtils.fromUtc((BArray) startTime));
            }
            Object expiryTime = record.get(RecordMapper.EXPIRY_TIME);
            if (expiryTime != null) {
                policy.setExpiresOn(ValueUtils.fromUtc((BArray) expiryTime));
            }
            result.add(new BlobSignedIdentifier()
                    .setId(record.getStringValue(RecordMapper.ID).getValue())
                    .setAccessPolicy(policy));
        }
        return result;
    }

    /**
     * Converts a {@code ServiceProperties} record to the SDK model. A group absent from the
     * record is left {@code null}, which the service reads as "leave unchanged".
     */
    public static BlobServiceProperties serviceProperties(BMap<BString, Object> record) {
        BlobServiceProperties sdk = new BlobServiceProperties();
        Object hourMetrics = record.get(RecordMapper.HOUR_METRICS);
        if (hourMetrics != null) {
            sdk.setHourMetrics(metrics(record(hourMetrics)));
        }
        Object minuteMetrics = record.get(RecordMapper.MINUTE_METRICS);
        if (minuteMetrics != null) {
            sdk.setMinuteMetrics(metrics(record(minuteMetrics)));
        }
        Object logging = record.get(RecordMapper.LOGGING);
        if (logging != null) {
            BMap<BString, Object> loggingRecord = record(logging);
            sdk.setLogging(new BlobAnalyticsLogging()
                    .setVersion(loggingRecord.getStringValue(RecordMapper.VERSION).getValue())
                    .setRead(loggingRecord.getBooleanValue(RecordMapper.READ))
                    .setWrite(loggingRecord.getBooleanValue(RecordMapper.WRITE))
                    .setDelete(loggingRecord.getBooleanValue(RecordMapper.DELETE))
                    .setRetentionPolicy(retentionPolicy(loggingRecord.get(RecordMapper.RETENTION_POLICY))));
        }
        Object cors = record.get(RecordMapper.CORS);
        if (cors != null) {
            List<BlobCorsRule> rules = new ArrayList<>();
            BArray ruleArray = (BArray) cors;
            for (int i = 0; i < ruleArray.size(); i++) {
                BMap<BString, Object> rule = record(ruleArray.get(i));
                rules.add(new BlobCorsRule()
                        .setAllowedOrigins(joined(rule.get(RecordMapper.ALLOWED_ORIGINS)))
                        .setAllowedMethods(joined(rule.get(RecordMapper.ALLOWED_METHODS)))
                        .setAllowedHeaders(joined(rule.get(RecordMapper.ALLOWED_HEADERS)))
                        .setExposedHeaders(joined(rule.get(RecordMapper.EXPOSED_HEADERS)))
                        .setMaxAgeInSeconds(Math.toIntExact((Long) rule.get(RecordMapper.MAX_AGE_IN_SECONDS))));
            }
            sdk.setCors(rules);
        }
        Object deleteRetention = record.get(RecordMapper.DELETE_RETENTION_POLICY);
        if (deleteRetention != null) {
            sdk.setDeleteRetentionPolicy(retentionPolicy(deleteRetention));
        }
        Object staticWebsite = record.get(RecordMapper.STATIC_WEBSITE);
        if (staticWebsite != null) {
            BMap<BString, Object> site = record(staticWebsite);
            sdk.setStaticWebsite(new StaticWebsite()
                    .setEnabled(site.getBooleanValue(RecordMapper.ENABLED))
                    .setIndexDocument(ValueUtils.optString(site, RecordMapper.INDEX_DOCUMENT))
                    .setErrorDocument404Path(ValueUtils.optString(site, RecordMapper.ERROR_DOCUMENT_404_PATH))
                    .setDefaultIndexDocumentPath(ValueUtils.optString(site, RecordMapper.DEFAULT_INDEX_DOCUMENT_PATH)));
        }
        sdk.setDefaultServiceVersion(ValueUtils.optString(record, RecordMapper.DEFAULT_SERVICE_VERSION));
        return sdk;
    }

    private static BlobMetrics metrics(BMap<BString, Object> record) {
        BlobMetrics sdk = new BlobMetrics()
                .setVersion(record.getStringValue(RecordMapper.VERSION).getValue())
                .setEnabled(record.getBooleanValue(RecordMapper.ENABLED));
        Object includeApis = record.get(RecordMapper.INCLUDE_APIS);
        if (includeApis != null) {
            sdk.setIncludeApis((Boolean) includeApis);
        }
        sdk.setRetentionPolicy(retentionPolicy(record.get(RecordMapper.RETENTION_POLICY)));
        return sdk;
    }

    // An absent policy is sent as disabled: the wire requires the element inside a metrics or
    // logging group, and disabled is what "no retention" means there.
    private static BlobRetentionPolicy retentionPolicy(Object value) {
        BlobRetentionPolicy sdk = new BlobRetentionPolicy();
        if (value == null) {
            return sdk.setEnabled(false);
        }
        BMap<BString, Object> record = record(value);
        sdk.setEnabled(record.getBooleanValue(RecordMapper.ENABLED));
        Object days = record.get(RecordMapper.DAYS);
        if (days != null) {
            sdk.setDays(Math.toIntExact((Long) days));
        }
        return sdk;
    }

    // The wire carries the CORS lists as comma-separated strings.
    private static String joined(Object stringArray) {
        return String.join(",", ((BArray) stringArray).getStringArray());
    }
}
