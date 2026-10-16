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

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerListDetails;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.OptionsReader;
import io.ballerina.lib.azure.storage.blob.util.RecordMapper;
import io.ballerina.lib.azure.storage.blob.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.Iterator;

/**
 * Native backing of the listings: the account's containers (whole or one page), the bound
 * container's blobs as a lazy stream, and one page of blobs with a resume marker.
 */
public final class ListOps {

    // Key under which the native iterator state is stored on a stream generator object.
    private static final String NATIVE_ITERATOR = "blobIterator";

    private ListOps() {
    }

    /** Lists containers: every container without a limit, or one page of up to {@code 'limit}. */
    public static Object listContainers(Environment env, BObject self, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            ListBlobContainersOptions sdkOptions = new ListBlobContainersOptions();
            Integer limit = null;
            String marker = null;
            if (record != null) {
                sdkOptions.setPrefix(ValueUtils.optString(record, OptionsReader.PREFIX))
                        .setDetails(new BlobContainerListDetails()
                                .setRetrieveMetadata(record.getBooleanValue(OptionsReader.INCLUDE_METADATA))
                                .setRetrieveDeleted(record.getBooleanValue(OptionsReader.INCLUDE_DELETED)));
                Object limitValue = record.get(OptionsReader.LIMIT);
                limit = limitValue == null ? null : Math.toIntExact((Long) limitValue);
                marker = ValueUtils.optString(record, OptionsReader.MARKER);
                sdkOptions.setMaxResultsPerPage(limit);
            }
            PagedIterable<BlobContainerItem> iterable =
                    BallerinaAzureClient.getServiceClient(self).listBlobContainers(sdkOptions, null);
            BArray containers = RecordMapper.recordArray(RecordMapper.RECORD_CONTAINER_INFO);
            if (limit == null) {
                // Without a limit the whole listing is returned, from the marker when one is given.
                Iterable<PagedResponse<BlobContainerItem>> pages =
                        marker == null ? iterable.iterableByPage() : iterable.iterableByPage(marker);
                for (PagedResponse<BlobContainerItem> page : pages) {
                    for (BlobContainerItem item : page.getValue()) {
                        containers.append(RecordMapper.containerInfo(item));
                    }
                }
                return RecordMapper.containerList(containers, null);
            }
            PagedResponse<BlobContainerItem> page = firstPage(iterable, marker, limit);
            if (page != null) {
                for (BlobContainerItem item : page.getValue()) {
                    containers.append(RecordMapper.containerInfo(item));
                }
            }
            return RecordMapper.containerList(containers, page == null ? null : page.getContinuationToken());
        });
    }

    /**
     * Attaches a fresh blob listing iterator to the Ballerina stream generator object. Runs off
     * the scheduler because the SDK's paged iterable fetches its first page eagerly.
     */
    public static Object newBlobIterator(Environment env, BObject self, BObject generator, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            generator.addNativeData(NATIVE_ITERATOR, listing(self, options, null, null).iterator());
            return null;
        });
    }

    /** Pulls the next listed blob: a {@code BlobEntry} record, {@code null} at the end, or an error. */
    public static Object nextBlobEntry(Environment env, BObject generator) {
        return BallerinaAzureClient.invoke(env, () -> {
            @SuppressWarnings("unchecked")
            Iterator<BlobItem> iterator = (Iterator<BlobItem>) generator.getNativeData(NATIVE_ITERATOR);
            if (iterator == null || !iterator.hasNext()) {
                return null;
            }
            return RecordMapper.blobEntry(iterator.next());
        });
    }

    /** Stops an in-progress listing early. */
    public static Object closeBlobIterator(BObject generator) {
        generator.addNativeData(NATIVE_ITERATOR, null);
        return null;
    }

    /** Lists one page of the bound container's blobs, with the marker to resume from. */
    public static Object listBlobsPage(Environment env, BObject self, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            BMap<BString, Object> record = OptionsReader.record(options);
            Integer pageSize = null;
            String marker = null;
            if (record != null) {
                Object pageSizeValue = record.get(OptionsReader.PAGE_SIZE);
                pageSize = pageSizeValue == null ? null : Math.toIntExact((Long) pageSizeValue);
                marker = ValueUtils.optString(record, OptionsReader.MARKER);
            }
            PagedResponse<BlobItem> page = firstPage(listing(self, options, marker, pageSize), marker, pageSize);
            BArray blobs = RecordMapper.recordArray(RecordMapper.RECORD_BLOB_ENTRY);
            if (page != null) {
                for (BlobItem item : page.getValue()) {
                    blobs.append(RecordMapper.blobEntry(item));
                }
            }
            return RecordMapper.blobList(blobs, page == null ? null : page.getContinuationToken());
        });
    }

    // Builds the SDK listing for the bound container: flat, or hierarchical when a delimiter is set.
    private static PagedIterable<BlobItem> listing(BObject self, Object options, String marker, Integer pageSize) {
        BlobContainerClient container = BallerinaAzureClient.getContainerClient(self);
        ListBlobsOptions sdkOptions = new ListBlobsOptions().setMaxResultsPerPage(pageSize);
        String delimiter = null;
        BMap<BString, Object> record = OptionsReader.record(options);
        if (record != null) {
            delimiter = ValueUtils.optString(record, OptionsReader.DELIMITER);
            sdkOptions.setPrefix(ValueUtils.optString(record, OptionsReader.PREFIX))
                    .setDetails(new BlobListDetails()
                            .setRetrieveMetadata(record.getBooleanValue(OptionsReader.INCLUDE_METADATA))
                            .setRetrieveTags(record.getBooleanValue(OptionsReader.INCLUDE_TAGS))
                            .setRetrieveSnapshots(record.getBooleanValue(OptionsReader.INCLUDE_SNAPSHOTS))
                            .setRetrieveDeletedBlobs(record.getBooleanValue(OptionsReader.INCLUDE_DELETED)));
        }
        if (delimiter != null) {
            return container.listBlobsByHierarchy(delimiter, sdkOptions, null);
        }
        return container.listBlobs(sdkOptions, marker, null);
    }

    // The first page of a listing, resumed from a marker when one is given.
    private static <T> PagedResponse<T> firstPage(PagedIterable<T> iterable, String marker, Integer pageSize) {
        Iterable<PagedResponse<T>> pages;
        if (marker == null) {
            pages = pageSize == null ? iterable.iterableByPage() : iterable.iterableByPage(pageSize);
        } else {
            pages = pageSize == null ? iterable.iterableByPage(marker) : iterable.iterableByPage(marker, pageSize);
        }
        Iterator<PagedResponse<T>> iterator = pages.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }
}
