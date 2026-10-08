// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# Container-scoped client for Azure Blob Storage, bound to one container at initialization. A
# blob is addressed by its container-relative path, such as `2026/07/invoice.pdf`.
public isolated client class Client {

    # Initializes the client and binds it to a container. No call is made to Azure; a container
    # that does not exist fails on the first operation with a `NotFoundError`.
    #
    # + containerName - The name of the container to bind to
    # + config - The client configuration (authentication, retry, transport)
    # + return - An `Error` if the client could not be initialized, otherwise `()`
    public isolated function init(string containerName, *ClientConfiguration config) returns Error? {
        return initClient(self, containerName, config);
    }

    // -----------------------------------------------------------------------
    // Container operations
    // -----------------------------------------------------------------------

    # Reads the bound container's properties and metadata.
    #
    # + return - The `ContainerProperties`, or an `Error`
    isolated remote function getContainerProperties() returns ContainerProperties|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ContainerOps"
    } external;

    # Replaces the container's complete metadata set. Any key absent from `metadata` is removed.
    #
    # + metadata - The new complete metadata set; pass `{}` to clear it
    # + options - Optional options (the lease id, when the container is leased)
    # + return - An `Error` if the metadata could not be set, otherwise `()`
    isolated remote function setContainerMetadata(map<string> metadata,
            ContainerMetadataOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ContainerOps"
    } external;

    # Reads the container's anonymous access level and its stored access policies together.
    #
    # + return - The `ContainerAccessPolicy`, or an `Error`
    isolated remote function getContainerAccessPolicy() returns ContainerAccessPolicy|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ContainerOps"
    } external;

    # Sets the container's anonymous access level. The stored access policies are left unchanged.
    #
    # + access - The anonymous access level to apply
    # + options - Optional options (the lease id, when the container is leased)
    # + return - An `Error` if the access level could not be set, otherwise `()`
    isolated remote function setPublicAccess(PublicAccess access, AccessPolicyOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ContainerOps"
    } external;

    # Replaces the container's stored access policies. The anonymous access level is left unchanged.
    #
    # + identifiers - The new complete policy set, at most five; pass `[]` to remove them all
    # + options - Optional options (the lease id, when the container is leased)
    # + return - An `Error` if the policies could not be set, otherwise `()`
    isolated remote function setContainerAccessPolicy(SignedIdentifier[] identifiers,
            AccessPolicyOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ContainerOps"
    } external;

    // -----------------------------------------------------------------------
    // Blob operations
    // -----------------------------------------------------------------------

    # Lists the container's blobs as a lazy stream.
    #
    # + options - Optional listing options (prefix, delimiter, inclusion toggles)
    # + return - A stream of `BlobEntry`, or an `Error`
    isolated remote function listBlobs(BlobListOptions? options = ())
            returns stream<BlobEntry, Error?>|Error {
        BlobEntryStreamGenerator generator = new;
        check newBlobIterator(self, generator, options);
        return new stream<BlobEntry, Error?>(generator);
    }

    # Lists one page of the container's blobs, with the marker to resume from.
    #
    # + options - Optional listing options, plus the page size and marker
    # + return - The `BlobList`, or an `Error`
    isolated remote function listBlobsPage(BlobPageOptions? options = ())
            returns BlobList|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ListOps"
    } external;

    # Checks whether a blob exists. Returns `false` only when Azure confirms the blob is
    # absent; an `Error` means the check itself failed.
    #
    # + path - The container-relative path of the blob
    # + return - `true` if the blob exists, `false` if not, or an `Error`
    isolated remote function hasBlob(string path) returns boolean|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    # Deletes a blob, or one of its snapshots. A blob that has snapshots cannot be deleted
    # without directing what happens to them.
    #
    # + path - The container-relative path of the blob
    # + options - Optional deletion options (snapshot handling, a snapshot id, the lease id)
    # + return - An `Error` if the blob could not be deleted, otherwise `()`
    isolated remote function deleteBlob(string path, DeleteBlobOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    # Restores a soft-deleted blob, together with any soft-deleted snapshots it had. Find
    # restorable blobs with `listBlobs({includeDeleted: true})`.
    #
    # + path - The container-relative path of the deleted blob
    # + return - An `Error` if the blob could not be restored, otherwise `()`
    isolated remote function undeleteBlob(string path) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    # Reads a blob's properties, metadata, and the state of its most recent copy operation.
    # This is also how a pending copy is watched.
    #
    # + path - The container-relative path of the blob
    # + return - The `BlobProperties`, or an `Error`
    isolated remote function getBlobProperties(string path) returns BlobProperties|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    # Replaces a blob's complete metadata set. Any key absent from `metadata` is removed.
    #
    # + path - The container-relative path of the blob
    # + metadata - The new complete metadata set; pass `{}` to clear it
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the metadata could not be set, otherwise `()`
    isolated remote function setBlobMetadata(string path, map<string> metadata,
            BlobMetadataOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    # Replaces a blob's complete content-header set. The wire applies the set as a whole, so
    # any header omitted from the record is cleared on the blob. Metadata is untouched.
    #
    # + path - The container-relative path of the blob
    # + headers - The new complete content-header set
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the headers could not be set, otherwise `()`
    isolated remote function setContentHeaders(string path, ContentHeaders headers,
            ContentHeaderOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlobOps"
    } external;

    // -----------------------------------------------------------------------
    // Transfer operations
    // -----------------------------------------------------------------------

    # Uploads a local file as a block blob.
    #
    # ```ballerina
    # // ./reports/q1.pdf (local disk) --> 2026/q1/report.pdf (in the container)
    # check blobClient->uploadFromFile("./reports/q1.pdf", "2026/q1/report.pdf");
    # ```
    #
    # + sourcePath - The local file to upload, including its name
    # + destinationPath - The container-relative path to write, including the blob's name
    # + options - Optional upload options (content headers, metadata, tags, tier, lease id)
    # + return - An `Error` if the upload failed, otherwise `()`
    isolated remote function uploadFromFile(string sourcePath, string destinationPath,
            UploadOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
    } external;

    # Uploads in-memory content to the bound container.
    #
    # ```ballerina
    # Metrics metrics = {revenue: 1250000, growth: 0.12};
    # check blobClient->upload(metrics, "2026/q1/metrics.json");
    # ```
    #
    # + content - The content to upload. A record, a record array, or another `json` value is
    #             serialized per the resolved format; `byte[]` and `string` are written as-is
    # + destinationPath - The container-relative path, including the blob name
    # + options - Optional upload options (headers, metadata, format override)
    # + return - An `Error` if the upload failed, otherwise `()`
    isolated remote function upload(UploadContent content, string destinationPath,
            UploadContentOptions? options = ()) returns Error? {
        if content is stream<byte[], error?> {
            return self.uploadByteStream(content, destinationPath, options);
        }
        if content is stream<record {}, error?> {
            return self.uploadRecordStream(content, destinationPath, options);
        }
        byte[]|string|xml payload;
        FileFormat? appliedFormat = ();
        if content is record {} {
            [payload, appliedFormat] = check serializeRecord(content, destinationPath, options?.fileFormat);
        } else if content is record {}[] {
            payload = check serializeRecordArray(content, destinationPath, options?.fileFormat);
            appliedFormat = CSV;
        } else if content is byte[]|string {
            payload = content;
        } else if content is xml {
            if resolveUploadFormat(destinationPath, options?.fileFormat) !is XML {
                return error Error("xml content requires a '.xml' extension in the destination path or an explicit "
                        + "XML fileFormat");
            }
            payload = content;
            appliedFormat = XML;
        } else {
            // The compiler does not subtract the record shapes from the union here, but
            // both are handled above, so the residual json value is cast-safe.
            [payload, appliedFormat] = check serializeJson(<json>content, destinationPath, options?.fileFormat);
        }
        return externUpload(self, payload, destinationPath, options, appliedFormat);
    }

    // Stages the source's bytes as blocks of the stream block size and commits them at the end.
    // Source chunks coalesce into full blocks, so the request count tracks the content size
    // rather than the source's chunking; a chunk larger than a block is carried over in slices.
    private isolated function uploadByteStream(stream<byte[], error?> content, string destinationPath,
            UploadContentOptions? options) returns Error? {
        Error? result = self.stageByteStream(content, destinationPath, options);
        if result is Error {
            closeByteStreamQuietly(content);
        }
        return result;
    }

    private isolated function stageByteStream(stream<byte[], error?> content, string destinationPath,
            UploadContentOptions? options) returns Error? {
        string uploadId = newStreamUploadId();
        byte[] buffer = [];
        byte[] carry = [];
        int blockCount = 0;
        while true {
            byte[] bytes;
            if carry.length() > 0 {
                bytes = carry;
                carry = [];
            } else {
                ContentStreamEntry|error? chunk = content.next();
                if chunk is () {
                    break;
                }
                if chunk is error {
                    return error Error("the source stream failed: " + chunk.message(), chunk);
                }
                bytes = chunk.value;
            }
            int room = STREAM_BLOCK_BYTES - buffer.length();
            if bytes.length() > room {
                carry = bytes.slice(room);
                bytes = bytes.slice(0, room);
            }
            // Copied, never aliased: a source may hand out readonly chunks, and the buffer grows.
            buffer.push(...bytes);
            if buffer.length() >= STREAM_BLOCK_BYTES {
                check stageStreamBlock(self, destinationPath, uploadId, blockCount, buffer, options);
                blockCount += 1;
                buffer = [];
            }
        }
        if buffer.length() > 0 {
            check stageStreamBlock(self, destinationPath, uploadId, blockCount, buffer, options);
            blockCount += 1;
        }
        return commitStreamBlocks(self, destinationPath, uploadId, blockCount, options, ());
    }

    // Writes CSV rows as they are pulled: the first record's field names form the header, and
    // a later record's extra field is not added to a header already written.
    private isolated function uploadRecordStream(stream<record {}, error?> content, string destinationPath,
            UploadContentOptions? options) returns Error? {
        if resolveUploadFormat(destinationPath, options?.fileFormat) !is CSV {
            closeRecordStreamQuietly(content);
            return error Error("a record stream requires CSV format: use a '.csv' destination path "
                    + "or an explicit fileFormat");
        }
        Error? result = self.stageRecordStream(content, destinationPath, options);
        if result is Error {
            closeRecordStreamQuietly(content);
        }
        return result;
    }

    private isolated function stageRecordStream(stream<record {}, error?> content, string destinationPath,
            UploadContentOptions? options) returns Error? {
        string uploadId = newStreamUploadId();
        string[]? header = ();
        string buffer = "";
        int blockCount = 0;
        while true {
            record {|record {} value;|}|error? entry = content.next();
            if entry is () {
                break;
            }
            if entry is error {
                return error Error("the source stream failed: " + entry.message(), entry);
            }
            string[] columns;
            if header is string[] {
                columns = header;
                buffer += "\n";
            } else {
                columns = entry.value.keys();
                header = columns;
                buffer += csvRow(columns) + "\n";
            }
            buffer += csvRow(cells(entry.value, columns));
            if buffer.length() >= STREAM_BLOCK_BYTES {
                check stageStreamBlock(self, destinationPath, uploadId, blockCount, buffer.toBytes(), options);
                blockCount += 1;
                buffer = "";
            }
        }
        if buffer.length() > 0 {
            check stageStreamBlock(self, destinationPath, uploadId, blockCount, buffer.toBytes(), options);
            blockCount += 1;
        }
        return commitStreamBlocks(self, destinationPath, uploadId, blockCount, options, CSV);
    }

    # Downloads a blob to a local file. Fails if a local file already exists at the destination.
    #
    # ```ballerina
    # // 2026/q1/report.pdf (in the container) --> ./downloads/report.pdf (local disk)
    # check blobClient->download("2026/q1/report.pdf", "./downloads/report.pdf");
    # ```
    #
    # + sourcePath - The container-relative path of the blob
    # + destinationPath - The local path to write, including the file name
    # + options - Optional download options (a byte range, a snapshot id)
    # + return - An `Error` if the download failed, otherwise `()`
    isolated remote function download(string sourcePath, string destinationPath,
            DownloadOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
    } external;

    # Retrieves a blob's content in the form the target type selects. Binding is strict: content
    # that does not match the target fails with a client-side `Error`.
    #
    # ```ballerina
    # byte[] raw = check blobClient->getBlob("2026/q1/report.pdf");
    # Person[] people = check blobClient->getBlob("2026/q1/people.csv");
    # stream<byte[], error?> chunks = check blobClient->getBlob("2026/q1/large.bin");
    # ```
    #
    # + path - The container-relative path of the blob
    # + options - Optional retrieval options (a byte range, a snapshot id, the binding format)
    # + targetType - Expected return type (to be used for automatic data binding).
    #                Supported types:
    #                - Raw bytes (`byte[]`) or UTF-8 text (`string`)
    #                - A `json` or `xml` value
    #                - Custom records (e.g., `Person`, `Person[]`), bound per `GetBlobOptions.fileFormat`,
    #                  else the path's extension (`.json`, `.xml`, `.csv`)
    #                - A lazy byte stream (`stream<byte[], error?>`)
    #                - A lazy stream of CSV-bound records (e.g., `stream<Person, error?>`)
    # + return - The content in the requested form, or an `Error` on a failed retrieval or a data binding failure
    isolated remote function getBlob(string path, GetBlobOptions? options = (),
            typedesc<RetrievableType> targetType = <>) returns targetType|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TypedReadOps"
    } external;

    // -----------------------------------------------------------------------
    // Copy operations
    // -----------------------------------------------------------------------

    # Copies a blob within the bound container. The copy is asynchronous; watch it with
    # `getBlobProperties` or cancel it with `abortCopy`.
    #
    # + sourcePath - The container-relative path of the source blob
    # + destinationPath - The container-relative path of the destination blob
    # + options - Optional copy options (destination metadata, tags, tier, lease id)
    # + return - The `CopyInfo` for the accepted copy, or an `Error`
    isolated remote function copyBlob(string sourcePath, string destinationPath,
            CopyOptions? options = ()) returns CopyInfo|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.CopyOps"
    } external;

    # Copies into the bound container from any Azure Storage URL the service can read. The copy
    # is asynchronous.
    #
    # + sourceUrl - The URL of the source. A source outside this storage account carries its own
    #               authorization, typically a SAS token
    # + destinationPath - The container-relative path of the destination blob
    # + options - Optional copy options (destination metadata, tags, tier, lease id)
    # + return - The `CopyInfo` for the accepted copy, or an `Error`
    isolated remote function copyBlobFromUrl(string sourceUrl, string destinationPath,
            CopyOptions? options = ()) returns CopyInfo|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.CopyOps"
    } external;

    # Cancels a pending copy.
    #
    # + path - The container-relative path of the destination blob
    # + copyId - The identifier from the `CopyInfo` the copy returned
    # + options - Optional options (the lease id, when the destination is leased)
    # + return - An `Error` if the copy could not be aborted, otherwise `()`
    isolated remote function abortCopy(string path, string copyId, AbortCopyOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.CopyOps"
    } external;

    // -----------------------------------------------------------------------
    // Access tier operations
    // -----------------------------------------------------------------------

    # Moves a blob to another access tier. Moving an archived blob to an online tier starts a
    # rehydration that takes hours.
    #
    # + path - The container-relative path of the blob
    # + accessTier - The tier to move the blob to
    # + options - Optional options (the rehydration priority, the lease id)
    # + return - An `Error` if the tier could not be set, otherwise `()`
    isolated remote function setAccessTier(string path, AccessTier accessTier,
            SetAccessTierOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TierOps"
    } external;

    // -----------------------------------------------------------------------
    // Blob index tag operations
    // -----------------------------------------------------------------------

    # Replaces a blob's complete index-tag set, at most ten tags. Note the wire's exception to
    # the usual lease rule: a missing lease id here fails with an `AuthorizationError`.
    #
    # + path - The container-relative path of the blob
    # + tags - The new complete tag set; pass `{}` to clear it
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the tags could not be set, otherwise `()`
    isolated remote function setTags(string path, map<string> tags, TagOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TagOps"
    } external;

    # Reads a blob's index tags.
    #
    # + path - The container-relative path of the blob
    # + return - The blob's index tags, empty when it has none, or an `Error`
    isolated remote function getTags(string path) returns map<string>|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.TagOps"
    } external;

    # Finds the bound container's blobs whose index tags match a query. The query is validated
    # locally before any request is made.
    #
    # + query - The filter expression, such as `"status" = 'done' AND "priority" >= '05'`
    # + return - A stream of `TaggedBlobEntry`, or an `Error`
    isolated remote function findBlobsByTags(string query)
            returns stream<TaggedBlobEntry, Error?>|Error {
        TaggedBlobStreamGenerator generator = new;
        check newTaggedBlobIterator(self, generator, query);
        return new stream<TaggedBlobEntry, Error?>(generator);
    }

    // -----------------------------------------------------------------------
    // Snapshot operations
    // -----------------------------------------------------------------------

    # Creates a point-in-time, read-only copy of a blob. Read the snapshot by passing its
    # identifier in the options of `getBlob` or `download`, and delete it through `deleteBlob`.
    #
    # + path - The container-relative path of the blob
    # + options - Optional options (the snapshot's metadata, the lease id)
    # + return - The snapshot identifier, or an `Error`
    isolated remote function createSnapshot(string path, CreateSnapshotOptions? options = ())
            returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SnapshotOps"
    } external;

    // -----------------------------------------------------------------------
    // Lease operations
    // -----------------------------------------------------------------------

    # Acquires a lease on a blob, locking it against writes and deletion by others. The duration
    # is validated locally.
    #
    # + path - The container-relative path of the blob
    # + leaseDurationSeconds - 15 to 60 for a fixed lease, or -1 for an infinite one
    # + proposedLeaseId - A lease id to use; omit and the service generates one
    # + return - The lease id, or an `Error`
    isolated remote function acquireLease(string path, int leaseDurationSeconds,
            string? proposedLeaseId = ()) returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.LeaseOps"
    } external;

    # Renews a fixed-duration lease, restarting its clock.
    #
    # + path - The container-relative path of the blob
    # + leaseId - The current lease id
    # + return - An `Error` if the lease could not be renewed, otherwise `()`
    isolated remote function renewLease(string path, string leaseId) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.LeaseOps"
    } external;

    # Releases a lease, making the blob immediately available to other clients.
    #
    # + path - The container-relative path of the blob
    # + leaseId - The current lease id
    # + return - An `Error` if the lease could not be released, otherwise `()`
    isolated remote function releaseLease(string path, string leaseId) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.LeaseOps"
    } external;

    # Breaks a lease without holding its id. With no break period a fixed lease runs out its
    # remaining time and an infinite lease breaks immediately.
    #
    # + path - The container-relative path of the blob
    # + breakPeriodSeconds - How long the lease keeps running before it breaks, 0 to 60
    # + return - The seconds until the lease is actually free, or an `Error`
    isolated remote function breakLease(string path, int? breakPeriodSeconds = ())
            returns int|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.LeaseOps"
    } external;

    # Changes a lease's id, handing it over without a window in which another client could
    # acquire it.
    #
    # + path - The container-relative path of the blob
    # + leaseId - The current lease id
    # + proposedLeaseId - The new lease id
    # + return - The new lease id, or an `Error`
    isolated remote function changeLease(string path, string leaseId, string proposedLeaseId)
            returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.LeaseOps"
    } external;

    // -----------------------------------------------------------------------
    // Append blob operations
    // -----------------------------------------------------------------------

    # Creates an empty append blob. Append blobs grow only through the append operations: the
    # transfer operations always create block blobs.
    #
    # + path - The container-relative path of the blob to create
    # + options - Optional creation options (content headers, metadata, tags, lease id)
    # + return - An `Error` if the blob could not be created, otherwise `()`
    isolated remote function createAppendBlob(string path, CreateBlobOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AppendBlobOps"
    } external;

    # Appends a block of content at the blob's end. Blocks append in arrival order; each block
    # may be at most 4 MiB, and a blob holds at most 50,000 blocks.
    #
    # + path - The container-relative path of the append blob
    # + content - The content to append
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the block could not be appended, otherwise `()`
    isolated remote function appendBlock(string path, byte[] content, AppendBlockOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AppendBlobOps"
    } external;

    # Appends content the service reads from a URL, with the same source rules as
    # `copyBlobFromUrl`.
    #
    # + path - The container-relative path of the append blob
    # + sourceUrl - The URL of the source
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the block could not be appended, otherwise `()`
    isolated remote function appendBlockFromUrl(string path, string sourceUrl,
            AppendBlockOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AppendBlobOps"
    } external;

    // -----------------------------------------------------------------------
    // Page blob operations
    // -----------------------------------------------------------------------

    # Creates an empty page blob of a fixed capacity. The capacity is reserved sparsely:
    # unwritten pages read as zeros and only written pages are billed.
    #
    # + path - The container-relative path of the blob to create
    # + sizeInBytes - The blob's fixed capacity, a multiple of 512 up to 8 TiB
    # + options - Optional creation options (content headers, metadata, tags, lease id)
    # + return - An `Error` if the blob could not be created, otherwise `()`
    isolated remote function createPageBlob(string path, int sizeInBytes,
            CreateBlobOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.PageBlobOps"
    } external;

    # Writes content at the given offset of a page blob. The offset must be 512-byte aligned
    # and the content length a multiple of 512, at most 4 MiB per call.
    #
    # + path - The container-relative path of the page blob
    # + offset - The 512-byte-aligned offset to write at
    # + content - The content to write
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the pages could not be written, otherwise `()`
    isolated remote function uploadPages(string path, int offset, byte[] content,
            PageOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.PageBlobOps"
    } external;

    # Resets a 512-byte-aligned range of a page blob to zeros, releasing its storage.
    #
    # + path - The container-relative path of the page blob
    # + offset - The 512-byte-aligned offset to clear from
    # + length - The 512-byte-aligned number of bytes to clear
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the pages could not be cleared, otherwise `()`
    isolated remote function clearPages(string path, int offset, int length,
            PageOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.PageBlobOps"
    } external;

    # Lists the written byte ranges of a page blob.
    #
    # + path - The container-relative path of the page blob
    # + options - Optional options (a byte range to restrict the listing, a snapshot id)
    # + return - An array of `PageRange`, or an `Error`
    isolated remote function listPageRanges(string path, PageRangeOptions? options = ())
            returns PageRange[]|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.PageBlobOps"
    } external;

    // -----------------------------------------------------------------------
    // Block operations
    // -----------------------------------------------------------------------

    # Stages a block of content under a base64 block id, without changing the visible blob. An
    # id that is not valid base64 fails before any request is made.
    #
    # + path - The container-relative path of the blob
    # + blockId - The block's base64 identifier; all ids of one blob must be equal in length
    # + content - The content to stage, at most 4,000 MiB
    # + options - Optional options (the lease id, when the blob is leased)
    # + return - An `Error` if the block could not be staged, otherwise `()`
    isolated remote function stageBlock(string path, string blockId, byte[] content,
            StageBlockOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlockOps"
    } external;

    # Stages a block the service reads from a URL, so the content never passes through the
    # caller. The source rules are those of `copyBlobFromUrl`.
    #
    # + path - The container-relative path of the blob
    # + blockId - The block's base64 identifier; all ids of one blob must be equal in length
    # + sourceUrl - The URL of the source
    # + options - Optional options (a source byte range, the lease id)
    # + return - An `Error` if the block could not be staged, otherwise `()`
    isolated remote function stageBlockFromUrl(string path, string blockId, string sourceUrl,
            StageBlockFromUrlOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlockOps"
    } external;

    # Commits an ordered list of staged block ids as the blob's content, creating or replacing
    # it in one atomic step.
    #
    # + path - The container-relative path of the blob
    # + blockIds - The block ids to commit, in order
    # + options - Optional options (content headers, metadata, tags, tier, lease id)
    # + return - An `Error` if the blocks could not be committed, otherwise `()`. All block ids
    #            must be equal in length; an unequal set fails before the request is made
    isolated remote function commitBlockList(string path, string[] blockIds,
            UploadOptions? options = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlockOps"
    } external;

    # Reports a blob's committed blocks and its staged, uncommitted blocks. This is how a
    # restarted producer finds what it already staged.
    #
    # + path - The container-relative path of the blob
    # + return - The `BlockList`, or an `Error`
    isolated remote function listBlocks(string path) returns BlockList|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.BlockOps"
    } external;

    // -----------------------------------------------------------------------
    // SAS generation
    // -----------------------------------------------------------------------

    # Mints a SAS token scoped to the whole container, signed locally with the account key.
    # Requires a shared-key credential or a connection string carrying an account key.
    #
    # + values - The values signed into the token (window, permissions, policy identifier)
    # + return - The SAS token, without a leading `?`, or an `Error`
    public isolated function generateContainerSas(ContainerSasSignatureValues values)
            returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SasOps"
    } external;

    # Mints a SAS token scoped to a single blob. Signs locally with the account key and makes
    # no service call, so this is an ordinary method.
    #
    # + path - The container-relative path of the blob the token grants access to
    # + values - The values signed into the token (window, permissions, policy identifier)
    # + return - The SAS token, without a leading `?`, or an `Error`
    public isolated function generateSas(string path, BlobSasSignatureValues values)
            returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SasOps"
    } external;

    # Mints a container-scoped user-delegation SAS token, signed with the given key. Valid at
    # most 7 days; a stored access policy `identifier` is rejected.
    #
    # + values - The values signed into the token; an explicit window and permissions are required
    # + key - The user-delegation key from `AdminClient.getUserDelegationKey`
    # + return - The SAS token, without a leading `?`, or an `Error`
    public isolated function generateContainerUserDelegationSas(ContainerSasSignatureValues values,
            UserDelegationKey key) returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SasOps"
    } external;

    # Mints a blob-scoped user-delegation SAS token, signed with the given key. Valid at most
    # 7 days; a stored access policy `identifier` is rejected.
    #
    # + path - The container-relative path of the blob the token grants access to
    # + values - The values signed into the token; an explicit window and permissions are required
    # + key - The user-delegation key from `AdminClient.getUserDelegationKey`
    # + return - The SAS token, without a leading `?`, or an `Error`
    public isolated function generateUserDelegationSas(string path, BlobSasSignatureValues values,
            UserDelegationKey key) returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SasOps"
    } external;
}
