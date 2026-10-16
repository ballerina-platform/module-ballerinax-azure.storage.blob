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

// The jsondata module is called from the native side only; the import keeps its jar on the
// package's classpath.
import ballerina/data.jsondata as _;
import ballerina/jballerina.java;

// The externs that are not themselves client methods: client construction, which validates the
// credential locally and makes no call to Azure.

isolated function initAdminClient(AdminClient adminClient, ClientConfiguration config)
        returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.ClientInit"
} external;

isolated function initClient(Client blobClient, string containerName, ClientConfiguration config)
        returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.ClientInit"
} external;

// Writes already-serialized upload content; record content is serialized in Ballerina before
// reaching this call (see Client.upload). `appliedFormat` names the serialization the
// connector applied, so the content type can follow it.
isolated function externUpload(Client blobClient, byte[]|string|xml content, string destinationPath,
        UploadContentOptions? options, FileFormat? appliedFormat) returns Error? = @java:Method {
    name: "upload", 'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;

// A stream upload stages each buffered chunk as one block and commits them at the end. The
// block size is the SDK's own default upload block size (4 MiB); with the service's 50,000
// block ceiling one streamed upload can reach about 195 GiB.
const int STREAM_BLOCK_BYTES = 4 * 1024 * 1024;

// Each stream upload stages its blocks under an id of its own, so two uploads to the same path
// cannot commit each other's blocks.
isolated function newStreamUploadId() returns string = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;

isolated function stageStreamBlock(Client blobClient, string destinationPath, string uploadId, int index,
        byte[] chunk, UploadContentOptions? options) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;

isolated function commitStreamBlocks(Client blobClient, string destinationPath, string uploadId, int blockCount,
        UploadContentOptions? options, FileFormat? appliedFormat) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;

// ---------------------------------------------------------------------------
// Stream generators
// ---------------------------------------------------------------------------

type BlobStreamEntry record {|
    BlobEntry value;
|};

isolated class BlobEntryStreamGenerator {

    public isolated function next() returns BlobStreamEntry|Error? {
        BlobEntry|Error? entry = nextBlobEntry(self);
        if entry is BlobEntry {
            return {value: entry};
        }
        return entry;
    }

    public isolated function close() returns Error? {
        return closeBlobIterator(self);
    }
}

isolated function newBlobIterator(Client blobClient, BlobEntryStreamGenerator generator,
        BlobListOptions? options) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.ListOps"
} external;

isolated function nextBlobEntry(BlobEntryStreamGenerator generator) returns BlobEntry|Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.ListOps"
} external;

isolated function closeBlobIterator(BlobEntryStreamGenerator generator) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.ListOps"
} external;

type TaggedBlobStreamEntry record {|
    TaggedBlobEntry value;
|};

isolated class TaggedBlobStreamGenerator {

    public isolated function next() returns TaggedBlobStreamEntry|Error? {
        TaggedBlobEntry|Error? entry = nextTaggedBlobEntry(self);
        if entry is TaggedBlobEntry {
            return {value: entry};
        }
        return entry;
    }

    public isolated function close() returns Error? {
        return closeTaggedBlobIterator(self);
    }
}

isolated function newTaggedBlobIterator(Client blobClient, TaggedBlobStreamGenerator generator,
        string query) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TagOps"
} external;

isolated function nextTaggedBlobEntry(TaggedBlobStreamGenerator generator)
        returns TaggedBlobEntry|Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TagOps"
} external;

isolated function closeTaggedBlobIterator(TaggedBlobStreamGenerator generator) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TagOps"
} external;

# One entry of a byte stream: the chunk wrapper the byte-stream iterators return.
type ContentStreamEntry record {|
    # The chunk of bytes read from the blob
    byte[] value;
|};

// Backs the lazy byte stream `getBlob` returns; the native side creates it by name.
isolated class ContentStreamGenerator {

    public isolated function next() returns ContentStreamEntry|Error? {
        byte[]|Error? chunk = nextContentChunk(self);
        if chunk is byte[] {
            return {value: chunk};
        }
        return chunk;
    }

    public isolated function close() returns Error? {
        return closeContentStream(self);
    }
}

isolated function nextContentChunk(ContentStreamGenerator generator) returns byte[]|Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;

isolated function closeContentStream(ContentStreamGenerator generator) returns Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.client.TransferOps"
} external;
