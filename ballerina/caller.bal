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

# The context object passed to a listener service's handlers, exposing a container-scoped
# subset of `Client` to act on the event's blob. It cannot be instantiated by user code.
public isolated client class Caller {

    private final Client 'client;

    isolated function init(string containerName, *ClientConfiguration config) returns Error? {
        self.'client = check new (containerName, config);
    }

    # Retrieves a blob's content in the form the target type selects. Binding is strict: content
    # that does not match the target fails with a client-side `Error`.
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

    # Reads a blob's properties, metadata, and the state of its most recent copy.
    #
    # + path - The container-relative path of the blob
    # + return - The `BlobProperties`, or an `Error`
    isolated remote function getBlobProperties(string path) returns BlobProperties|Error {
        return self.'client->getBlobProperties(path);
    }

    # Downloads a blob to a local file. Fails if a local file already exists at the destination.
    #
    # + sourcePath - The container-relative path of the blob
    # + destinationPath - The local path to write, including the file name
    # + options - Optional download options (a byte range, a snapshot id)
    # + return - An `Error` if the download failed, otherwise `()`
    isolated remote function download(string sourcePath, string destinationPath,
            DownloadOptions? options = ()) returns Error? {
        return self.'client->download(sourcePath, destinationPath, options);
    }

    # Uploads in-memory content to the service's container.
    #
    # + content - The content to upload. A record, a record array, or another `json` value is
    #             serialized per the resolved format; `byte[]` and `string` are written as-is
    # + destinationPath - The container-relative path, including the blob name
    # + options - Optional upload options (headers, metadata, format override)
    # + return - An `Error` if the upload failed, otherwise `()`
    isolated remote function upload(UploadContent content, string destinationPath,
            UploadContentOptions? options = ()) returns Error? {
        return self.'client->upload(content, destinationPath, options);
    }

    # Deletes a blob, one of its snapshots, or the blob together with its snapshots.
    #
    # + path - The container-relative path of the blob
    # + options - Optional deletion options (snapshot handling, lease id)
    # + return - An `Error` if the blob could not be deleted, otherwise `()`
    isolated remote function deleteBlob(string path, DeleteBlobOptions? options = ()) returns Error? {
        return self.'client->deleteBlob(path, options);
    }

    # Starts a copy from any readable blob URL, such as `BlobEvent.url`, to a path in the
    # service's container. The copy is asynchronous; read its state through `getBlobProperties`.
    #
    # + sourceUrl - The URL of the blob to copy, including any SAS it needs
    # + destinationPath - The container-relative path to write
    # + options - Optional copy options (metadata, tags, tier, lease id)
    # + return - The `CopyInfo` of the accepted copy, or an `Error`
    isolated remote function copyBlobFromUrl(string sourceUrl, string destinationPath,
            CopyOptions? options = ()) returns CopyInfo|Error {
        return self.'client->copyBlobFromUrl(sourceUrl, destinationPath, options);
    }

    # Replaces a blob's index tags.
    #
    # + path - The container-relative path of the blob
    # + tags - The complete tag set to store
    # + options - Optional tag options (lease id)
    # + return - An `Error` if the tags could not be set, otherwise `()`
    isolated remote function setTags(string path, map<string> tags, TagOptions? options = ())
            returns Error? {
        return self.'client->setTags(path, tags, options);
    }
}
