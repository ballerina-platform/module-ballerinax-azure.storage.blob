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

import ballerina/log;

import ballerinax/azure.storage.blob;

configurable string accountName = ?;
configurable string accountKey = ?;
configurable string queueName = "blob-events";
configurable string containerName = "invoices";

// The shape an invoice blob binds to.
type Invoice record {|
    string id;
    string customer;
    decimal total;
|};

listener blob:Listener invoiceListener = new (queueName, auth = {accountName, accountKey});

// Handles the events of the invoices container: a .json blob binds to an Invoice and is
// tagged and moved into "processed/"; any other blob is logged.
service /invoices on invoiceListener {

    remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
        // The copy below creates a blob in this same container, which fires its own created
        // event back into this handler; the processed copies are not invoices to process.
        if event.path.startsWith("processed/") {
            return;
        }
        log:printInfo("received invoice", path = event.path, id = invoice.id, customer = invoice.customer,
                total = invoice.total);
        // Blob Storage has no rename: copying under the new path and deleting the original moves
        // it. A copy carries no index tags, so the processed copy gets its tags with the copy.
        string processedPath = "processed/" + invoice.id + ".json";
        _ = check caller->copyBlobFromUrl(event.url, processedPath,
                {tags: {status: "processed", customer: invoice.customer}});
        check caller->deleteBlob(event.path);
        log:printInfo("moved invoice", sourcePath = event.path, destinationPath = processedPath);
    }

    remote function onBlob(byte[] content, blob:BlobEvent event) returns error? {
        log:printInfo("received a non-invoice blob", path = event.path, sizeBytes = content.length(),
                contentType = event.contentType ?: "unknown");
    }

    remote function onBlobDeleted(blob:BlobEvent event) returns error? {
        log:printInfo("blob deleted", path = event.path);
    }

    // Notified of poll failures, of events whose blob could not be fetched, and of .json blobs
    // that do not bind to the Invoice record; those events are acknowledged after this call.
    remote function onError(blob:Error err) returns error? {
        log:printError("invoice listener reported an error", 'error = err);
    }
}
