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

// The shape an invoice blob binds to.
type Invoice record {|
    string id;
    string customer;
    decimal total;
|};

listener blob:Listener invoiceListener = check new (auth = {accountName, accountKey});

// Consumes the queue the invoices container's Event Grid subscription delivers to. A .json blob
// binds to an Invoice and is tagged and moved into "processed/"; any other blob is logged.
@blob:ServiceConfig {queueName: "invoice-events"}
isolated service on invoiceListener {

    isolated remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
        // The copy below creates a blob in this same container. The subscription excludes the
        // "processed/" prefix (see the walkthrough); this check covers a subscription without it.
        if event.path.startsWith("processed/") {
            return;
        }
        if invoice.total < 0d {
            // Retrying cannot help: park the message in the poison queue for inspection.
            return error blob:DeadLetterError(string `negative total in ${event.path}`);
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

    isolated remote function onBlob(byte[] content, blob:BlobEvent event) returns error? {
        log:printInfo("received a non-invoice blob", path = event.path, sizeBytes = content.length(),
                contentType = event.contentType ?: "unknown");
    }

    isolated remote function onBlobDeleted(blob:BlobEvent event) returns error? {
        log:printInfo("blob deleted", path = event.path);
    }

    // Notified of failed polls, of events whose blob could not be fetched or bound, and of moves
    // to the poison queue that failed. It runs before the message is disposed of.
    isolated remote function onError(blob:Error err, blob:BlobEvent? event, blob:Caller? caller) returns error? {
        if err is blob:ContentBindingError && event is blob:BlobEvent && caller is blob:Caller {
            // A .json blob that is not an invoice: mark it, then its message moves to the poison queue.
            check caller->setTags(event.path, {status: "unreadable"});
        }
        log:printError("invoice listener reported an error", 'error = err);
    }
}
