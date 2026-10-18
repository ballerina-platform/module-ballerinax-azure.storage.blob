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

import ballerinax/asb;
import ballerinax/azure.storage.blob;

configurable string connectionString = ?;
configurable string accountName = ?;
configurable string accountKey = ?;

// The shape an invoice blob binds to.
type Invoice record {|
    string id;
    string customer;
    decimal total;
|};

final blob:Client invoices = check new ("invoices", auth = {accountName, accountKey});

listener asb:Listener asbListener = check new (
    connectionString = connectionString,
    entityConfig = {queueName: "blob-events"}
);

// Consumes the Service Bus queue an Event Grid subscription delivers blob events to, and reads
// each created invoice through the blob client.
isolated service on asbListener {

    isolated remote function onMessage(asb:Message message) returns error? {
        blob:BlobEvent? event = check blob:parseEvent(message.body);
        if event is () {
            // A valid event of another type, or a created event that carries no content.
            return;
        }
        // The queue may carry several accounts' events, and the subscription admits one container.
        if event.accountName != accountName || event.eventType != blob:BLOB_CREATED {
            return;
        }
        Invoice invoice = check invoices->getBlob(event.path);
        log:printInfo("received invoice", path = event.path, id = invoice.id, customer = invoice.customer,
                total = invoice.total);
    }
}
