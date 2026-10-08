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

import ballerina/io;
import ballerina/lang.runtime;

import ballerinax/azure.storage.blob;

configurable string accountName = ?;
configurable string accountKey = ?;
configurable string containerName = "tag-search-example";

type Order record {|
    string id;
    string region;
    string status;
|};

public function main() returns error? {
    blob:AdminClient admin = check new (auth = {accountName, accountKey});
    boolean exists = check admin->hasContainer(containerName);
    if !exists {
        check admin->createContainer(containerName);
    }
    blob:Client orders = check new (containerName, auth = {accountName, accountKey});

    // Upload a few orders, each carrying its region and status as index tags.
    Order[] batch = [
        {id: "ord-1", region: "eu", status: "pending"},
        {id: "ord-2", region: "us", status: "pending"},
        {id: "ord-3", region: "eu", status: "shipped"}
    ];
    foreach Order 'order in batch {
        check orders->upload('order, string `orders/${'order.id}.json`,
                {tags: {region: 'order.region, status: 'order.status}});
    }

    // Find the pending EU orders by their tags, without listing the container. The tag index is
    // updated shortly after a tag is written, so the query is retried until it reflects the uploads.
    io:println("Pending EU orders:");
    string[] found = [];
    foreach int attempt in 0 ..< 15 {
        stream<blob:TaggedBlobEntry, blob:Error?> pending =
                check orders->findBlobsByTags("\"region\" = 'eu' AND \"status\" = 'pending'");
        check from blob:TaggedBlobEntry entry in pending
            do {
                io:println(string `  ${entry.path}  ${entry.tags.toString()}`);
                found.push(entry.path);
            };
        if found.length() > 0 {
            break;
        }
        io:println("  (waiting for the tag index)");
        runtime:sleep(2);
    }

    // Mark them processed: index tags are replaced whole, so the full tag set is written back.
    foreach string path in found {
        map<string> tags = check orders->getTags(path);
        tags["status"] = "processed";
        check orders->setTags(path, tags);
        io:println(string `marked ${path} processed`);
    }
}
