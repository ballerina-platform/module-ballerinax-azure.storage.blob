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

import ballerina/file;
import ballerina/io;
import ballerina/time;

import ballerinax/azure.storage.blob;

configurable string accountName = ?;
configurable string accountKey = ?;
configurable string containerName = "archive-example";
configurable string localFolder = "resources/archive-me";
// Files older than this many days go straight to the archive tier; the rest to the cool tier.
configurable int archiveAfterDays = 30;

public function main() returns error? {
    blob:AdminClient admin = check new (auth = {accountName, accountKey});
    boolean exists = check admin->hasContainer(containerName);
    if !exists {
        check admin->createContainer(containerName);
    }
    blob:Client archive = check new (containerName, auth = {accountName, accountKey});

    // Upload every file of the folder under a prefix of today's date, and tier it by age.
    string prefix = time:utcToCivil(time:utcNow()).year.toString();
    file:MetaData[] entries = check file:readDir(localFolder);
    foreach file:MetaData entry in entries {
        if entry.dir {
            continue;
        }
        string name = check file:basename(entry.absPath);
        string destination = string `${prefix}/${name}`;
        check archive->uploadFromFile(entry.absPath, destination);
        int ageDays = <int>(time:utcDiffSeconds(time:utcNow(), entry.modifiedTime) / 86400d);
        blob:AccessTier tier = ageDays >= archiveAfterDays ? blob:ARCHIVE : blob:COOL;
        check archive->setAccessTier(destination, tier);
        io:println(string `archived ${name} as ${destination} (${tier})`);
    }

    // List what the archive now holds under this year's prefix.
    io:println(string `\nBlobs under ${prefix}/:`);
    stream<blob:BlobEntry, blob:Error?> listed = check archive->listBlobs({prefix: prefix + "/"});
    check from blob:BlobEntry blobEntry in listed
        do {
            io:println(string `  ${blobEntry.path}  ${blobEntry.contentLength ?: 0} bytes  ${blobEntry.accessTier ?: "-"}`);
        };
}
