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

import ballerinax/azure.storage.blob as blob;

listener blob:Listener lsn = new (auth = {accountName: "acc", accountKey: "a2V5"});

@blob:ServiceConfig {
    queueName: "raw-uploads",
    poisonQueueName: "raw-uploads-dead",
    batchSize: 32,
    newBatchThreshold: 32,
    maxPollingIntervalSeconds: 0.5,
    redeliveryDelaySeconds: 604800,
    maxDeliveryCount: 1
}
isolated service on lsn {
    isolated remote function onBlob(byte[] content) returns error? {
    }
}
