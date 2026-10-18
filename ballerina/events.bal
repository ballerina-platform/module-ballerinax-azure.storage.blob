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

# Parses one delivered blob event into a `BlobEvent`, for applications that receive blob events
# by another route, such as an Event Grid subscription delivering to Azure Service Bus.
#
# + message - The message body as bytes, a string, or parsed JSON; base64-encoded or raw; in the
#             Event Grid or CloudEvents schema; a single event or a one-element array
# + return - The event of a created or deleted blob, `()` for a valid event of another type or a
#            created event that carries no content, or an `Error` if the message is malformed
public isolated function parseEvent(anydata message) returns BlobEvent|Error? = @java:Method {
    'class: "io.ballerina.lib.azure.storage.blob.server.EventParser"
} external;
