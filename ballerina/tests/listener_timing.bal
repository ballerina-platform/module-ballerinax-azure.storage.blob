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

// The listener's time behaviours (visibility renewal, the expiry margin, the graceful stop
// bound) are derived from the ten-minute hold of a received message. The suite shortens the
// hold through the native module's test setting, so those behaviours run within a test's
// lifetime; the derived values scale with it (renewal at half, the expiry margin at a tenth).

import ballerina/jballerina.java;
import ballerina/test;

const int TEST_HOLD_SECONDS = 20;
const decimal TEST_HOLD = 20;
const decimal TEST_RENEWAL = 10;
const decimal TEST_MARGIN = 2;

@test:BeforeSuite
function shortenTheHold() {
    _ = setSystemProperty(java:fromString("io.ballerina.lib.azure.storage.blob.listener.holdSeconds"),
            java:fromString(TEST_HOLD_SECONDS.toString()));
}

function setSystemProperty(handle key, handle value) returns handle = @java:Method {
    'class: "java.lang.System",
    name: "setProperty"
} external;
