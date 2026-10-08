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

// Test backend selection. The suite runs against live Azure when account credentials are
// configured (Config.toml, or the environment variables a CI run supplies) and against the
// Azurite emulator otherwise. One backend per run: credentials present means live only, and
// no test behaves differently between the two.

import ballerina/lang.runtime;
import ballerina/os;
import ballerina/test;
import ballerina/time;

// Credentials come from Config.toml, or from environment variables when no Config.toml
// entry is present.
configurable string liveAccountName = os:getEnv("LIVE_ACCOUNT_NAME");
configurable string liveAccountKey = os:getEnv("LIVE_ACCOUNT_KEY");

// Microsoft Entra ID credentials for the Entra auth and user-delegation tests, which the
// emulator cannot serve. The identity must hold Storage Blob Data Contributor and Storage
// Blob Delegator on the account.
configurable string liveEntraTenantId = os:getEnv("LIVE_ENTRA_TENANT_ID");
configurable string liveEntraClientId = os:getEnv("LIVE_ENTRA_CLIENT_ID");
configurable string liveEntraClientSecret = os:getEnv("LIVE_ENTRA_CLIENT_SECRET");

// One backend per run, chosen by credential presence.
final boolean liveRun = liveAccountName != "" && liveAccountKey != "";

final boolean liveEntraEnabled = liveRun && liveEntraTenantId != ""
    && liveEntraClientId != "" && liveEntraClientSecret != "";

// Azurite's published development account (learn.microsoft.com/azure/storage/common/storage-use-azurite).
// The path-style service URL carries the account name as the first path segment, which is
// itself a required capability of the connector.
const string AZURITE_ACCOUNT = "devstoreaccount1";
const string AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const string AZURITE_URL = "http://127.0.0.1:10000/devstoreaccount1";

// Every container name carries a per-run prefix, so a rerun never collides with leftovers
// from an earlier failed run and the end-of-suite cleanup can find everything this run
// created by prefix alone.
final string containerPrefix = buildContainerPrefix();

isolated function buildContainerPrefix() returns string {
    string runId = os:getEnv("GITHUB_RUN_ID");
    string tag = runId != "" ? runId : time:utcNow()[0].toString();
    return string `azbt-${tag}`;
}

// The container name a test works in; created on demand by the test itself.
isolated function testContainer(string base) returns string => string `${containerPrefix}-${base}`;

// Creates a test container, tolerating ContainerAlreadyExists: container names are per-run
// unique, so a conflict here can only be this run's own create resurfacing through a
// transport retry whose first attempt succeeded but whose response was lost.
function createTestContainer(AdminClient admin, string container) returns Error? {
    Error? created = admin->createContainer(container);
    if created is ConflictError && created.detail().errorCode == "ContainerAlreadyExists" {
        return ();
    }
    return created;
}

// Backend-switching factories: tests obtain their clients here and stay unaware of which
// backend the run uses.
isolated function newAdmin() returns AdminClient|Error => liveRun
    ? new (auth = {accountName: liveAccountName, accountKey: liveAccountKey})
    : new (auth = {accountName: AZURITE_ACCOUNT, accountKey: AZURITE_KEY, serviceUrl: AZURITE_URL});

isolated function newContainerClient(string container) returns Client|Error => liveRun
    ? new (container, auth = {accountName: liveAccountName, accountKey: liveAccountKey})
    : new (container, auth = {accountName: AZURITE_ACCOUNT, accountKey: AZURITE_KEY, serviceUrl: AZURITE_URL});

// The shared-key credential of the active backend, for tests that construct their own client.
isolated function sharedKeyAuth() returns SharedKeyConfig => liveRun
    ? {accountName: liveAccountName, accountKey: liveAccountKey}
    : {accountName: AZURITE_ACCOUNT, accountKey: AZURITE_KEY, serviceUrl: AZURITE_URL};

// A client in the same backend that authenticates with a bare SAS token minted for this run.
isolated function newSasContainerClient(string container, string sasToken) returns Client|Error {
    if liveRun {
        return new (container, auth = {accountName: liveAccountName, sasToken});
    }
    return new (container, auth = {sasUrl: string `${AZURITE_URL}?${sasToken}`});
}

// Deletes every container this run created, by prefix, so a failed test leaves nothing behind.
@test:AfterSuite
function cleanUpContainers() returns error? {
    AdminClient admin = check newAdmin();
    ContainerList containers = check admin->listContainers({prefix: containerPrefix});
    foreach ContainerInfo container in containers.containers {
        Error? deleted = admin->deleteContainer(container.name);
        if deleted is Error && deleted !is NotFoundError {
            return deleted;
        }
    }
}

// Polls until the probe reports true, for the effects the service applies asynchronously
// (a container deletion completing, a restore landing).
function await(function () returns boolean|error probe, decimal timeoutSeconds = 30,
        decimal intervalSeconds = 0.25) returns error? {
    decimal waited = 0;
    while true {
        boolean|error met = probe();
        if met is boolean && met {
            return;
        }
        if waited >= timeoutSeconds {
            if met is error {
                return met;
            }
            return error(string `condition not met within ${timeoutSeconds}s`);
        }
        runtime:sleep(intervalSeconds);
        waited += intervalSeconds;
    }
}
