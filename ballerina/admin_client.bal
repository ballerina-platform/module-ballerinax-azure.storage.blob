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
import ballerina/time;

# Account-level client for Azure Blob Storage, managing the containers within a storage
# account, the account's blob service configuration, and account-level SAS tokens.
public isolated client class AdminClient {

    # Initializes the account-level client for the given storage account.
    #
    # + config - The client configuration (authentication, retry, transport)
    # + return - An `Error` if the client could not be initialized, otherwise `()`
    public isolated function init(*ClientConfiguration config) returns Error? {
        return initAdminClient(self, config);
    }

    # Checks whether a container exists in the storage account. Returns `false` only when Azure
    # confirms the container is absent; an `Error` means the check itself failed.
    #
    # + containerName - The name of the container to check
    # + return - `true` if the container exists, `false` if not, or an `Error`
    isolated remote function hasContainer(string containerName) returns boolean|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Lists the containers in the storage account.
    #
    # + options - Optional listing options (prefix, inclusion toggles, a result limit and marker)
    # + return - The `ContainerList`, or an `Error`
    isolated remote function listContainers(ContainerListOptions? options = ())
            returns ContainerList|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.ListOps"
    } external;

    # Creates a new container in the storage account.
    #
    # + containerName - The name of the container to create
    # + options - Optional creation options (metadata, anonymous access level)
    # + return - An `Error` if the container could not be created, otherwise `()`
    isolated remote function createContainer(string containerName, ContainerCreateOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Deletes a container and every blob in it. Where the account's container soft-delete
    # retention policy is enabled, the container is retained for the configured period.
    #
    # + containerName - The name of the container to delete
    # + options - Optional deletion options (the lease id, when the container is leased)
    # + return - An `Error` if the container could not be deleted, otherwise `()`
    isolated remote function deleteContainer(string containerName, DeleteContainerOptions? options = ())
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Restores a soft-deleted container.
    #
    # + containerName - The name of the soft-deleted container to restore
    # + deletedContainerVersion - The version of the soft-deleted container (from `ContainerInfo.deletedVersion`)
    # + return - An `Error` if the container could not be restored, otherwise `()`
    isolated remote function undeleteContainer(string containerName, string deletedContainerVersion)
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Reads the account's blob service configuration.
    #
    # + return - The `ServiceProperties`, or an `Error`
    isolated remote function getServiceProperties() returns ServiceProperties|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Writes the account's blob service configuration. A group present in the record replaces
    # that group whole; a group absent is left unchanged.
    #
    # + properties - The configuration groups to apply
    # + return - An `Error` if the configuration could not be written, otherwise `()`
    isolated remote function setServiceProperties(ServiceProperties properties)
            returns Error? = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Reads the storage account's SKU, kind, and whether it has a hierarchical namespace.
    #
    # + return - The `AccountInfo`, or an `Error`
    isolated remote function getAccountInfo() returns AccountInfo|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Obtains a key for signing user-delegation SAS tokens. Requires a Microsoft Entra ID
    # credential whose identity holds the `generateUserDelegationKey` action at storage account
    # scope or wider, which `Storage Blob Delegator` or any blob data role assigned there grants.
    #
    # + startTime - When the key becomes valid
    # + expiryTime - When the key expires; at most 7 days after `startTime`
    # + return - The `UserDelegationKey`, or an `Error`
    isolated remote function getUserDelegationKey(time:Utc startTime, time:Utc expiryTime)
            returns UserDelegationKey|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.AdminOps"
    } external;

    # Mints an account-level SAS token, signed locally with the account key. Requires a
    # shared-key credential or a connection string carrying an account key.
    #
    # + values - The values signed into the token (services, resource types, permissions, window)
    # + return - The SAS token, without a leading `?`, or an `Error`
    public isolated function generateAccountSas(AccountSasSignatureValues values)
            returns string|Error = @java:Method {
        'class: "io.ballerina.lib.azure.storage.blob.client.SasOps"
    } external;
}
