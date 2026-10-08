# Ballerina Azure Blob Storage Connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerinax-azure.storage.blob/branch/main/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerinax-azure.storage.blob)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-azure.storage.blob.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/commits/main)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/actions/workflows/build-with-bal-test-native.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/actions/workflows/build-with-bal-test-native.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

[Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction) is Microsoft's object storage for the cloud, holding unstructured data such as documents, media, backups, and logs in containers and serving it over HTTP.

The `ballerinax/azure.storage.blob` package offers APIs to connect to Azure Blob Storage and manage containers and the blobs within them, covering uploads, downloads, typed data binding, copies, access tiers, index tags, snapshots, leases, append and page blobs, block staging, and SAS token generation.

## Setup guide

To use the Azure Blob Storage connector, you must have an Azure subscription and an Azure storage account. If you do not have an Azure account, you can sign up for one [here](https://azure.microsoft.com/free/).

### Step 1: Create a storage account

1. Sign in to the [Azure portal](https://portal.azure.com/), search for **Storage accounts**, and open it.

2. Click **+ Create**.

3. On the **Basics** tab, provide the following:

    | Input | Value |
    |-------|-------|
    | **Subscription** and **Resource group** | The subscription and group the account bills to. |
    | **Storage account name** | A globally unique name. |
    | **Region** | The region closest to your workload. |
    | **Performance** | **Standard** for general-purpose blob storage; **Premium** with the **Block blobs** account type for low-latency workloads. |

4. Click **Review + create**, then **Create**, and wait for the deployment to complete. For the full set of options, see the [Azure documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create).

### Step 2: Create a container

1. Open the deployed storage account and navigate to **Data storage** > **Containers**.

2. Click **+ Container**, provide a name, and click **Create**. The container name is what you pass to the connector's `Client` at initialization.

### Step 3: Obtain the credentials

1. In the storage account, navigate to **Security + networking** > **Access keys**.

2. Click **Show** next to **key1** and copy the following values:

    | Value | Used as |
    |-------|---------|
    | Storage account name | `accountName` |
    | key1 **Key** | `accountKey` |

3. Optionally, use one of the other credentials the connector accepts: a SAS token or SAS URL (generated under **Security + networking** > **Shared access signature**), a connection string (shown alongside each access key), or Microsoft Entra ID credentials.

### Step 4: Enable soft delete (optional)

Restoring deleted blobs and containers needs soft delete. Under **Data management** > **Data protection**, enable **soft delete for blobs** (also settable through the connector's service configuration) and **soft delete for containers** (an account setting), each with a retention period.

## Quickstart

To use the `azure.storage.blob` connector in your Ballerina application, modify the `.bal` file as follows:

### Step 1: Import the module

```ballerina
import ballerina/io;

import ballerinax/azure.storage.blob;
```

### Step 2: Instantiate a new connector

A `Client` is bound to a single container. Provide the credentials through configurable variables:

```ballerina
configurable string accountName = ?;
configurable string accountKey = ?;

blob:Client blobClient = check new ("reports", auth = {accountName, accountKey});
```

### Step 3: Invoke the connector operation

Now, utilize the available connector operations.

#### Create the container

The client is bound to a container, so create it first if it does not exist yet.

```ballerina
blob:AdminClient admin = check new (auth = {accountName, accountKey});
if !(check admin->hasContainer("reports")) {
    check admin->createContainer("reports");
}
```

#### Upload a file

Paths are relative to the bound container; slashes in a path form virtual folders.

```ballerina
check blobClient->uploadFromFile("./local/q1.pdf", "2026/q1.pdf");
```

#### Upload and read back structured content

A record is serialized per the destination's extension, and read back into the type the target declares.

```ballerina
type Metric record {
    string quarter;
    int revenue;
};

Metric q1 = {quarter: "q1", revenue: 1250000};
check blobClient->upload(q1, "2026/q1/metrics.json");

Metric stored = check blobClient->getBlob("2026/q1/metrics.json");
```

#### List the blobs under a prefix

```ballerina
stream<blob:BlobEntry, blob:Error?> entries = check blobClient->listBlobs({prefix: "2026/"});
check from blob:BlobEntry entry in entries
    do {
        io:println(entry.path);
    };
```

### Step 4: Run the Ballerina application

Save the changes and run the Ballerina application using the following command.

```bash
bal run
```

## Examples

The `azure.storage.blob` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples), covering use cases like archiving files to a container, handing out a time-limited download link, and reacting to blobs as they arrive.

## Issues and projects

The **Issues** and **Projects** tabs are disabled for this repository as this is part of the Ballerina library. To report bugs, request new features, start new discussions, view project boards, etc., visit the Ballerina library [parent repository](https://github.com/ballerina-platform/ballerina-library).

This repository only contains the source code for the package.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Download and install [Docker](https://www.docker.com/get-started).

   > **Note**: Ensure that the Docker daemon is running before executing any tests.

4. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

    ```bash
    ./gradlew clean build
    ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build the without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run the tests against different environments:

   The suite runs against the Azurite emulator by default and against a live Azure storage account when credentials are provided. See the [Test Guide](ballerina/tests/README.md) for details.

5. To debug package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information go to the [`azure.storage.blob` package](https://lib.ballerina.io/ballerinax/azure.storage.blob/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
