## Overview

[Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction) is Microsoft's object storage for the cloud, holding unstructured data such as documents, media, backups, and logs in containers and serving it over HTTP.

The Azure Blob Storage connector offers APIs to connect to Azure Blob Storage and manage containers and the blobs within them, covering uploads, downloads, typed data binding, copies, access tiers, index tags, snapshots, leases, append and page blobs, block staging, and SAS token generation.

### Key Features

- Container-scoped `Client` for blob operations, transfers with typed data binding, copies, access tiers, index tags, snapshots, and leases
- Account-level `AdminClient` for creating, listing, deleting, and restoring containers and for the service configuration
- Append blobs, page blobs, and block staging for composing blobs from separately uploaded pieces
- `Listener` that consumes the storage queue an Event Grid subscription delivers blob events to, dispatching created and deleted events to typed handlers with a container-bound `Caller`
- Authentication with shared key, SAS tokens, connection strings, and Microsoft Entra ID
- GraalVM compatible for native image builds

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

### Step 5: Wire the event queue (listener only)

The connector's `Listener` consumes blob events from a storage queue that an Event Grid subscription fills. Create both once per storage account:

1. In the storage account, navigate to **Data storage** > **Queues**, click **+ Queue**, and provide a name (for example `blob-events`). This is the queue name you pass to the `Listener`.

2. Navigate to **Events** and click **+ Event Subscription**. Provide a name, choose the **Event Grid Schema** or **Cloud Event Schema v1.0** (the listener accepts both), and under **Event Types** select **Blob Created** and **Blob Deleted**.

3. Under **Endpoint Details**, choose **Storage Queue** as the endpoint type and select the queue created in step 1.

4. Optionally, under **Filters**, set a **Subject Begins With** filter such as `/blobServices/default/containers/invoices/` to limit the subscription to one container; otherwise the queue receives the events of every container, and the listener routes each to the service attached for its container.

5. Click **Create**. The credential the `Listener` uses must cover the queue as well as the blobs its handlers read: the account key does, an account SAS must span the queue and blob services, and a Microsoft Entra ID identity needs the **Storage Queue Data Contributor** role in addition to its blob data role.

The same wiring with the Azure CLI:

```bash
az storage queue create --name blob-events --account-name <storage account name> --account-key <storage account key>
az eventgrid event-subscription create --name blob-events-to-queue \
    --source-resource-id "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>" \
    --endpoint-type storagequeue \
    --endpoint "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>/queueServices/default/queues/blob-events" \
    --included-event-types Microsoft.Storage.BlobCreated Microsoft.Storage.BlobDeleted
```

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

#### React to blobs as they arrive

With the event queue wired (setup guide, step 5), a `Listener` dispatches each blob event to the service attached for the event's container. A `.json` blob created in the `reports` container binds to the handler's record.

```ballerina
listener blob:Listener blobListener = new ("blob-events", auth = {accountName, accountKey});

service /reports on blobListener {
    remote function onBlobJson(Metric metric, blob:BlobEvent event, blob:Caller caller) returns error? {
        io:println(string `${event.path}: ${metric.quarter} revenue ${metric.revenue}`);
        check caller->setTags(event.path, {status: "processed"});
    }
}
```

### Step 4: Run the Ballerina application

Save the changes and run the Ballerina application using the following command.

```bash
bal run
```

## Examples

The `azure.storage.blob` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples), covering the following use cases:

1. [Folder archive](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples/folder-archive) - Upload a local folder into a container, tier each blob by its age, and list what the archive holds.
2. [Download link](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples/download-link) - Upload a report and generate a time-limited, read-only SAS URL that can be handed to a third party.
3. [Blob event processor](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples/blob-event-processor) - React to blobs as they arrive with the listener, binding invoices to a record, tagging and moving them, and logging everything else.
4. [Tag search](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob/tree/main/examples/tag-search) - Find blobs by their index tags with a tag query and mark the matches processed.
