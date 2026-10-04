# Blob event processor

This example reacts to blobs as they arrive in a container. A `Listener` consumes the storage queue an Event Grid subscription delivers blob events to. A `.json` blob created in the `invoices` container is bound to an `Invoice` record, tagged, and moved under `processed/` (a copy under the new path followed by a delete, since Blob Storage has no rename); every other created blob is logged with its size and content type; deletions are logged too. Events whose content cannot be fetched or bound, and failed polls, reach `onError`.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account, a container, and obtain the credentials, and follow its "Wire the event queue" step to create the queue and the Event Grid subscription that fills it. With the Azure CLI, the one-time wiring for this example is:

```bash
az storage container create --name invoices --account-name <storage account name> --account-key <storage account key>
az storage queue create --name blob-events --account-name <storage account name> --account-key <storage account key>
az eventgrid event-subscription create --name blob-events-to-queue \
    --source-resource-id "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>" \
    --endpoint-type storagequeue \
    --endpoint "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>/queueServices/default/queues/blob-events" \
    --included-event-types Microsoft.Storage.BlobCreated Microsoft.Storage.BlobDeleted \
    --subject-begins-with /blobServices/default/containers/invoices/
```

The subject filter limits the subscription to the `invoices` container; drop it to receive the events of every container.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
accountName = "<storage account name>"
accountKey = "<storage account key>"
# optional, defaults to "blob-events"
# queueName = "my-queue"
```

## Run the example

```bash
bal run
```

The program starts consuming the queue and runs until you stop it with `Ctrl+C`.

To see it work, upload a file into the `invoices` container (through the [Azure portal](https://portal.azure.com) or the Azure CLI). A `.json` file should hold the fields of the example's `Invoice` record, for example an `inv-1001.json` containing:

```json
{"id": "inv-1001", "customer": "Contoso", "total": 250.00}
```

Within a few seconds the invoice is logged, tagged with `status=processed`, and moved to `processed/inv-1001.json`; the delete of the original is logged as its own event. Any other file is logged with its size and content type. A `.json` file that does not match the record is reported through `onError` and acknowledged, so it is not redelivered.
