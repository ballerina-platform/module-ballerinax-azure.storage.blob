# Blob event processor

This example reacts to blobs as they arrive in a container. A `Listener` service consumes the storage queue an Event Grid subscription delivers blob events to; the service names its queue in `@blob:ServiceConfig`. A `.json` blob created in the `invoices` container is bound to an `Invoice` record and moved under `processed/` with its tags set (a copy under the new path followed by a delete, since Blob Storage has no rename); an invoice with a negative total is parked in the poison queue by returning a `DeadLetterError`; every other created blob is logged with its size and content type; deletions are logged too. Failed polls, events whose content cannot be fetched or bound, and moves to the poison queue that failed reach `onError`, which marks an unreadable `.json` blob before its message moves to the poison queue.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account, a container, and obtain the credentials, and follow its "Wire the event queue" step to create the queue and the Event Grid subscription that fills it. With the Azure CLI, the one-time wiring for this example is:

```bash
az storage container create --name invoices --account-name <storage account name> --account-key <storage account key>
az storage queue create --name invoice-events --account-name <storage account name> --account-key <storage account key>
az eventgrid event-subscription create --name invoice-events-to-queue \
    --source-resource-id "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>" \
    --endpoint-type storagequeue \
    --endpoint "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>/queueServices/default/queues/invoice-events" \
    --included-event-types Microsoft.Storage.BlobCreated Microsoft.Storage.BlobDeleted \
    --subject-begins-with /blobServices/default/containers/invoices/ \
    --advanced-filter subject StringNotBeginsWith /blobServices/default/containers/invoices/blobs/processed/
```

The subject filter limits the subscription to the `invoices` container, and the advanced filter keeps the handler's own output, the copies under `processed/`, out of the queue. The poison queue, `invoice-events-poison`, is created by the listener when a message first moves there.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
accountName = "<storage account name>"
accountKey = "<storage account key>"
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

Within a few seconds the invoice is logged, tagged with `status=processed`, and moved to `processed/inv-1001.json`; the delete of the original is logged as its own event. Any other file is logged with its size and content type. A `.json` file that does not match the record is reported through `onError`, tagged `status=unreadable`, and its message moves to the poison queue; so does an invoice with a negative total. To inspect the poison queue, open **Data storage** > **Queues** > `invoice-events-poison` in the Azure portal, or run:

```bash
az storage message peek --queue-name invoice-events-poison --account-name <storage account name> --account-key <storage account key>
```
