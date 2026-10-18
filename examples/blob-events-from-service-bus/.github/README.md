# Blob events from Service Bus

This example consumes blob events that an Event Grid subscription delivers to an Azure Service Bus queue, with the `ballerinax/asb` connector, and reads each created invoice through the connector's `Client`. The `parseEvent` function turns a delivered message into a `BlobEvent` whatever its form: base64 or raw, the Event Grid or the CloudEvents schema. It returns `()` for a valid event of another type, or a created event that carries no content, which the handler skips.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account, an `invoices` container, and obtain the credentials. Create a Service Bus namespace with a queue named `blob-events`, and an Event Grid subscription on the storage account that delivers to it. With the Azure CLI:

```bash
az servicebus namespace create --name <namespace> --resource-group <resource group>
az servicebus queue create --name blob-events --namespace-name <namespace> --resource-group <resource group>
az eventgrid event-subscription create --name blob-events-to-service-bus \
    --source-resource-id "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.Storage/storageAccounts/<storage account name>" \
    --endpoint-type servicebusqueue \
    --endpoint "/subscriptions/<subscription id>/resourceGroups/<resource group>/providers/Microsoft.ServiceBus/namespaces/<namespace>/queues/blob-events" \
    --included-event-types Microsoft.Storage.BlobCreated \
    --subject-begins-with /blobServices/default/containers/invoices/
```

The subject filter limits the subscription to the `invoices` container.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
connectionString = "<Service Bus connection string>"
accountName = "<storage account name>"
accountKey = "<storage account key>"
```

## Run the example

```bash
bal run
```

The program consumes the Service Bus queue and runs until you stop it with `Ctrl+C`.

To see it work, upload a file into the `invoices` container holding the fields of the example's `Invoice` record, for example an `inv-1001.json` containing:

```json
{"id": "inv-1001", "customer": "Contoso", "total": 250.00}
```

Within a few seconds the invoice is logged. A message that is not a blob event fails `parseEvent` with an `Error`, which the handler returns, so the Service Bus connector handles it as a failed message.
