# Examples

The `ballerinax/azure.storage.blob` connector provides practical examples illustrating usage in various scenarios. Each example is a standalone Ballerina project with its own walkthrough.

1. [Folder archive](folder-archive) — upload a local folder into a container, tier each blob by its age, and list what the archive holds.
2. [Time-limited download link](download-link) — upload a report and generate a time-limited, read-only SAS URL that can be handed to a third party.
3. [Blob event processor](blob-event-processor) — react to blobs as they arrive with the listener, binding invoices to a record, tagging and moving them, parking bad ones in the poison queue, and logging everything else.
4. [Tag search](tag-search) — find blobs by their index tags with a tag query and mark the matches processed.
5. [Blob events from Service Bus](blob-events-from-service-bus) — consume blob events delivered to an Azure Service Bus queue with `parseEvent` and read each new invoice through the client.

## Prerequisites

Each example needs an Azure storage account and its access key; the connector's [setup guide](../README.md#setup-guide) walks through creating them. The folder archive, download link, and tag search examples need the container their `Config.toml` names to exist. The event processor additionally needs the storage queue and Event Grid subscription its walkthrough describes, and the Service Bus example a Service Bus queue with its subscription. Each example's walkthrough documents the `Config.toml` to create in the example directory.

## Running an example

Execute the following commands inside the example's directory:

```bash
bal build
bal run
```

## Building the examples against the local code

When changing the connector itself, build the examples against the local package rather than the released one:

```bash
./build.sh build
```

The script packs the `ballerina/` package into the local repository and builds every example offline against it. `./build.sh run` runs the examples the same way, which needs a `Config.toml` with credentials in each example directory.
