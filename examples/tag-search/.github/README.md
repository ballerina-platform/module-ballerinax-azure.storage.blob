# Tag search

This example finds blobs by their index tags instead of listing a container. It uploads a few order records, each carrying its region and status as blob index tags, queries the container for the pending orders of one region with a tag expression, and marks the matches processed by rewriting their tag set.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account and obtain the credentials. The example creates the container it writes to, `tag-search-example` by default, when it does not exist yet. Blob index tags need a general-purpose v2 or premium block blob account; the tag index the query reads is updated by the service shortly after a tag is written, so a run immediately after a previous one may still see the earlier tags.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
accountName = "<storage account name>"
accountKey = "<storage account key>"
# optional, defaults to "tag-search-example"
# containerName = "my-orders"
```

## Run the example

```bash
bal run
```

The program prints the orders the tag query found and marks each one processed. A second run finds nothing pending for that region, since the first run rewrote the matching tags; delete the blobs or change the status tag back to see them again.
