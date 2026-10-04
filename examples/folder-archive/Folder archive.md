# Folder archive

This example archives a local folder into Azure Blob Storage. It uploads every file of the folder under a prefix of the current year, sets each blob's access tier by the file's age (recently modified files go to the cool tier, older ones straight to the archive tier, where storage is cheapest), and lists what the archive holds under that prefix.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account and obtain the credentials. The example creates the container it writes to, `archive-example` by default, when it does not exist yet.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
accountName = "<storage account name>"
accountKey = "<storage account key>"
# optional, defaults to "archive-example"
# containerName = "my-archive"
# optional, defaults to the bundled "resources/archive-me" folder
# localFolder = "/data/reports"
# optional, defaults to 30: files modified at least this many days ago are archived outright
# archiveAfterDays = 90
```

## Run the example

```bash
bal run
```

The program uploads the files of the configured folder, prints the tier each one was given, and lists the blobs under the year's prefix. Run it again after editing or adding files to see the uploads replace the earlier blobs; a blob already in the archive tier is left as it is, since the service refuses to overwrite an archived blob until it is rehydrated.
