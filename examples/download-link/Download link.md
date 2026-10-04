# Download link

This example shares a blob with a third party without sharing the account credentials. It uploads a report to a container and generates a time-limited, read-only shared access signature (SAS) URL scoped to that single blob. Anyone with the URL can read the report, and nothing else in the account, for 24 hours; after that the link expires on its own.

## Prerequisites

Complete the connector's [setup guide](../../README.md#setup-guide) to create a storage account and obtain the credentials. The example creates the container it writes to, `handout-example` by default, when it does not exist yet.

## Configuration

Create `Config.toml` in the example directory with the following values:

```toml
accountName = "<storage account name>"
accountKey = "<storage account key>"
# optional, defaults to "handout-example"
# containerName = "my-reports"
```

## Run the example

```bash
bal run
```

The program prints a URL carrying the SAS token. Opening it in a browser (or fetching it with `curl`) downloads the report without any further authentication.
