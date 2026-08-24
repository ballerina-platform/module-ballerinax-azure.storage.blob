# Examples

The Ballerina Azure Blob Storage connector provides practical examples illustrating usage in various scenarios.

## Prerequisites

1. Follow the [Setup guide](https://github.com/ballerina-platform/module-ballerinax-azure.storage.blob#setup-guide) to create a storage account, a container, and the credentials the examples use.

2. For each example, create a `Config.toml` file with the credentials the example's own README lists. Here is a sample:

    ```toml
    accountName = "<storage account name>"
    accountKey = "<storage account key>"
    ```

## Running an example

Execute the following commands to build an example from the source:

* To build an example:

    ```bash
    bal build
    ```

* To run an example:

    ```bash
    bal run
    ```

## Building the examples with the local module

**Warning**: Because of the absence of support for reading local repositories for single Ballerina files, the bala of the module is manually written to the central repository as a workaround. Consequently, the bash script may modify your local Ballerina repositories.

Execute the following commands to build all the examples against the changes you have made to the module locally:

* To build all the examples:

    ```bash
    ./build.sh build
    ```

* To run all the examples:

    ```bash
    ./build.sh run
    ```
