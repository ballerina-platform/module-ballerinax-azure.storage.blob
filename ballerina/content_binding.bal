// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/data.csv;

// The listener's CSV binding runs here, on a Ballerina strand, because the data.csv parser
// needs one; the native dispatcher calls in through the runtime.

// Binds CSV blob content to the declared handler type. A record target maps its fields through
// the header row, which the data.csv default consumes; the string matrix keeps every row.
isolated function bindCsvContent(byte[] content, typedesc<string[][]|record {}[]> targetType,
        boolean laxDataBinding) returns string[][]|record {}[]|error {
    csv:ParseOptions options = csvParseOptions(laxDataBinding);
    if targetType !is typedesc<record {}[]> {
        options.header = ();
    }
    return csv:parseBytes(content, options, targetType);
}

// The CSV parse options shared by the materialized and stream binding paths.
isolated function csvParseOptions(boolean laxDataBinding) returns csv:ParseOptions {
    if laxDataBinding {
        return {allowDataProjection: {nilAsOptionalField: true, absentAsNilableType: true}};
    }
    return {allowDataProjection: false};
}

# One entry of a CSV row stream: a row bound to the handler's declared row type.
type CsvRowEntry record {|
    # The bound row, a record or a string array
    record {}|string[] value;
|};

// Backs a CSV stream content handler: a data.csv row stream over the blob's byte stream. A row
// that fails to bind surfaces as that next() call's error entry, after which the stream closes.
class ContentCsvStream {

    private boolean isClosed = false;
    private final stream<record {}|string[], error?> csvStream;

    public isolated function init(typedesc<record {}|string[]> targetType, stream<byte[], error?> byteStream,
            csv:ParseOptions options) returns error? {
        // A record target maps its fields through the header row, which the data.csv default
        // consumes; the string array form yields every row, so the header consumption is off.
        if targetType !is typedesc<record {}> {
            options.header = ();
        }
        stream<record {}|string[], error?>|csv:Error parsed = csv:parseToStream(byteStream, options, targetType);
        if parsed is csv:Error {
            closeStreamQuietly(byteStream);
            return error Error("CSV stream binding could not be created: " + parsed.message(), parsed);
        }
        self.csvStream = parsed;
    }

    public isolated function next() returns CsvRowEntry|error? {
        if self.isClosed {
            return;
        }
        CsvRowEntry|error? nextEntry = trap self.csvStream.next();
        if nextEntry is () {
            self.isClosed = true;
            closeStreamQuietly(self.csvStream);
            return;
        }
        if nextEntry is error {
            self.isClosed = true;
            closeStreamQuietly(self.csvStream);
            return error Error("CSV content does not bind to the declared row type: " + nextEntry.message(), nextEntry);
        }
        return nextEntry;
    }

    public isolated function close() returns error? {
        if self.isClosed {
            return;
        }
        self.isClosed = true;
        return self.csvStream.close();
    }
}

isolated function closeStreamQuietly(stream<anydata, error?> streamValue) {
    error? closed = streamValue.close();
    if closed is error {
        // Best effort cleanup.
    }
}

// Creates the CSV row stream for a stream content handler; the native dispatcher calls this on
// a strand with the handler's declared row type.
isolated function newContentCsvStream(typedesc<record {}|string[]> targetType,
        stream<byte[], error?> byteStream, boolean laxDataBinding) returns ContentCsvStream|error {
    return new (targetType, byteStream, csvParseOptions(laxDataBinding));
}
