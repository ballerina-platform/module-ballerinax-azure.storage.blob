/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.blob.server;

import io.ballerina.lib.azure.storage.blob.server.ServiceContext.HandlerConfig;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * The routing table of spec 5.5 and the wording of the never-ours warnings, which name the
 * subscription filter that keeps such events out of the queue.
 */
public class RoutingTest {

    private static HandlerConfig handler(String name, String namePattern, String contentTypePattern) {
        return new HandlerConfig(name, namePattern == null ? null : Pattern.compile(namePattern),
                contentTypePattern == null ? null : Pattern.compile(contentTypePattern), false, false, null);
    }

    private static Map<String, HandlerConfig> handlers(HandlerConfig... configs) {
        Map<String, HandlerConfig> map = new LinkedHashMap<>();
        for (HandlerConfig config : configs) {
            map.put(config.methodName(), config);
        }
        return map;
    }

    private static String route(Map<String, HandlerConfig> handlers, String name, String contentType) {
        HandlerConfig resolved = Routing.resolve(handlers, name, contentType);
        return resolved == null ? null : resolved.methodName();
    }

    @Test
    public void extensionsRouteIgnoringCaseAndEverythingElseGoesToOnBlob() {
        Map<String, HandlerConfig> all = handlers(handler("onBlobText", null, null), handler("onBlobJson", null, null),
                handler("onBlobXml", null, null), handler("onBlobCsv", null, null), handler("onBlob", null, null));
        assertEquals(route(all, "a.txt", null), "onBlobText");
        assertEquals(route(all, "A.JSON", "image/png"), "onBlobJson");
        assertEquals(route(all, "a.Xml", null), "onBlobXml");
        assertEquals(route(all, "a.csv", "application/json"), "onBlobCsv");
        assertEquals(route(all, "a.png", "application/json"), "onBlob", "an extension is routed by the extension");
        assertEquals(route(all, "a.tar.gz", null), "onBlob");
    }

    @Test
    public void namesWithoutAnExtensionRouteByTheContentTypeWithoutItsParameters() {
        Map<String, HandlerConfig> all = handlers(handler("onBlobText", null, null), handler("onBlobJson", null, null),
                handler("onBlobXml", null, null), handler("onBlobCsv", null, null), handler("onBlob", null, null));
        assertEquals(route(all, "noext", "application/json; charset=utf-8"), "onBlobJson");
        assertEquals(route(all, "noext", "Application/XML"), "onBlobXml");
        assertEquals(route(all, "noext", "text/xml"), "onBlobXml");
        assertEquals(route(all, "noext", "text/csv"), "onBlobCsv", "text/csv before the text/* rule");
        assertEquals(route(all, "noext", "text/markdown"), "onBlobText");
        assertEquals(route(all, "noext", "application/octet-stream"), "onBlob");
        assertEquals(route(all, "noext", null), "onBlob");
    }

    @Test
    public void anUndeclaredTypedHandlerFallsBackToOnBlobOrToNothing() {
        assertEquals(route(handlers(handler("onBlob", null, null)), "a.json", null), "onBlob");
        assertNull(route(handlers(handler("onBlobText", null, null)), "a.json", null));
        assertNull(route(handlers(handler("onBlobDelete", null, null)), "a.txt", null));
    }

    @Test
    public void patternsWinInTheFixedOrderAndEitherPatternMatches() {
        Map<String, HandlerConfig> patterned = handlers(
                handler("onBlobJson", "^invoice-.*", null),
                handler("onBlobText", null, "text/.*|application/json"),
                handler("onBlob", null, null));
        assertEquals(route(patterned, "invoice-9.dat", null), "onBlobJson");
        assertEquals(route(patterned, "report.bin", "text/plain"), "onBlobText");
        assertEquals(route(patterned, "invoice-both.txt", "application/json"), "onBlobText",
                "onBlobText is checked before onBlobJson whatever the declaration order");
        assertEquals(route(patterned, "unmatched.json", "application/octet-stream"), "onBlobJson",
                "without a pattern match the extension routes to the pattern handler too");
        assertEquals(route(patterned, "other.png", "image/png"), "onBlob");
        // Patterns match the whole name and the whole base content type.
        assertEquals(route(handlers(handler("onBlobJson", "invoice", null), handler("onBlob", null, null)),
                "invoice-9.json", null), "onBlobJson", "the extension, not the partial pattern");
        assertEquals(route(handlers(handler("onBlobText", null, "text"), handler("onBlob", null, null)),
                "x.bin", "text/plain"), "onBlob");
    }

    @Test
    public void neverOursWarningsNameTheFilterForTheirKind() {
        String type = Routing.typeWarning("q", "Microsoft.Storage.BlobTierChanged");
        assertTrue(type.contains("includedEventTypes") && type.contains("BlobTierChanged"), type);
        String deleted = Routing.deletedWarning("q");
        assertTrue(deleted.contains("onBlobDelete") && deleted.contains("includedEventTypes"), deleted);
        EventParser.Parsed createFile = parsed("CreateFile", null);
        String api = Routing.noContentWarning("q", createFile);
        assertTrue(api.contains("StringNotIn on data.api"), api);
        assertTrue(api.contains("CreateFile, SftpCreate, and SftpWrite"), api);
        assertEquals(Routing.noContentKey(createFile), "api:CreateFile");
        EventParser.Parsed append = parsed("PutBlob", "AppendBlob");
        String blobType = Routing.noContentWarning("q", append);
        assertTrue(blobType.contains("StringNotIn on data.blobType") && blobType.contains("AppendBlob and PageBlob")
                && blobType.contains("copies and deletes"), blobType);
        assertEquals(Routing.noContentKey(append), "blobType:AppendBlob");
    }

    @Test
    public void theUnroutedWarningNamesTheExtensionSeenAndTheHandledOnes() {
        Map<String, HandlerConfig> one = handlers(handler("onBlobText", null, null));
        String warning = Routing.unroutedWarning("q", "dir/report.PDF", one);
        assertTrue(warning.contains("StringNotEndsWith on subject with '.pdf'"), warning);
        assertTrue(warning.contains("subjectEndsWith with '.txt'"), warning);
        Map<String, HandlerConfig> two = handlers(handler("onBlobJson", null, null), handler("onBlobCsv", null, null));
        String several = Routing.unroutedWarning("q", "a.png", two);
        assertTrue(several.contains("StringEndsWith on subject with .json, .csv"), several);
        String noExtension = Routing.unroutedWarning("q", "README", one);
        assertTrue(noExtension.contains("without an extension") && !noExtension.contains("StringNotEndsWith"),
                noExtension);
        assertEquals(Routing.extension("a.b.JSON"), "json");
        assertEquals(Routing.extension("noext"), "");
        assertEquals(List.of(Routing.UNROUTED_KEY, Routing.typeKey("T")), List.of("unrouted", "type:T"));
    }

    private static EventParser.Parsed parsed(String api, String blobType) {
        return new EventParser.Parsed("Microsoft.Storage.BlobCreated", false, "acct", "c", "p", "https://x/c/p",
                null, api, null, null, blobType, null, "1");
    }
}
