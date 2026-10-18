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

package io.ballerina.lib.azure.storage.blob.plugin;

import io.ballerina.projects.DiagnosticResult;
import io.ballerina.tools.diagnostics.Diagnostic;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static io.ballerina.lib.azure.storage.blob.plugin.CompilerPluginTestUtils.assertError;
import static io.ballerina.lib.azure.storage.blob.plugin.CompilerPluginTestUtils.loadPackage;
import static org.testng.Assert.assertEquals;

/**
 * The checks across the services of one listener: duplicate queues and poison queue cycles,
 * detected for literal queue names and grouped by the module-level listener.
 */
public class ListenerPackageValidationTest {

    @Test
    public void testValidTwoServicesTwoQueues() {
        assertNoErrors(loadPackage("valid_two_services_two_queues"), "valid_two_services_two_queues");
    }

    @Test
    public void testValidConsumesOtherPoison() {
        assertNoErrors(loadPackage("valid_consumes_other_poison"), "valid_consumes_other_poison");
    }

    @Test
    public void testValidSharedPoisonQueue() {
        assertNoErrors(loadPackage("valid_shared_poison_queue"), "valid_shared_poison_queue");
    }

    @Test
    public void testValidSameQueueTwoListeners() {
        assertNoErrors(loadPackage("valid_same_queue_two_listeners"), "valid_same_queue_two_listeners");
    }

    @Test
    public void testInvalidDuplicateQueue() {
        DiagnosticResult result = loadPackage("invalid_duplicate_queue");
        assertEquals(result.errorCount(), 2, describe(result));
        assertError(result, 0, "AZURE_BLOB_124", "Duplicate queue 'events'");
        assertError(result, 1, "AZURE_BLOB_124", "Duplicate queue 'events'");
    }

    @Test
    public void testInvalidPoisonCycleTwo() {
        DiagnosticResult result = loadPackage("invalid_poison_cycle_two");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_125", "queue-b -> queue-a -> queue-b");
    }

    @Test
    public void testInvalidPoisonCycleThree() {
        DiagnosticResult result = loadPackage("invalid_poison_cycle_three");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_125", "queue-c -> queue-a -> queue-b -> queue-c");
    }

    private static void assertNoErrors(DiagnosticResult result, String fixture) {
        assertEquals(result.errorCount(), 0, "expected no diagnostics for " + fixture + ": " + describe(result));
    }

    private static String describe(DiagnosticResult result) {
        return result.errors().stream().map(Diagnostic::toString).collect(Collectors.joining("; "));
    }
}
