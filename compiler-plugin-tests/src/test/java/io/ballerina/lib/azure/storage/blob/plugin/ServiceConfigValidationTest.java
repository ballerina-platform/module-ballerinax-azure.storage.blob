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
 * The {@code @blob:ServiceConfig} rules at compile time: the annotation is required, the queue
 * names follow Azure's rules, the default poison queue name fits, the two names differ, and the
 * consumption settings given as literals or constants stay in range.
 */
public class ServiceConfigValidationTest {

    @Test
    public void testValidServiceConfigFull() {
        assertNoErrors(loadPackage("valid_service_config_full"), "valid_service_config_full");
    }

    @Test
    public void testValidServiceConfigConstQueue() {
        assertNoErrors(loadPackage("valid_service_config_const_queue"), "valid_service_config_const_queue");
    }

    @Test
    public void testValidServiceConfigConfigurableQueue() {
        assertNoErrors(loadPackage("valid_service_config_configurable_queue"),
                "valid_service_config_configurable_queue");
    }

    @Test
    public void testInvalidMissingServiceConfig() {
        DiagnosticResult result = loadPackage("invalid_missing_service_config");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_119", "Missing '@blob:ServiceConfig'");
    }

    @Test
    public void testInvalidQueueNames() {
        for (String fixture : new String[]{"invalid_queue_name_uppercase", "invalid_queue_name_too_short",
                "invalid_queue_name_double_hyphen"}) {
            DiagnosticResult result = loadPackage(fixture);
            assertEquals(result.errorCount(), 1, fixture + ": " + describe(result));
            assertError(result, 0, "AZURE_BLOB_120", "Invalid queueName");
        }
        DiagnosticResult poison = loadPackage("invalid_poison_queue_name");
        assertEquals(poison.errorCount(), 1, describe(poison));
        assertError(poison, 0, "AZURE_BLOB_120", "Invalid poisonQueueName 'Dead'");
    }

    @Test
    public void testInvalidQueueNameLongWithoutPoison() {
        DiagnosticResult result = loadPackage("invalid_queue_name_long_without_poison");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_121", "Missing 'poisonQueueName'");
    }

    @Test
    public void testInvalidPoisonEqualsQueue() {
        DiagnosticResult result = loadPackage("invalid_poison_equals_queue");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_122", "must differ from 'queueName'");
    }

    @Test
    public void testInvalidSettingValues() {
        String[][] cases = {
                {"invalid_batch_size_zero", "Invalid 'batchSize' 0. Expected 1 to 32"},
                {"invalid_batch_size_33", "Invalid 'batchSize' 33. Expected 1 to 32"},
                {"invalid_new_batch_threshold_above_batch",
                        "Invalid 'newBatchThreshold' 5. Expected 0 to batchSize (4)"},
                {"invalid_max_polling_interval_zero",
                        "Invalid 'maxPollingIntervalSeconds' 0. Expected a value above 0"},
                {"invalid_redelivery_delay_negative", "Invalid 'redeliveryDelaySeconds' -1. Expected 0 to 604800"},
                {"invalid_redelivery_delay_over_week", "Invalid 'redeliveryDelaySeconds' 604801. Expected 0 to 604800"},
                {"invalid_max_delivery_count_zero", "Invalid 'maxDeliveryCount' 0. Expected at least 1"},
        };
        for (String[] testCase : cases) {
            DiagnosticResult result = loadPackage(testCase[0]);
            assertEquals(result.errorCount(), 1, testCase[0] + ": " + describe(result));
            assertError(result, 0, "AZURE_BLOB_123", testCase[1]);
        }
    }

    private static void assertNoErrors(DiagnosticResult result, String fixture) {
        assertEquals(result.errorCount(), 0, "expected no diagnostics for " + fixture + ": " + describe(result));
    }

    private static String describe(DiagnosticResult result) {
        return result.errors().stream().map(Diagnostic::toString).collect(Collectors.joining("; "));
    }
}
