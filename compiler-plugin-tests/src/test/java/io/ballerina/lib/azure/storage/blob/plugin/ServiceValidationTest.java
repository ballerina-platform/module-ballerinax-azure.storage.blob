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
 * Compiles one fixture package per service shape the specification allows or refuses, and
 * checks the diagnostics the plugin reports.
 */
public class ServiceValidationTest {

    @Test
    public void testValidOnBlob() {
        DiagnosticResult result = loadPackage("valid_on_blob");
        assertNoErrors(result, "valid_on_blob");
    }

    @Test
    public void testValidOnBlobStream() {
        DiagnosticResult result = loadPackage("valid_on_blob_stream");
        assertNoErrors(result, "valid_on_blob_stream");
    }

    @Test
    public void testValidOnBlobText() {
        DiagnosticResult result = loadPackage("valid_on_blob_text");
        assertNoErrors(result, "valid_on_blob_text");
    }

    @Test
    public void testValidOnBlobJsonBare() {
        DiagnosticResult result = loadPackage("valid_on_blob_json_bare");
        assertNoErrors(result, "valid_on_blob_json_bare");
    }

    @Test
    public void testValidOnBlobJsonRecord() {
        DiagnosticResult result = loadPackage("valid_on_blob_json_record");
        assertNoErrors(result, "valid_on_blob_json_record");
    }

    @Test
    public void testValidOnBlobJsonRecordAliasChain() {
        DiagnosticResult result = loadPackage("valid_on_blob_json_record_alias_chain");
        assertNoErrors(result, "valid_on_blob_json_record_alias_chain");
    }

    @Test
    public void testValidOnBlobXmlBare() {
        DiagnosticResult result = loadPackage("valid_on_blob_xml_bare");
        assertNoErrors(result, "valid_on_blob_xml_bare");
    }

    @Test
    public void testValidOnBlobXmlRecord() {
        DiagnosticResult result = loadPackage("valid_on_blob_xml_record");
        assertNoErrors(result, "valid_on_blob_xml_record");
    }

    @Test
    public void testValidOnBlobCsvStringMatrix() {
        DiagnosticResult result = loadPackage("valid_on_blob_csv_string_matrix");
        assertNoErrors(result, "valid_on_blob_csv_string_matrix");
    }

    @Test
    public void testValidOnBlobCsvRecordArray() {
        DiagnosticResult result = loadPackage("valid_on_blob_csv_record_array");
        assertNoErrors(result, "valid_on_blob_csv_record_array");
    }

    @Test
    public void testValidOnBlobCsvStreamRecord() {
        DiagnosticResult result = loadPackage("valid_on_blob_csv_stream_record");
        assertNoErrors(result, "valid_on_blob_csv_stream_record");
    }

    @Test
    public void testValidOnBlobCsvStreamStringArray() {
        DiagnosticResult result = loadPackage("valid_on_blob_csv_stream_string_array");
        assertNoErrors(result, "valid_on_blob_csv_stream_string_array");
    }

    @Test
    public void testValidOnBlobDelete() {
        DiagnosticResult result = loadPackage("valid_on_blob_delete");
        assertNoErrors(result, "valid_on_blob_delete");
    }

    @Test
    public void testValidOnBlobDeleteWithCaller() {
        DiagnosticResult result = loadPackage("valid_on_blob_delete_with_caller");
        assertNoErrors(result, "valid_on_blob_delete_with_caller");
    }

    @Test
    public void testValidMixed() {
        DiagnosticResult result = loadPackage("valid_mixed");
        assertNoErrors(result, "valid_mixed");
    }

    @Test
    public void testValidFunctionConfig() {
        DiagnosticResult result = loadPackage("valid_function_config");
        assertNoErrors(result, "valid_function_config");
    }

    @Test
    public void testValidNonIsolated() {
        DiagnosticResult result = loadPackage("valid_non_isolated");
        assertNoErrors(result, "valid_non_isolated");
    }

    @Test
    public void testValidOnErrorWithEvent() {
        DiagnosticResult result = loadPackage("valid_on_error_with_event");
        assertNoErrors(result, "valid_on_error_with_event");
    }

    @Test
    public void testValidOnErrorFull() {
        DiagnosticResult result = loadPackage("valid_on_error_full");
        assertNoErrors(result, "valid_on_error_full");
    }

    @Test
    public void testValidReturnSettlementErrors() {
        DiagnosticResult result = loadPackage("valid_return_settlement_errors");
        assertNoErrors(result, "valid_return_settlement_errors");
    }

    @Test
    public void testValidOnError() {
        DiagnosticResult result = loadPackage("valid_on_error");
        assertNoErrors(result, "valid_on_error");
    }

    @Test
    public void testValidOnErrorBareError() {
        DiagnosticResult result = loadPackage("valid_on_error_bare_error");
        assertNoErrors(result, "valid_on_error_bare_error");
    }

    @Test
    public void testValidOnErrorWithCaller() {
        DiagnosticResult result = loadPackage("valid_on_error_with_caller");
        assertNoErrors(result, "valid_on_error_with_caller");
    }

    @Test
    public void testValidReturnErrorAlias() {
        DiagnosticResult result = loadPackage("valid_return_error_alias");
        assertNoErrors(result, "valid_return_error_alias");
    }

    @Test
    public void testValidContentTypeAliases() {
        DiagnosticResult result = loadPackage("valid_content_type_aliases");
        assertNoErrors(result, "valid_content_type_aliases");
    }

    @Test
    public void testInvalidResourceFunction() {
        DiagnosticResult result = loadPackage("invalid_resource_function");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_102", "Unsupported resource function");
    }

    @Test
    public void testInvalidUnknownRemote() {
        DiagnosticResult result = loadPackage("invalid_unknown_remote");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_101", "Invalid remote method 'onUpload'");
    }

    @Test
    public void testInvalidNoHandler() {
        DiagnosticResult result = loadPackage("invalid_no_handler");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_103", "At least one handler must be added");
    }

    @Test
    public void testInvalidOnErrorOnly() {
        DiagnosticResult result = loadPackage("invalid_on_error_only");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_103", "At least one handler must be added");
    }

    @Test
    public void testInvalidNonRemoteHandler() {
        DiagnosticResult result = loadPackage("invalid_non_remote_handler");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_104", "handler must be declared as remote");
    }

    @Test
    public void testInvalidMissingParameter() {
        DiagnosticResult result = loadPackage("invalid_missing_parameter");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_105", "Missing parameter for 'onBlob'");
    }

    @Test
    public void testInvalidContentType() {
        DiagnosticResult result = loadPackage("invalid_content_type");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_106", "Invalid parameter type for 'onBlob'");
    }

    @Test
    public void testInvalidOnBlobJsonMap() {
        DiagnosticResult result = loadPackage("invalid_on_blob_json_map");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_106", "Invalid parameter type for 'onBlobJson'");
    }

    @Test
    public void testInvalidOnBlobCsvScalarArray() {
        DiagnosticResult result = loadPackage("invalid_on_blob_csv_scalar_array");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_106", "Invalid parameter type for 'onBlobCsv'");
    }

    @Test
    public void testInvalidOnBlobStreamItem() {
        DiagnosticResult result = loadPackage("invalid_on_blob_stream_item");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_106", "Invalid parameter type for 'onBlob'");
    }

    @Test
    public void testInvalidSecondParameter() {
        DiagnosticResult result = loadPackage("invalid_second_parameter");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_107", "Optional second parameter must be 'BlobEvent'");
    }

    @Test
    public void testInvalidThirdParameter() {
        DiagnosticResult result = loadPackage("invalid_third_parameter");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_108", "Optional third parameter must be 'Caller'");
    }

    @Test
    public void testInvalidTooManyParameters() {
        DiagnosticResult result = loadPackage("invalid_too_many_parameters");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_109", "Too many parameters for 'onBlob'");
    }

    @Test
    public void testInvalidReturnType() {
        DiagnosticResult result = loadPackage("invalid_return_type");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_110", "Expected 'error?'");
    }

    @Test
    public void testInvalidReturnRecordRef() {
        DiagnosticResult result = loadPackage("invalid_return_record_ref");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_110", "Expected 'error?'");
    }

    @Test
    public void testInvalidAttachPointTwoSegments() {
        DiagnosticResult result = loadPackage("invalid_attach_point_two_segments");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_111",
                "Invalid attach point 'invoices/archive'. A service attaches with no name");
    }

    @Test
    public void testInvalidAttachPointLong() {
        DiagnosticResult result = loadPackage("invalid_attach_point_long");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_111", "Invalid attach point");
    }

    @Test
    public void testInvalidAttachPointName() {
        DiagnosticResult result = loadPackage("invalid_attach_point_name");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_111", "Invalid attach point 'Invoices'");
    }

    @Test
    public void testInvalidAttachPointString() {
        DiagnosticResult result = loadPackage("invalid_attach_point_string");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_111", "Invalid attach point '$root'");
    }

    @Test
    public void testInvalidOnErrorFirstParam() {
        DiagnosticResult result = loadPackage("invalid_on_error_first_param");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_112", "The first parameter must be");
    }

    @Test
    public void testInvalidOnErrorNarrowError() {
        DiagnosticResult result = loadPackage("invalid_on_error_narrow_error");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_112", "The first parameter must be");
    }

    @Test
    public void testInvalidOnErrorSecondParamNotNilable() {
        DiagnosticResult result = loadPackage("invalid_on_error_second_param");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_117", "The 'event' parameter must be nilable ('BlobEvent?')");
    }

    @Test
    public void testInvalidOnErrorSecondParamOfAnotherType() {
        DiagnosticResult result = loadPackage("invalid_on_error_second_string");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_113", "Optional second parameter must be 'BlobEvent?' or 'Caller?'");
    }

    @Test
    public void testInvalidOnErrorSecondNonNilableCaller() {
        DiagnosticResult result = loadPackage("invalid_on_error_second_non_nilable_caller");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_117", "The 'caller' parameter must be nilable ('Caller?')");
    }

    @Test
    public void testInvalidOnErrorThirdNotCaller() {
        DiagnosticResult result = loadPackage("invalid_on_error_third_not_caller");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_118", "Optional third parameter must be 'Caller?'");
    }

    @Test
    public void testInvalidOnErrorCallerThenEvent() {
        DiagnosticResult result = loadPackage("invalid_on_error_caller_then_event");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_118", "Optional third parameter must be 'Caller?'");
    }

    @Test
    public void testInvalidOnErrorTooManyParams() {
        DiagnosticResult result = loadPackage("invalid_on_error_too_many_params");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_114", "It accepts at most 3 parameters");
    }

    @Test
    public void testInvalidOnErrorNonRemote() {
        DiagnosticResult result = loadPackage("invalid_on_error_non_remote");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_104", "handler must be declared as remote");
    }

    @Test
    public void testInvalidOnErrorReturnType() {
        DiagnosticResult result = loadPackage("invalid_on_error_return_type");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_110", "Expected 'error?'");
    }

    @Test
    public void testInvalidDeletedFirstParam() {
        DiagnosticResult result = loadPackage("invalid_delete_first_param");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_106", "Invalid parameter type for 'onBlobDelete'");
    }

    @Test
    public void testInvalidDeletedSecondParam() {
        DiagnosticResult result = loadPackage("invalid_delete_second_param");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_115", "Optional second parameter must be 'Caller'");
    }

    @Test
    public void testInvalidDeletedTooManyParams() {
        DiagnosticResult result = loadPackage("invalid_delete_too_many_params");
        assertEquals(result.errorCount(), 1, describe(result));
        assertError(result, 0, "AZURE_BLOB_116", "Too many parameters for 'onBlobDelete'");
    }

    private static void assertNoErrors(DiagnosticResult result, String fixture) {
        assertEquals(result.errorCount(), 0, "expected no diagnostics for " + fixture + ": " + describe(result));
    }

    private static String describe(DiagnosticResult result) {
        return result.errors().stream().map(Diagnostic::toString).collect(Collectors.joining("; "));
    }
}
