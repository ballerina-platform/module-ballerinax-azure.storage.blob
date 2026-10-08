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

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Constants for the {@code azure.storage.blob} listener compiler plugin: the module identity
 * used to recognize the connector's listener, the handler names, the parameter type names, the
 * attach-point rule, and the diagnostic catalog.
 */
public final class PluginConstants {

    private PluginConstants() {
    }

    // The listener's package identity (org and module name).
    public static final String PACKAGE_ORG = "ballerinax";
    public static final String PACKAGE_PREFIX = "azure.storage.blob";

    // Bounds every walk over a type-reference chain. The plugin also analyzes in-progress and
    // erroneous sources, whose semantic models can expose cyclic or unresolved reference chains;
    // an unbounded walk there hangs the analysis, and with it the IDE's language server.
    public static final int MAX_TYPE_REFERENCE_DEPTH = 8;

    // The content-handler function names.
    public static final String ON_BLOB_FUNC = "onBlob";
    public static final String ON_BLOB_TEXT_FUNC = "onBlobText";
    public static final String ON_BLOB_JSON_FUNC = "onBlobJson";
    public static final String ON_BLOB_XML_FUNC = "onBlobXml";
    public static final String ON_BLOB_CSV_FUNC = "onBlobCsv";
    // The deleted-event handler: a dispatchable handler, taking the event instead of content.
    public static final String ON_BLOB_DELETED_FUNC = "onBlobDeleted";

    // The optional error-notification handler name. Not an event handler: it does not count
    // toward the at-least-one-handler requirement.
    public static final String ON_ERROR_FUNC = "onError";

    /** The set of content-handler names. */
    public static final Set<String> CONTENT_HANDLERS = Set.of(
            ON_BLOB_FUNC, ON_BLOB_TEXT_FUNC, ON_BLOB_JSON_FUNC, ON_BLOB_XML_FUNC, ON_BLOB_CSV_FUNC);

    // Parameter type names.
    public static final String CALLER = "Caller";
    public static final String BLOB_EVENT = "BlobEvent";
    public static final String ERROR_TYPE = "Error";

    // The container attach point: Azure's container name rule, or the two special containers.
    public static final Pattern CONTAINER_NAME = Pattern.compile("^[a-z0-9](?:-?[a-z0-9]){2,62}$");
    public static final Set<String> SPECIAL_CONTAINERS = Set.of("$root", "$logs");

    /**
     * The diagnostics the plugin can report, each paired with its stable code. The messages are
     * plain text; {@code PluginUtils.getDiagnostic} escapes them for the {@code MessageFormat}
     * pass the diagnostic factory applies.
     */
    public enum CompilationErrors {
        INVALID_REMOTE_FUNCTION("Invalid remote method '%s'. A listener service allows only handlers: "
                + "onBlob, onBlobText, onBlobJson, onBlobXml, onBlobCsv, onBlobDeleted, onError.", "AZURE_BLOB_101"),
        RESOURCE_FUNCTION_NOT_ALLOWED("Unsupported resource function.", "AZURE_BLOB_102"),
        NO_VALID_REMOTE_METHOD("At least one handler must be added: onBlob, onBlobText, "
                + "onBlobJson, onBlobXml, onBlobCsv, or onBlobDeleted.", "AZURE_BLOB_103"),
        CONTENT_METHOD_MUST_BE_REMOTE("'%s' handler must be declared as remote.", "AZURE_BLOB_104"),
        MANDATORY_PARAMETER_NOT_FOUND("Missing parameter for '%s'. Expected '%s' as the first parameter.",
                "AZURE_BLOB_105"),
        INVALID_CONTENT_PARAMETER_TYPE("Invalid parameter type for '%s'. Expected '%s', found '%s'.",
                "AZURE_BLOB_106"),
        INVALID_EVENT_PARAMETER("Invalid parameter for '%s'. Optional second parameter must be 'BlobEvent', "
                + "or 'Caller' when it is the last.", "AZURE_BLOB_107"),
        INVALID_CALLER_PARAMETER("Invalid parameter for '%s'. Optional third parameter must be 'Caller'.",
                "AZURE_BLOB_108"),
        TOO_MANY_PARAMETERS("Too many parameters for '%s'. Handlers accept at most 3 parameters: "
                + "(content, event?, caller?).", "AZURE_BLOB_109"),
        INVALID_RETURN_TYPE_ERROR_OR_NIL("Invalid return type. Expected 'error?' or 'blob:Error?'.",
                "AZURE_BLOB_110"),
        INVALID_ATTACH_POINT("Invalid attach point '%s'. Expected one container name (3 to 63 lowercase letters, "
                + "digits and single hyphens), '$root', or '$logs'.", "AZURE_BLOB_111"),
        INVALID_ON_ERROR_FIRST_PARAMETER("Invalid parameter for 'onError'. The first parameter must be "
                + "'error' or 'blob:Error'.", "AZURE_BLOB_112"),
        INVALID_ON_ERROR_SECOND_PARAMETER("Invalid parameter for 'onError'. Optional second parameter must be "
                + "'Caller'.", "AZURE_BLOB_113"),
        TOO_MANY_PARAMETERS_ON_ERROR("Too many parameters for 'onError'. It accepts at most 2 parameters: "
                + "(error, caller?).", "AZURE_BLOB_114"),
        INVALID_DELETED_CALLER_PARAMETER("Invalid parameter for 'onBlobDeleted'. Optional second parameter must "
                + "be 'Caller'.", "AZURE_BLOB_115"),
        TOO_MANY_PARAMETERS_DELETED("Too many parameters for 'onBlobDeleted'. It accepts at most 2 parameters: "
                + "(event, caller?).", "AZURE_BLOB_116");

        private final String error;
        private final String errorCode;

        CompilationErrors(String error, String errorCode) {
            this.error = error;
            this.errorCode = errorCode;
        }

        String getError() {
            return error;
        }

        String getErrorCode() {
            return errorCode;
        }
    }
}
