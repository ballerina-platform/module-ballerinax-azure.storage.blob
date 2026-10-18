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
 * service configuration rules, and the diagnostic catalog.
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

    // The service configuration annotation and its fields.
    public static final String SERVICE_CONFIG_ANNOTATION = "ServiceConfig";
    public static final String QUEUE_NAME_FIELD = "queueName";
    public static final String POISON_QUEUE_NAME_FIELD = "poisonQueueName";
    public static final String BATCH_SIZE_FIELD = "batchSize";
    public static final String NEW_BATCH_THRESHOLD_FIELD = "newBatchThreshold";
    public static final String MAX_POLLING_INTERVAL_SECONDS_FIELD = "maxPollingIntervalSeconds";
    public static final String REDELIVERY_DELAY_SECONDS_FIELD = "redeliveryDelaySeconds";
    public static final String MAX_DELIVERY_COUNT_FIELD = "maxDeliveryCount";

    // Azure's queue name rule, the length a queue name may have before its default poison queue
    // name exceeds the limit, and the ranges of the consumption settings; the runtime applies
    // the same rules at attach.
    public static final Pattern QUEUE_NAME = Pattern.compile("^(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$");
    public static final String POISON_SUFFIX = "-poison";
    public static final int MAX_QUEUE_NAME_WITH_DEFAULT_POISON = 56;
    public static final int MAX_BATCH_SIZE = 32;
    public static final long MAX_REDELIVERY_DELAY_SECONDS = 604_800;

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
        INVALID_RETURN_TYPE_ERROR_OR_NIL("Invalid return type. Expected 'error?' or 'blob:Error?'; a handler may "
                + "also name 'blob:DeadLetterError' and 'blob:RetryError'.", "AZURE_BLOB_110"),
        INVALID_ATTACH_POINT("Invalid attach point '%s'. A service attaches with no name; it names its queue in "
                + "'@blob:ServiceConfig'.", "AZURE_BLOB_111"),
        INVALID_ON_ERROR_FIRST_PARAMETER("Invalid parameter for 'onError'. The first parameter must be "
                + "'error' or 'blob:Error'.", "AZURE_BLOB_112"),
        INVALID_ON_ERROR_SECOND_PARAMETER("Invalid parameter for 'onError'. Optional second parameter must be "
                + "'BlobEvent?' or 'Caller?'.", "AZURE_BLOB_113"),
        TOO_MANY_PARAMETERS_ON_ERROR("Too many parameters for 'onError'. It accepts at most 3 parameters: "
                + "(error, event?, caller?).", "AZURE_BLOB_114"),
        INVALID_DELETED_CALLER_PARAMETER("Invalid parameter for 'onBlobDeleted'. Optional second parameter must "
                + "be 'Caller'.", "AZURE_BLOB_115"),
        TOO_MANY_PARAMETERS_DELETED("Too many parameters for 'onBlobDeleted'. It accepts at most 2 parameters: "
                + "(event, caller?).", "AZURE_BLOB_116"),
        ON_ERROR_PARAMETER_NOT_NILABLE("Invalid parameter for 'onError'. The '%s' parameter must be nilable "
                + "('%s?'): it is '()' for a failure that carries none.", "AZURE_BLOB_117"),
        INVALID_ON_ERROR_THIRD_PARAMETER("Invalid parameter for 'onError'. Optional third parameter must be "
                + "'Caller?'.", "AZURE_BLOB_118"),
        MISSING_SERVICE_CONFIG("Missing '@blob:ServiceConfig'. A service names the queue it consumes in the "
                + "annotation's 'queueName'.", "AZURE_BLOB_119"),
        INVALID_QUEUE_NAME("Invalid %s '%s'. Expected 3 to 63 lowercase letters, digits and single hyphens, "
                + "beginning and ending with a letter or digit.", "AZURE_BLOB_120"),
        POISON_QUEUE_NAME_REQUIRED("Missing 'poisonQueueName'. The default '%s-poison' exceeds 63 characters.",
                "AZURE_BLOB_121"),
        POISON_QUEUE_EQUALS_QUEUE("Invalid 'poisonQueueName'. It must differ from 'queueName' ('%s').",
                "AZURE_BLOB_122"),
        INVALID_SETTING_VALUE("Invalid '%s' %s. Expected %s.", "AZURE_BLOB_123"),
        DUPLICATE_QUEUE("Duplicate queue '%s'. Another service of listener '%s' already consumes it.",
                "AZURE_BLOB_124"),
        POISON_QUEUE_CYCLE("Poison queue cycle on listener '%s': %s.", "AZURE_BLOB_125");

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
