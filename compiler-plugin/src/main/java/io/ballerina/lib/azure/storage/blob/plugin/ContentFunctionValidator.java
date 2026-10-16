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

import io.ballerina.compiler.api.symbols.ArrayTypeSymbol;
import io.ballerina.compiler.api.symbols.StreamTypeSymbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;

import java.util.Optional;

import static io.ballerina.compiler.api.symbols.TypeDescKind.ARRAY;
import static io.ballerina.compiler.api.symbols.TypeDescKind.BYTE;
import static io.ballerina.compiler.api.symbols.TypeDescKind.JSON;
import static io.ballerina.compiler.api.symbols.TypeDescKind.RECORD;
import static io.ballerina.compiler.api.symbols.TypeDescKind.STREAM;
import static io.ballerina.compiler.api.symbols.TypeDescKind.STRING;
import static io.ballerina.compiler.api.symbols.TypeDescKind.XML;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.CONTENT_METHOD_MUST_BE_REMOTE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_CALLER_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_CONTENT_PARAMETER_TYPE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_EVENT_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.MANDATORY_PARAMETER_NOT_FOUND;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.TOO_MANY_PARAMETERS;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_CSV_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_JSON_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_TEXT_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_XML_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.isRemoteFunction;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.reportErrorDiagnostic;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.unwrapTypeReference;

/**
 * Validates a content handler: remote, {@code (content, BlobEvent?, Caller?)} with the content
 * type its name admits, returning {@code error?}.
 */
public class ContentFunctionValidator {

    private final SyntaxNodeAnalysisContext context;
    private final FunctionDefinitionNode funcDefinitionNode;
    private final String contentMethodName;

    public ContentFunctionValidator(SyntaxNodeAnalysisContext context, FunctionDefinitionNode funcDefinitionNode,
                                    String contentMethodName) {
        this.context = context;
        this.funcDefinitionNode = funcDefinitionNode;
        this.contentMethodName = contentMethodName;
    }

    public void validate() {
        if (!isRemoteFunction(context, funcDefinitionNode)) {
            reportErrorDiagnostic(context, CONTENT_METHOD_MUST_BE_REMOTE, funcDefinitionNode.location(),
                    contentMethodName);
        }
        validateParameters(funcDefinitionNode.functionSignature().parameters());
        PluginUtils.validateReturnTypeErrorOrNil(funcDefinitionNode, context);
    }

    private void validateParameters(SeparatedNodeList<ParameterNode> parameters) {
        if (parameters.isEmpty()) {
            reportErrorDiagnostic(context, MANDATORY_PARAMETER_NOT_FOUND, funcDefinitionNode.location(),
                    contentMethodName, expectedContentType());
            return;
        }
        if (parameters.size() > 3) {
            reportErrorDiagnostic(context, TOO_MANY_PARAMETERS, funcDefinitionNode.location(), contentMethodName);
            return;
        }
        ParameterNode firstParameter = parameters.get(0);
        if (!validateContentParameter(firstParameter)) {
            reportErrorDiagnostic(context, INVALID_CONTENT_PARAMETER_TYPE, firstParameter.location(),
                    contentMethodName, expectedContentType(),
                    PluginUtils.getParameterTypeSignature(firstParameter, context));
        }
        if (parameters.size() == 1) {
            return;
        }
        if (parameters.size() == 2) {
            // A two-parameter handler takes either the event or the Caller second.
            if (!PluginUtils.validateEventParameter(parameters.get(1), context)
                    && !PluginUtils.validateCallerParameter(parameters.get(1), context)) {
                reportErrorDiagnostic(context, INVALID_EVENT_PARAMETER, parameters.get(1).location(),
                        contentMethodName);
            }
            return;
        }
        if (!PluginUtils.validateEventParameter(parameters.get(1), context)) {
            reportErrorDiagnostic(context, INVALID_EVENT_PARAMETER, parameters.get(1).location(), contentMethodName);
            return;
        }
        if (!PluginUtils.validateCallerParameter(parameters.get(2), context)) {
            reportErrorDiagnostic(context, INVALID_CALLER_PARAMETER, parameters.get(2).location(), contentMethodName);
        }
    }

    // Validates the declared type of the handler's first parameter against its content set.
    private boolean validateContentParameter(ParameterNode parameterNode) {
        Optional<TypeSymbol> typeSymbolOpt = PluginUtils.getParameterTypeSymbol(parameterNode, context);
        if (typeSymbolOpt.isEmpty()) {
            return false;
        }
        TypeSymbol typeSymbol = unwrapTypeReference(typeSymbolOpt.get());
        TypeDescKind typeKind = typeSymbol.typeKind();
        return switch (contentMethodName) {
            case ON_BLOB_FUNC -> isByteArray(typeSymbol) || isByteStream(typeSymbol);
            case ON_BLOB_TEXT_FUNC -> typeKind == STRING;
            case ON_BLOB_JSON_FUNC -> typeKind == JSON || typeKind == RECORD;
            case ON_BLOB_XML_FUNC -> typeKind == XML || typeKind == RECORD;
            case ON_BLOB_CSV_FUNC -> isStringMatrix(typeSymbol) || isRecordArray(typeSymbol) || isCsvStream(typeSymbol);
            default -> false;
        };
    }

    private static boolean isByteArray(TypeSymbol typeSymbol) {
        return typeSymbol.typeKind() == ARRAY
                && unwrapTypeReference(((ArrayTypeSymbol) typeSymbol).memberTypeDescriptor()).typeKind() == BYTE;
    }

    private static boolean isByteStream(TypeSymbol typeSymbol) {
        return typeSymbol.typeKind() == STREAM
                && isByteArray(unwrapTypeReference(((StreamTypeSymbol) typeSymbol).typeParameter()));
    }

    private static boolean isRecordArray(TypeSymbol typeSymbol) {
        return typeSymbol.typeKind() == ARRAY
                && unwrapTypeReference(((ArrayTypeSymbol) typeSymbol).memberTypeDescriptor()).typeKind() == RECORD;
    }

    private static boolean isStringArray(TypeSymbol typeSymbol) {
        return typeSymbol.typeKind() == ARRAY
                && unwrapTypeReference(((ArrayTypeSymbol) typeSymbol).memberTypeDescriptor()).typeKind() == STRING;
    }

    private static boolean isStringMatrix(TypeSymbol typeSymbol) {
        return typeSymbol.typeKind() == ARRAY
                && isStringArray(unwrapTypeReference(((ArrayTypeSymbol) typeSymbol).memberTypeDescriptor()));
    }

    private static boolean isCsvStream(TypeSymbol typeSymbol) {
        if (typeSymbol.typeKind() != STREAM) {
            return false;
        }
        TypeSymbol itemType = unwrapTypeReference(((StreamTypeSymbol) typeSymbol).typeParameter());
        return itemType.typeKind() == RECORD || isStringArray(itemType);
    }

    private String expectedContentType() {
        return switch (contentMethodName) {
            case ON_BLOB_FUNC -> "byte[] or stream<byte[], error?>";
            case ON_BLOB_TEXT_FUNC -> "string";
            case ON_BLOB_JSON_FUNC -> "json or a record";
            case ON_BLOB_XML_FUNC -> "xml or a record";
            case ON_BLOB_CSV_FUNC -> "string[][], record{}[], stream<string[], error?>, "
                    + "or stream<record{}, error?>";
            default -> "unknown";
        };
    }
}
