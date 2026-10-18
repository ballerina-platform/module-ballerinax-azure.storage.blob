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

import io.ballerina.compiler.api.SemanticModel;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDefinitionSymbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.OptionalParameter;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;

import java.util.Optional;

import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.BLOB_EVENT;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CALLER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ERROR_TYPE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_ERROR_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.PACKAGE_ORG;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.PACKAGE_PREFIX;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.CONTENT_METHOD_MUST_BE_REMOTE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_ON_ERROR_FIRST_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_ON_ERROR_SECOND_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_ON_ERROR_THIRD_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.ON_ERROR_PARAMETER_NOT_NILABLE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.TOO_MANY_PARAMETERS_ON_ERROR;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.isRemoteFunction;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.reportErrorDiagnostic;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.unwrapTypeReference;

/**
 * Validates the error notification handler: remote, {@code (error|blob:Error, BlobEvent?,
 * Caller?)} with the two optional parameters nilable and in that order, returning
 * {@code error?}.
 */
public class OnErrorFunctionValidator {

    private final SyntaxNodeAnalysisContext context;
    private final FunctionDefinitionNode functionDefinitionNode;

    public OnErrorFunctionValidator(SyntaxNodeAnalysisContext context, FunctionDefinitionNode functionDefinitionNode) {
        this.context = context;
        this.functionDefinitionNode = functionDefinitionNode;
    }

    public void validate() {
        if (!isRemoteFunction(context, functionDefinitionNode)) {
            reportErrorDiagnostic(context, CONTENT_METHOD_MUST_BE_REMOTE, functionDefinitionNode.location(),
                    ON_ERROR_FUNC);
            return;
        }
        SeparatedNodeList<ParameterNode> parameters = functionDefinitionNode.functionSignature().parameters();
        int paramCount = parameters.size();
        if (paramCount == 0) {
            reportErrorDiagnostic(context, INVALID_ON_ERROR_FIRST_PARAMETER, functionDefinitionNode.location());
            return;
        }
        if (paramCount > 3) {
            reportErrorDiagnostic(context, TOO_MANY_PARAMETERS_ON_ERROR, functionDefinitionNode.location());
            return;
        }
        validateErrorParameter(parameters.get(0));
        OptionalParameter second = paramCount >= 2
                ? PluginUtils.classifyOptionalParameter(parameters.get(1), context) : null;
        OptionalParameter third = paramCount == 3
                ? PluginUtils.classifyOptionalParameter(parameters.get(2), context) : null;
        if (second != null) {
            // The second is the event, or the Caller when it is the last; a Caller followed by
            // another Caller means the event was meant.
            boolean secondAllowed = second.isEvent() || (second.isCaller() && (third == null || !third.isCaller()));
            if (!secondAllowed) {
                reportErrorDiagnostic(context, INVALID_ON_ERROR_SECOND_PARAMETER, parameters.get(1).location());
            } else if (!second.nilable()) {
                reportErrorDiagnostic(context, ON_ERROR_PARAMETER_NOT_NILABLE, parameters.get(1).location(),
                        second.name(), second.isEvent() ? BLOB_EVENT : CALLER);
            }
        }
        if (third != null) {
            if (!third.isCaller()) {
                reportErrorDiagnostic(context, INVALID_ON_ERROR_THIRD_PARAMETER, parameters.get(2).location());
            } else if (!third.nilable()) {
                reportErrorDiagnostic(context, ON_ERROR_PARAMETER_NOT_NILABLE, parameters.get(2).location(),
                        third.name(), CALLER);
            }
        }
        PluginUtils.validateReturnTypeErrorOrNil(functionDefinitionNode, context);
    }

    // The first parameter is the bare error type or the module's Error, not a narrower error.
    private void validateErrorParameter(ParameterNode parameterNode) {
        Optional<TypeSymbol> paramType = PluginUtils.getParameterTypeSymbol(parameterNode, context);
        if (paramType.isEmpty()) {
            reportErrorDiagnostic(context, INVALID_ON_ERROR_FIRST_PARAMETER, parameterNode.location());
            return;
        }
        SemanticModel semanticModel = context.semanticModel();
        TypeSymbol normalizedParamType = unwrapTypeReference(paramType.get());
        boolean isError = normalizedParamType.subtypeOf(semanticModel.types().ERROR)
                && semanticModel.types().ERROR.subtypeOf(normalizedParamType);
        boolean isModuleError = findModuleErrorTypeSymbol(semanticModel)
                .map(PluginUtils::unwrapTypeReference)
                .map(moduleError -> normalizedParamType.subtypeOf(moduleError)
                        && moduleError.subtypeOf(normalizedParamType))
                .orElse(false);
        if (!isError && !isModuleError) {
            reportErrorDiagnostic(context, INVALID_ON_ERROR_FIRST_PARAMETER, parameterNode.location());
        }
    }

    private Optional<TypeSymbol> findModuleErrorTypeSymbol(SemanticModel semanticModel) {
        Optional<Symbol> errorSymbol = semanticModel.types().getTypeByName(PACKAGE_ORG, PACKAGE_PREFIX, "", ERROR_TYPE);
        if (errorSymbol.isEmpty()) {
            return Optional.empty();
        }
        Symbol symbol = errorSymbol.get();
        if (symbol instanceof TypeDefinitionSymbol typeDefinitionSymbol) {
            return Optional.of(typeDefinitionSymbol.typeDescriptor());
        }
        if (symbol instanceof TypeSymbol typeSymbol) {
            return Optional.of(typeSymbol);
        }
        return Optional.empty();
    }
}
