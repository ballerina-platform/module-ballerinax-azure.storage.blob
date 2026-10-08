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

import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;

import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.BLOB_EVENT;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.CONTENT_METHOD_MUST_BE_REMOTE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_CONTENT_PARAMETER_TYPE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_DELETED_CALLER_PARAMETER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.MANDATORY_PARAMETER_NOT_FOUND;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.TOO_MANY_PARAMETERS_DELETED;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.ON_BLOB_DELETED_FUNC;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.isRemoteFunction;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.reportErrorDiagnostic;

/**
 * Validates the deleted-event handler: remote, {@code (BlobEvent, Caller?)}, returning
 * {@code error?}.
 */
public class DeletedFunctionValidator {

    private final SyntaxNodeAnalysisContext context;
    private final FunctionDefinitionNode funcDefinitionNode;

    public DeletedFunctionValidator(SyntaxNodeAnalysisContext context, FunctionDefinitionNode funcDefinitionNode) {
        this.context = context;
        this.funcDefinitionNode = funcDefinitionNode;
    }

    public void validate() {
        if (!isRemoteFunction(context, funcDefinitionNode)) {
            reportErrorDiagnostic(context, CONTENT_METHOD_MUST_BE_REMOTE, funcDefinitionNode.location(),
                    ON_BLOB_DELETED_FUNC);
        }
        SeparatedNodeList<ParameterNode> parameters = funcDefinitionNode.functionSignature().parameters();
        if (parameters.isEmpty()) {
            reportErrorDiagnostic(context, MANDATORY_PARAMETER_NOT_FOUND, funcDefinitionNode.location(),
                    ON_BLOB_DELETED_FUNC, BLOB_EVENT);
        } else if (parameters.size() > 2) {
            reportErrorDiagnostic(context, TOO_MANY_PARAMETERS_DELETED, funcDefinitionNode.location());
        } else {
            ParameterNode first = parameters.get(0);
            if (!PluginUtils.validateEventParameter(first, context)) {
                reportErrorDiagnostic(context, INVALID_CONTENT_PARAMETER_TYPE, first.location(),
                        ON_BLOB_DELETED_FUNC, BLOB_EVENT, PluginUtils.getParameterTypeSignature(first, context));
            }
            if (parameters.size() == 2 && !PluginUtils.validateCallerParameter(parameters.get(1), context)) {
                reportErrorDiagnostic(context, INVALID_DELETED_CALLER_PARAMETER, parameters.get(1).location());
            }
        }
        PluginUtils.validateReturnTypeErrorOrNil(funcDefinitionNode, context);
    }
}
