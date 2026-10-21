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

import io.ballerina.compiler.api.symbols.MethodSymbol;
import io.ballerina.compiler.syntax.tree.BasicLiteralNode;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.NodeList;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.Token;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.ballerina.compiler.syntax.tree.SyntaxKind.RESOURCE_ACCESSOR_DEFINITION;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CONTENT_HANDLERS;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_ATTACH_POINT;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_REMOTE_FUNCTION;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.NO_VALID_REMOTE_METHOD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.RESOURCE_FUNCTION_NOT_ALLOWED;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.getDiagnostic;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.getMethodSymbol;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.isRemoteFunction;

/**
 * Validates a listener service: it attaches with no name, carries a valid
 * {@code @blob:ServiceConfig}, declares at least one event handler, and every member is a
 * known handler of the right shape.
 */
public class ServiceValidator {

    /**
     * Validates a listener service's attach point and members.
     *
     * @param context the syntax node analysis context
     */
    public void validate(SyntaxNodeAnalysisContext context) {
        ServiceDeclarationNode serviceDeclarationNode = (ServiceDeclarationNode) context.node();
        validateAttachPoint(context, serviceDeclarationNode);
        List<Diagnostic> configDiagnostics = new ArrayList<>();
        ServiceConfigValidator.validate(context.semanticModel(), serviceDeclarationNode, configDiagnostics);
        configDiagnostics.forEach(context::reportDiagnostic);

        NodeList<Node> members = serviceDeclarationNode.members();
        List<FunctionDefinitionNode> contentMethods = new ArrayList<>();
        List<String> contentMethodNames = new ArrayList<>();
        FunctionDefinitionNode deletedMethod = null;

        for (Node node : members) {
            if (node.kind() == RESOURCE_ACCESSOR_DEFINITION) {
                context.reportDiagnostic(getDiagnostic(RESOURCE_FUNCTION_NOT_ALLOWED,
                        DiagnosticSeverity.ERROR, node.location()));
                continue;
            }
            if (node.kind() != SyntaxKind.OBJECT_METHOD_DEFINITION) {
                continue;
            }
            FunctionDefinitionNode functionDefinitionNode = (FunctionDefinitionNode) node;
            MethodSymbol methodSymbol = getMethodSymbol(context, functionDefinitionNode);
            if (methodSymbol == null) {
                continue;
            }
            Optional<String> functionName = methodSymbol.getName();
            if (functionName.isEmpty()) {
                continue;
            }
            String name = functionName.get();
            if (CONTENT_HANDLERS.contains(name)) {
                contentMethods.add(functionDefinitionNode);
                contentMethodNames.add(name);
            } else if (PluginConstants.ON_BLOB_DELETE_FUNC.equals(name)) {
                deletedMethod = functionDefinitionNode;
            } else if (PluginConstants.ON_ERROR_FUNC.equals(name)) {
                // onError is validated separately and deliberately not counted: a service
                // declaring only onError still fails the at-least-one-handler check.
                new OnErrorFunctionValidator(context, functionDefinitionNode).validate();
            } else if (isRemoteFunction(context, functionDefinitionNode)) {
                context.reportDiagnostic(getDiagnostic(INVALID_REMOTE_FUNCTION,
                        DiagnosticSeverity.ERROR, functionDefinitionNode.location(), name));
            }
        }

        if (contentMethods.isEmpty() && deletedMethod == null) {
            context.reportDiagnostic(getDiagnostic(NO_VALID_REMOTE_METHOD,
                    DiagnosticSeverity.ERROR, serviceDeclarationNode.location()));
            return;
        }

        for (int i = 0; i < contentMethods.size(); i++) {
            new ContentFunctionValidator(context, contentMethods.get(i), contentMethodNames.get(i)).validate();
        }
        if (deletedMethod != null) {
            new DeleteFunctionValidator(context, deletedMethod).validate();
        }
    }

    // A service attaches with no name: any attach point, a name or a path, is rejected.
    private void validateAttachPoint(SyntaxNodeAnalysisContext context, ServiceDeclarationNode service) {
        NodeList<Node> path = service.absoluteResourcePath();
        if (path.isEmpty()) {
            return;
        }
        List<String> segments = new ArrayList<>();
        for (Node node : path) {
            if (node.kind() == SyntaxKind.SLASH_TOKEN) {
                continue;
            }
            if (node instanceof BasicLiteralNode literal) {
                segments.add(unquote(literal.literalToken().text()));
            } else if (node instanceof Token token) {
                segments.add(token.text().replace("\\", ""));
            } else {
                segments.add(node.toSourceCode().strip());
            }
        }
        context.reportDiagnostic(getDiagnostic(INVALID_ATTACH_POINT, DiagnosticSeverity.ERROR,
                path.get(0).location(), String.join("/", segments)));
    }

    private static String unquote(String literal) {
        if (literal.length() >= 2 && literal.startsWith("\"") && literal.endsWith("\"")) {
            return literal.substring(1, literal.length() - 1);
        }
        return literal;
    }
}
