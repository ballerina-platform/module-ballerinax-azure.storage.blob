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
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.ServiceDeclarationSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.List;
import java.util.Optional;

import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.validateModuleId;

/**
 * Validates the services attached to this module's {@code Listener}; services on any other
 * listener are left alone, as are sources that already carry compile errors.
 */
public class ServiceAnalysisTask implements AnalysisTask<SyntaxNodeAnalysisContext> {

    private final ServiceValidator serviceValidator = new ServiceValidator();

    @Override
    public void perform(SyntaxNodeAnalysisContext context) {
        for (Diagnostic diagnostic : context.semanticModel().diagnostics()) {
            if (diagnostic.diagnosticInfo().severity() == DiagnosticSeverity.ERROR) {
                return;
            }
        }
        if (!isAzureBlobService(context)) {
            return;
        }
        serviceValidator.validate(context);
    }

    // True when every listener the service attaches to is this module's Listener.
    private boolean isAzureBlobService(SyntaxNodeAnalysisContext context) {
        SemanticModel semanticModel = context.semanticModel();
        ServiceDeclarationNode serviceDeclarationNode = (ServiceDeclarationNode) context.node();
        Optional<Symbol> symbol = semanticModel.symbol(serviceDeclarationNode);
        if (symbol.isEmpty()) {
            return false;
        }
        List<TypeSymbol> listeners = ((ServiceDeclarationSymbol) symbol.get()).listenerTypes();
        if (listeners.isEmpty()) {
            return false;
        }
        for (TypeSymbol listener : listeners) {
            if (!isAzureBlobListener(listener)) {
                return false;
            }
        }
        return true;
    }

    private boolean isAzureBlobListener(TypeSymbol listener) {
        if (listener.typeKind() == TypeDescKind.UNION) {
            for (TypeSymbol member : ((UnionTypeSymbol) listener).memberTypeDescriptors()) {
                Optional<ModuleSymbol> module = member.getModule();
                if (module.isPresent() && validateModuleId(module.get())) {
                    return true;
                }
            }
            return false;
        }
        Optional<ModuleSymbol> module = listener.getModule();
        return module.isPresent() && validateModuleId(module.get());
    }
}
