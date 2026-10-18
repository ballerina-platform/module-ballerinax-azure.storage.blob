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
import io.ballerina.compiler.api.symbols.ServiceDeclarationSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.compiler.syntax.tree.ModuleMemberDeclarationNode;
import io.ballerina.compiler.syntax.tree.ModulePartNode;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SimpleNameReferenceNode;
import io.ballerina.lib.azure.storage.blob.plugin.ServiceConfigValidator.QueueNames;
import io.ballerina.projects.Document;
import io.ballerina.projects.DocumentId;
import io.ballerina.projects.Module;
import io.ballerina.projects.ModuleId;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.CompilationAnalysisContext;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.DUPLICATE_QUEUE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.POISON_QUEUE_CYCLE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.getDiagnostic;

/**
 * The checks across the services of one listener: no two may name the same queue, and their
 * poison queues may not form a cycle. The services are grouped by the module-level listener
 * they are declared on; only literal queue names take part. Imperative attachments and
 * listeners shared across modules are checked at runtime.
 */
public class ListenerQueueAnalysisTask implements AnalysisTask<CompilationAnalysisContext> {

    @Override
    public void perform(CompilationAnalysisContext context) {
        Set<String> documentsWithErrors = new HashSet<>();
        for (Diagnostic diagnostic : context.compilation().diagnosticResult().errors()) {
            documentsWithErrors.add(diagnostic.location().lineRange().fileName());
        }
        for (ModuleId moduleId : context.currentPackage().moduleIds()) {
            Module module = context.currentPackage().module(moduleId);
            SemanticModel semanticModel = context.compilation().getSemanticModel(moduleId);
            Map<String, List<QueueNames>> byListener = new LinkedHashMap<>();
            for (DocumentId documentId : module.documentIds()) {
                Document document = module.document(documentId);
                if (documentsWithErrors.contains(document.name())) {
                    continue;
                }
                ModulePartNode root = document.syntaxTree().rootNode();
                for (ModuleMemberDeclarationNode member : root.members()) {
                    if (!(member instanceof ServiceDeclarationNode service) || !isBlobService(semanticModel, service)) {
                        continue;
                    }
                    String listener = listenerName(service);
                    if (listener == null) {
                        continue;
                    }
                    List<Diagnostic> ignored = new ArrayList<>();
                    QueueNames names = ServiceConfigValidator.validate(semanticModel, service, ignored);
                    if (names != null) {
                        byListener.computeIfAbsent(listener, key -> new ArrayList<>()).add(names);
                    }
                }
            }
            for (Map.Entry<String, List<QueueNames>> entry : byListener.entrySet()) {
                check(context, entry.getKey(), entry.getValue());
            }
        }
    }

    // Reports a duplicate queue on both services, and a cycle at the service that closes it.
    private static void check(CompilationAnalysisContext context, String listener, List<QueueNames> services) {
        Map<String, Integer> firstByQueue = new HashMap<>();
        Set<Integer> duplicates = new HashSet<>();
        for (int i = 0; i < services.size(); i++) {
            Integer first = firstByQueue.putIfAbsent(services.get(i).queueName(), i);
            if (first != null) {
                duplicates.add(first);
                duplicates.add(i);
            }
        }
        for (int index : duplicates) {
            context.reportDiagnostic(getDiagnostic(DUPLICATE_QUEUE, DiagnosticSeverity.ERROR,
                    services.get(index).location(), services.get(index).queueName(), listener));
        }
        Map<String, String> poisonOf = new HashMap<>();
        for (QueueNames service : services) {
            poisonOf.put(service.queueName(), service.poisonQueueName());
            List<String> chain = new ArrayList<>();
            chain.add(service.queueName());
            String next = service.poisonQueueName();
            while (next != null && chain.size() <= poisonOf.size()) {
                chain.add(next);
                if (next.equals(service.queueName())) {
                    context.reportDiagnostic(getDiagnostic(POISON_QUEUE_CYCLE, DiagnosticSeverity.ERROR,
                            service.location(), listener, String.join(" -> ", chain)));
                    break;
                }
                next = poisonOf.get(next);
            }
        }
    }

    // The simple name of the one module-level listener the service is declared on.
    private static String listenerName(ServiceDeclarationNode service) {
        if (service.expressions().size() != 1) {
            return null;
        }
        ExpressionNode expression = service.expressions().get(0);
        return expression instanceof SimpleNameReferenceNode reference ? reference.name().text() : null;
    }

    private static boolean isBlobService(SemanticModel semanticModel, ServiceDeclarationNode service) {
        Optional<Symbol> symbol = semanticModel.symbol(service);
        return symbol.isPresent() && symbol.get() instanceof ServiceDeclarationSymbol declaration
                && ServiceAnalysisTask.attachesToBlobListener(declaration);
    }
}
