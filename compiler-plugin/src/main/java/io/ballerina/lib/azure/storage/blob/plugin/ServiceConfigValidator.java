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
import io.ballerina.compiler.api.symbols.AnnotationSymbol;
import io.ballerina.compiler.api.symbols.ConstantSymbol;
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.syntax.tree.AnnotationNode;
import io.ballerina.compiler.syntax.tree.BasicLiteralNode;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingConstructorExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingFieldNode;
import io.ballerina.compiler.syntax.tree.MetadataNode;
import io.ballerina.compiler.syntax.tree.NameReferenceNode;
import io.ballerina.compiler.syntax.tree.ServiceDeclarationNode;
import io.ballerina.compiler.syntax.tree.SpecificFieldNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.UnaryExpressionNode;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;
import io.ballerina.tools.diagnostics.Location;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.BATCH_SIZE_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_QUEUE_NAME;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_SETTING_VALUE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.MISSING_SERVICE_CONFIG;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.POISON_QUEUE_EQUALS_QUEUE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.POISON_QUEUE_NAME_REQUIRED;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.MAX_BATCH_SIZE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.MAX_DELIVERY_COUNT_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.MAX_POLLING_INTERVAL_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.MAX_QUEUE_NAME_WITH_DEFAULT_POISON;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.MAX_REDELIVERY_DELAY;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.NEW_BATCH_THRESHOLD_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.POISON_QUEUE_NAME_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.POISON_SUFFIX;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.QUEUE_NAME;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.QUEUE_NAME_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.REDELIVERY_DELAY_FIELD;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.SERVICE_CONFIG_ANNOTATION;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.getDiagnostic;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginUtils.validateModuleId;

/**
 * Reads a service's {@code @blob:ServiceConfig} annotation and validates the values given as
 * literals or constants against the rules the runtime applies at attach: the queue name rules,
 * the default poison queue name's length, the two names differing, and the setting ranges. A
 * value that is not a literal is left to the runtime.
 */
final class ServiceConfigValidator {

    // The literal queue names of a service, for the cross-service checks.
    record QueueNames(String queueName, String poisonQueueName, Location location) {
    }

    private ServiceConfigValidator() {
    }

    /**
     * Validates the annotation and returns the diagnostics, with the literal queue names when
     * the annotation is present and names a queue.
     *
     * @param semanticModel the module's semantic model
     * @param service       the service declaration
     * @param diagnostics   where the diagnostics go
     * @return the literal queue names, or {@code null} when the queue name is not a literal
     */
    static QueueNames validate(SemanticModel semanticModel, ServiceDeclarationNode service,
                               List<Diagnostic> diagnostics) {
        Optional<AnnotationNode> annotation = findServiceConfig(semanticModel, service);
        if (annotation.isEmpty()) {
            diagnostics.add(getDiagnostic(MISSING_SERVICE_CONFIG, DiagnosticSeverity.ERROR, service.location()));
            return null;
        }
        Optional<MappingConstructorExpressionNode> value = annotation.get().annotValue();
        if (value.isEmpty()) {
            return null;
        }
        String queueName = null;
        String poisonQueueName = null;
        boolean poisonGiven = false;
        Long batchSize = null;
        Long newBatchThreshold = null;
        boolean queueValid = true;
        Location queueLocation = annotation.get().location();
        for (MappingFieldNode field : value.get().fields()) {
            if (!(field instanceof SpecificFieldNode specific) || specific.valueExpr().isEmpty()) {
                continue;
            }
            String name = specific.fieldName().toSourceCode().strip();
            ExpressionNode expression = specific.valueExpr().get();
            Object literal = literalValue(semanticModel, expression);
            switch (name) {
                case QUEUE_NAME_FIELD -> {
                    if (literal instanceof String text) {
                        queueName = text;
                        queueLocation = expression.location();
                        queueValid = checkQueueName(diagnostics, QUEUE_NAME_FIELD, text, expression.location());
                    }
                }
                case POISON_QUEUE_NAME_FIELD -> {
                    poisonGiven = true;
                    if (literal instanceof String text) {
                        poisonQueueName = text;
                        checkQueueName(diagnostics, POISON_QUEUE_NAME_FIELD, text, expression.location());
                    }
                }
                case BATCH_SIZE_FIELD -> {
                    if (literal instanceof Long number) {
                        batchSize = number;
                        if (number < 1 || number > MAX_BATCH_SIZE) {
                            diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR,
                                    expression.location(), BATCH_SIZE_FIELD, number, "1 to 32"));
                        }
                    }
                }
                case NEW_BATCH_THRESHOLD_FIELD -> {
                    if (literal instanceof Long number) {
                        newBatchThreshold = number;
                        if (number < 0) {
                            diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR,
                                    expression.location(), NEW_BATCH_THRESHOLD_FIELD, number, "0 to batchSize"));
                        }
                    }
                }
                case MAX_POLLING_INTERVAL_FIELD -> {
                    if (decimal(literal) instanceof BigDecimal number && number.signum() <= 0) {
                        diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR,
                                expression.location(), MAX_POLLING_INTERVAL_FIELD, number.toPlainString(),
                                "a value above 0"));
                    }
                }
                case REDELIVERY_DELAY_FIELD -> {
                    if (decimal(literal) instanceof BigDecimal number && (number.signum() < 0
                            || number.compareTo(BigDecimal.valueOf(MAX_REDELIVERY_DELAY)) > 0)) {
                        diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR,
                                expression.location(), REDELIVERY_DELAY_FIELD, number.toPlainString(),
                                "0 to 604800"));
                    }
                }
                case MAX_DELIVERY_COUNT_FIELD -> {
                    if (literal instanceof Long number && number < 1) {
                        diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR,
                                expression.location(), MAX_DELIVERY_COUNT_FIELD, number, "at least 1"));
                    }
                }
                default -> {
                    // Unknown fields are the compiler's to report.
                }
            }
        }
        if (batchSize != null && newBatchThreshold != null && newBatchThreshold > batchSize) {
            diagnostics.add(getDiagnostic(INVALID_SETTING_VALUE, DiagnosticSeverity.ERROR, value.get().location(),
                    NEW_BATCH_THRESHOLD_FIELD, newBatchThreshold, "0 to batchSize (" + batchSize + ")"));
        }
        if (queueName == null || !queueValid) {
            return null;
        }
        if (!poisonGiven && queueName.length() > MAX_QUEUE_NAME_WITH_DEFAULT_POISON) {
            diagnostics.add(getDiagnostic(POISON_QUEUE_NAME_REQUIRED, DiagnosticSeverity.ERROR, queueLocation,
                    queueName));
        }
        if (poisonQueueName != null && poisonQueueName.equals(queueName)) {
            diagnostics.add(getDiagnostic(POISON_QUEUE_EQUALS_QUEUE, DiagnosticSeverity.ERROR, value.get().location(),
                    queueName));
            return null;
        }
        String effectivePoison = poisonGiven ? poisonQueueName : queueName + POISON_SUFFIX;
        return new QueueNames(queueName, effectivePoison, service.location());
    }

    // A decimal field given as an integer literal.
    private static Object decimal(Object literal) {
        return literal instanceof Long number ? BigDecimal.valueOf(number) : literal;
    }

    private static boolean checkQueueName(List<Diagnostic> diagnostics, String field, String value,
                                          Location location) {
        if (QUEUE_NAME.matcher(value).matches()) {
            return true;
        }
        diagnostics.add(getDiagnostic(INVALID_QUEUE_NAME, DiagnosticSeverity.ERROR, location, field, value));
        return false;
    }

    // The module's ServiceConfig annotation on the service, resolved through the semantic
    // model so an import alias is honoured.
    private static Optional<AnnotationNode> findServiceConfig(SemanticModel semanticModel,
                                                              ServiceDeclarationNode service) {
        Optional<MetadataNode> metadata = service.metadata();
        if (metadata.isEmpty()) {
            return Optional.empty();
        }
        for (AnnotationNode annotation : metadata.get().annotations()) {
            Optional<Symbol> symbol = semanticModel.symbol(annotation);
            if (symbol.isPresent() && symbol.get() instanceof AnnotationSymbol annotationSymbol) {
                Optional<ModuleSymbol> module = annotationSymbol.getModule();
                if (module.isPresent() && validateModuleId(module.get())
                        && annotationSymbol.getName().map(SERVICE_CONFIG_ANNOTATION::equals).orElse(false)) {
                    return Optional.of(annotation);
                }
            }
        }
        return Optional.empty();
    }

    // A string, integer (Long) or decimal (BigDecimal) literal, a negated numeric literal, or a
    // constant reference's value; null for anything else.
    private static Object literalValue(SemanticModel semanticModel, ExpressionNode expression) {
        if (expression instanceof BasicLiteralNode literal) {
            return literal(literal.literalToken().text(), literal.kind());
        }
        if (expression instanceof UnaryExpressionNode unary && unary.unaryOperator().kind() == SyntaxKind.MINUS_TOKEN
                && unary.expression() instanceof BasicLiteralNode literal) {
            Object value = literal(literal.literalToken().text(), literal.kind());
            if (value instanceof Long number) {
                return -number;
            }
            if (value instanceof BigDecimal number) {
                return number.negate();
            }
            return null;
        }
        if (expression instanceof NameReferenceNode) {
            Optional<Symbol> symbol = semanticModel.symbol(expression);
            if (symbol.isPresent() && symbol.get() instanceof ConstantSymbol constant) {
                Object value = constant.constValue();
                if (value instanceof String || value instanceof Long) {
                    return value;
                }
                if (value instanceof Number number) {
                    return new BigDecimal(number.toString());
                }
                return value == null ? null : value.toString();
            }
        }
        return null;
    }

    private static Object literal(String text, SyntaxKind kind) {
        if (kind == SyntaxKind.STRING_LITERAL) {
            return text.length() >= 2 ? text.substring(1, text.length() - 1) : text;
        }
        if (kind == SyntaxKind.NUMERIC_LITERAL) {
            String digits = text.replace("_", "");
            try {
                if (digits.matches("-?\\d+")) {
                    return Long.parseLong(digits);
                }
                return new BigDecimal(digits.replaceAll("[dDfF]$", ""));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

}
