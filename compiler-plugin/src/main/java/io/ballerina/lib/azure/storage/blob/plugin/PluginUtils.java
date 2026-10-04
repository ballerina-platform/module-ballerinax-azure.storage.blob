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
import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.ParameterSymbol;
import io.ballerina.compiler.api.symbols.Qualifier;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeReferenceTypeSymbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.api.symbols.UnionTypeSymbol;
import io.ballerina.compiler.syntax.tree.FunctionDefinitionNode;
import io.ballerina.compiler.syntax.tree.ParameterNode;
import io.ballerina.compiler.syntax.tree.RequiredParameterNode;
import io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.Diagnostic;
import io.ballerina.tools.diagnostics.DiagnosticFactory;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;
import io.ballerina.tools.diagnostics.Location;

import java.util.Optional;

import static io.ballerina.compiler.api.symbols.TypeDescKind.TYPE_REFERENCE;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.BLOB_EVENT;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CALLER;
import static io.ballerina.lib.azure.storage.blob.plugin.PluginConstants.CompilationErrors.INVALID_RETURN_TYPE_ERROR_OR_NIL;

/**
 * Shared helpers of the validators: diagnostics, symbol lookups, and the parameter and return
 * type checks every handler shares.
 */
public final class PluginUtils {

    private PluginUtils() {
    }

    public static void reportErrorDiagnostic(SyntaxNodeAnalysisContext context, CompilationErrors error,
                                             Location location, Object... args) {
        context.reportDiagnostic(getDiagnostic(error, DiagnosticSeverity.ERROR, location, args));
    }

    public static Diagnostic getDiagnostic(CompilationErrors error, DiagnosticSeverity severity, Location location,
                                           Object... args) {
        String message = args.length == 0 ? error.getError() : String.format(error.getError(), args);
        DiagnosticInfo diagnosticInfo = new DiagnosticInfo(error.getErrorCode(), message, severity);
        return DiagnosticFactory.createDiagnostic(diagnosticInfo, location);
    }

    public static boolean validateModuleId(ModuleSymbol moduleSymbol) {
        if (moduleSymbol == null) {
            return false;
        }
        String moduleName = moduleSymbol.id().moduleName();
        String orgName = moduleSymbol.id().orgName();
        return moduleName.equals(PluginConstants.PACKAGE_PREFIX) && orgName.equals(PluginConstants.PACKAGE_ORG);
    }

    public static boolean isRemoteFunction(SyntaxNodeAnalysisContext context,
                                           FunctionDefinitionNode functionDefinitionNode) {
        MethodSymbol methodSymbol = getMethodSymbol(context, functionDefinitionNode);
        return methodSymbol != null && methodSymbol.qualifiers().contains(Qualifier.REMOTE);
    }

    public static MethodSymbol getMethodSymbol(SyntaxNodeAnalysisContext context,
                                               FunctionDefinitionNode functionDefinitionNode) {
        Optional<Symbol> symbol = context.semanticModel().symbol(functionDefinitionNode);
        return symbol.map(value -> (MethodSymbol) value).orElse(null);
    }

    /** True when the parameter is the connector's {@code BlobEvent}, through any alias. */
    public static boolean validateEventParameter(ParameterNode parameterNode, SyntaxNodeAnalysisContext context) {
        return validateQualifiedParameter(parameterNode, context, BLOB_EVENT);
    }

    /** True when the parameter is the connector's {@code Caller}, through any alias. */
    public static boolean validateCallerParameter(ParameterNode parameterNode, SyntaxNodeAnalysisContext context) {
        return validateQualifiedParameter(parameterNode, context, CALLER);
    }

    private static boolean validateQualifiedParameter(ParameterNode parameterNode, SyntaxNodeAnalysisContext context,
                                                      String expectedTypeName) {
        if (!(parameterNode instanceof RequiredParameterNode)) {
            return false;
        }
        // Resolved through the type rather than the syntax, so a local alias of the module's type
        // is accepted: naming a type is ordinary Ballerina, and the runtime already resolves it.
        Optional<TypeSymbol> typeSymbol = getParameterTypeSymbol(parameterNode, context);
        if (typeSymbol.isEmpty()) {
            return false;
        }
        TypeSymbol resolved = typeSymbol.get();
        // Bounded and null-guarded: erroneous sources can expose cyclic or unresolved reference
        // chains, and this walk must never hang or crash the analysis they run under.
        for (int depth = 0; resolved != null && depth < PluginConstants.MAX_TYPE_REFERENCE_DEPTH; depth++) {
            Optional<ModuleSymbol> moduleSymbol = resolved.getModule();
            if (moduleSymbol.isPresent() && validateModuleId(moduleSymbol.get())
                    && resolved.getName().map(expectedTypeName::equals).orElse(false)) {
                return true;
            }
            if (!(resolved instanceof TypeReferenceTypeSymbol reference)) {
                return false;
            }
            resolved = reference.typeDescriptor();
        }
        return false;
    }

    public static Optional<TypeSymbol> getParameterTypeSymbol(ParameterNode parameterNode,
                                                              SyntaxNodeAnalysisContext context) {
        if (!(parameterNode instanceof RequiredParameterNode requiredParameterNode)) {
            return Optional.empty();
        }
        Optional<Symbol> symbol = context.semanticModel().symbol(requiredParameterNode);
        if (symbol.isEmpty() || !(symbol.get() instanceof ParameterSymbol parameterSymbol)) {
            return Optional.empty();
        }
        return Optional.ofNullable(parameterSymbol.typeDescriptor());
    }

    public static String getParameterTypeSignature(ParameterNode parameterNode, SyntaxNodeAnalysisContext context) {
        return getParameterTypeSymbol(parameterNode, context).map(TypeSymbol::signature).orElse("unknown");
    }

    /**
     * Unwraps a type reference chain to the type it names, bounded so an erroneous source
     * cannot hang the analysis; an unresolved reference is returned as is.
     */
    public static TypeSymbol unwrapTypeReference(TypeSymbol typeSymbol) {
        TypeSymbol resolved = typeSymbol;
        for (int depth = 0; depth < PluginConstants.MAX_TYPE_REFERENCE_DEPTH; depth++) {
            if (resolved.typeKind() != TYPE_REFERENCE
                    || !(resolved instanceof TypeReferenceTypeSymbol typeReferenceTypeSymbol)) {
                return resolved;
            }
            TypeSymbol referred = typeReferenceTypeSymbol.typeDescriptor();
            if (referred == null) {
                return resolved;
            }
            resolved = referred;
        }
        return resolved;
    }

    /**
     * Validates that a handler's return type is {@code error?} (nil, an error, or a union of
     * them), reporting a diagnostic otherwise.
     *
     * @param functionDefinitionNode the handler
     * @param context                the analysis context
     */
    public static void validateReturnTypeErrorOrNil(FunctionDefinitionNode functionDefinitionNode,
                                                    SyntaxNodeAnalysisContext context) {
        MethodSymbol methodSymbol = getMethodSymbol(context, functionDefinitionNode);
        if (methodSymbol == null) {
            return;
        }
        Optional<TypeSymbol> returnTypeDesc = methodSymbol.typeDescriptor().returnTypeDescriptor();
        if (returnTypeDesc.isEmpty()) {
            return;
        }
        TypeSymbol returnType = returnTypeDesc.get();
        TypeDescKind kind = returnType.typeKind();
        if (kind == TypeDescKind.NIL || isErrorType(returnType)) {
            return;
        }
        if (kind == TypeDescKind.UNION && returnType instanceof UnionTypeSymbol unionTypeSymbol) {
            for (TypeSymbol memberType : unionTypeSymbol.memberTypeDescriptors()) {
                if (memberType.typeKind() != TypeDescKind.NIL && !isErrorType(memberType)) {
                    context.reportDiagnostic(getDiagnostic(INVALID_RETURN_TYPE_ERROR_OR_NIL, DiagnosticSeverity.ERROR,
                            functionDefinitionNode.functionSignature().location()));
                    return;
                }
            }
            return;
        }
        context.reportDiagnostic(getDiagnostic(INVALID_RETURN_TYPE_ERROR_OR_NIL, DiagnosticSeverity.ERROR,
                functionDefinitionNode.functionSignature().location()));
    }

    private static boolean isErrorType(TypeSymbol typeSymbol) {
        return unwrapTypeReference(typeSymbol).typeKind() == TypeDescKind.ERROR;
    }
}
