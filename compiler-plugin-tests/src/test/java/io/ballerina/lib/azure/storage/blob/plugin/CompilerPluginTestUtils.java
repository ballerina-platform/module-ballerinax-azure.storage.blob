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

import io.ballerina.projects.DiagnosticResult;
import io.ballerina.projects.Package;
import io.ballerina.projects.PackageCompilation;
import io.ballerina.projects.ProjectEnvironmentBuilder;
import io.ballerina.projects.directory.BuildProject;
import io.ballerina.projects.environment.Environment;
import io.ballerina.projects.environment.EnvironmentBuilder;
import io.ballerina.tools.diagnostics.Diagnostic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Compiles the fixture packages against the installed distribution and the module in the
 * local repository, and asserts on the diagnostics.
 */
final class CompilerPluginTestUtils {

    static final Path RESOURCE_DIRECTORY = resourceDirectory();

    private CompilerPluginTestUtils() {
    }

    private static Path resourceDirectory() {
        String dir = System.getProperty("fixtures.dir");
        if (dir == null || dir.isBlank()) {
            throw new IllegalStateException(
                    "set -Dfixtures.dir to the generated fixture directory (the gradle prepareTestFixtures task)");
        }
        return Paths.get(dir).toAbsolutePath();
    }

    static DiagnosticResult loadPackage(String path) {
        Path projectDirPath = RESOURCE_DIRECTORY.resolve(path);
        BuildProject project = BuildProject.load(getEnvironmentBuilder(), projectDirPath);
        Package currentPackage = project.currentPackage();
        PackageCompilation compilation = currentPackage.getCompilation();
        return compilation.diagnosticResult();
    }

    static void assertError(DiagnosticResult result, int index, String expectedCode, String expectedMessage) {
        List<Diagnostic> errors = result.errors().stream().toList();
        assertTrue(errors.size() > index,
                "expected at least " + (index + 1) + " error(s), found " + errors.size());
        Diagnostic diagnostic = errors.get(index);
        assertEquals(diagnostic.diagnosticInfo().code(), expectedCode,
                "unexpected diagnostic code: " + diagnostic.message());
        assertTrue(diagnostic.message().contains(expectedMessage),
                "expected message to contain '" + expectedMessage + "', found: " + diagnostic.message());
    }

    private static ProjectEnvironmentBuilder getEnvironmentBuilder() {
        Environment environment = EnvironmentBuilder.getBuilder().setBallerinaHome(distributionPath()).build();
        return ProjectEnvironmentBuilder.getBuilder(environment);
    }

    private static Path distributionPath() {
        String home = System.getProperty("ballerina.home");
        if (home == null || home.isBlank()) {
            throw new IllegalStateException(
                    "set -Dballerina.home to the Ballerina distribution directory (the gradle test task does)");
        }
        Path path = Paths.get(home);
        if (!Files.exists(path)) {
            throw new IllegalStateException("the Ballerina distribution was not found at: " + path);
        }
        return path;
    }
}
