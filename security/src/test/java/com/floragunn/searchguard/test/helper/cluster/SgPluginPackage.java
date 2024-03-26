/*
 * Copyright 2024 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.test.helper.cluster;

import java.io.File;
import java.util.concurrent.CompletableFuture;

import org.apache.maven.cli.MavenCli;

public class SgPluginPackage {
    private static final CompletableFuture<SgPluginPackage> result = new CompletableFuture<>();
    private static boolean building = false;

    public synchronized static CompletableFuture<SgPluginPackage> get() {

        if (result.isDone() || building) {
            return result;
        }

        building = true;

        String externalFile = System.getProperty("sg.tests.sg_plugin.file");

        if (externalFile != null) {
            if (!new File(externalFile).exists()) {
                result.completeExceptionally(new RuntimeException("File specified by sg.tests.sg_plugin.file does not exist: " + externalFile));
            } else {
                SgPluginPackage instance = new SgPluginPackage(new File(externalFile));
                result.complete(instance);
            }
        } else {
            new Thread(() -> {
                try {
                    SgPluginPackage instance = new SgPluginPackage(buildSgPluginFile());
                    result.complete(instance);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }).start();
        }

        return result;
    }

    private final File file;

    private SgPluginPackage(File file) {
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    private static File findProjectRootDir() {
        for (File dir = new File(".").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            if (new File(dir, "plugin").exists() && new File(dir, "pom.xml").exists()) {
                return dir;
            }
        }

        throw new RuntimeException("Could not find project root directory");
    }

    private static File buildSgPluginFile() {
        File projectRoot = findProjectRootDir();
        File pluginBuildTarget = new File(projectRoot, "plugin/target/releases/");

        if (pluginBuildTarget.exists()) {
            for (File f : pluginBuildTarget.listFiles()) {
                if (f.isFile()) {
                    f.delete();
                }
            }
        }

        System.setProperty("maven.multiModuleProjectDirectory", projectRoot.getAbsolutePath());
        MavenCli cli = new MavenCli();
        int rc = cli.doMain(new String[] { "--no-transfer-progress", "--quiet", "--batch-mode", "install", "-DskipTests=true", "-Pquick" },
                projectRoot.getAbsolutePath(), null, null);

        if (rc != 0) {
            throw new RuntimeException("mvn install failed");
        }

        for (File f : pluginBuildTarget.listFiles()) {
            if (f.isFile() && f.getName().startsWith("search-guard-flx-elasticsearch-plugin")) {
                return f;
            }
        }

        throw new RuntimeException("Could not find plugin file in " + pluginBuildTarget);
    }

}
