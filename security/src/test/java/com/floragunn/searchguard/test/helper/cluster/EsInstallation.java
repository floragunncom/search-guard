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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.documents.Document;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration.NodeSettings;
import com.floragunn.searchguard.test.helper.cluster.EsDownload.EsInstallationUnavailableException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class EsInstallation {
    private static final Logger log = LogManager.getLogger(EsInstallation.class);

    private final File path;
    private final File configPath;
    private final String esVersion;
    private final ExecutorService executorService;

    EsInstallation(File path, String esVersion, ExecutorService executorService) {
        this.path = path;
        this.configPath = new File(path, "config");
        this.esVersion = esVersion;
        this.executorService = executorService;
    }

    public void installPlugin(File plugin) throws EsInstallationUnavailableException {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();

            processBuilder.command("bin/elasticsearch-plugin", "install", "-v", "-b", "file:///" + plugin.getAbsolutePath());
            processBuilder.directory(path);
            Process process = processBuilder.start();

            StringBuilder result = new StringBuilder();

            executorService.submit(() -> {
                new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach((l) -> result.append(l).append("\n"));
            });

            int rc = process.waitFor();

            if (rc != 0) {
                log.info("{}", result);
                throw new Exception("Command failed with rc " + rc);
            }
        } catch (Exception e) {
            throw new EsInstallationUnavailableException("Error while installing " + plugin, e);
        }
    }

    public synchronized void ensureKeystore() throws EsInstallationUnavailableException {
        try {
            if (new File(configPath, "elasticsearch.keystore").exists()) {
                return;
            }

            KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
            keyStoreWrapper.save(configPath.toPath(), new char[0]);
        } catch (Exception e) {
            throw new EsInstallationUnavailableException("Error while creating default key store", e);
        }
    }

    public void appendConfig(String name, String content) throws EsInstallationUnavailableException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(configPath, name), true))) {
            writer.newLine();
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            throw new EsInstallationUnavailableException("Error while writing configuration to " + name, e);
        }
    }

    public void writeConfig(String name, String content) throws EsInstallationUnavailableException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(configPath, name)))) {
            writer.write(content);
        } catch (IOException e) {
            throw new EsInstallationUnavailableException("Error while writing configuration to " + name, e);
        }
    }

    public void writeConfig(String name, File file) throws EsInstallationUnavailableException {
        try {
            writeConfig(name, Files.asCharSource(file, Charsets.UTF_8).read());
        } catch (IOException e) {
            throw new EsInstallationUnavailableException("Error while reading configuration from " + file, e);
        }
    }

    public void initSgConfig(Map<String, Document<?>> config) throws EsInstallationUnavailableException {
        File sgConfigDir = new File(path, "plugins/search-guard-flx/sgconfig/");

        for (File file : sgConfigDir.listFiles()) {
            if (file.isFile()) {
                file.delete();
            }
        }

        for (Map.Entry<String, Document<?>> entry : config.entrySet()) {
            File file = new File(sgConfigDir, "sg_" + entry.getKey() + ".yml");

            try {
                Files.asCharSink(file, Charsets.UTF_8).write(entry.getValue().toYamlString());
            } catch (IOException e) {
                throw new EsInstallationUnavailableException("Error while writing " + file, e);
            }
        }
    }

    public Process startProcess(int httpPort, int transportPort, File workingDir, Settings settings, NodeSettings nodeSettings)
            throws EsInstallationUnavailableException {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            File dataDir = new File(workingDir, "data");
            dataDir.mkdir();
            File logsDir = new File(workingDir, "logs");
            logsDir.mkdir();
            Map<String, String> env = processBuilder.environment();
            env.remove("path.home");

            List<String> command = new ArrayList<>();
            command.add("bin/elasticsearch");
            command.add("-Ehttp.port=" + httpPort);
            command.add("-Etransport.tcp.port=" + transportPort);

            for (String key : settings.keySet()) {
                List<String> asList = settings.getAsList(key);

                if (asList.size() == 1) {
                    command.add("-E" + key + "=" + asList.get(0));
                } else {
                    command.add("-E" + key + "=" + asList.stream().collect(Collectors.joining(",")));
                }

            }

            if (nodeSettings.masterNode && nodeSettings.dataNode) {
                command.add("-Enode.roles=master,data");
            } else if (nodeSettings.masterNode) {
                command.add("-Enode.roles=master");
            } else if (nodeSettings.dataNode) {
                command.add("-Enode.roles=data");
            }

            command.add("-Epath.data=" + dataDir.getAbsolutePath());
            command.add("-Epath.logs=" + logsDir.getAbsolutePath());

            log.info("Executing {}", command);

            processBuilder.command(command);
            processBuilder.directory(path);
            Process process = processBuilder.start();

            return process;
        } catch (Exception e) {
            throw new EsInstallationUnavailableException("Error while stating " + this, e);
        }
    }

    @Override
    public String toString() {
        return "EsInstallation [path=" + path + ", esVersion=" + esVersion + "]";
    }

    public File getConfigPath() {
        return configPath;
    }

}
