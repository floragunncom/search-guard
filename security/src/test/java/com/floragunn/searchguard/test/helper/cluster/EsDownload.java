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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EsDownload {
    private static final Logger log = LogManager.getLogger(EsDownload.class);

    private static final Map<String, EsDownload> instancesByVersion = Collections.synchronizedMap(new HashMap<>());

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    static EsDownload get(String version) {
        return instancesByVersion.computeIfAbsent(version, (k) -> new EsDownload(version));
    }

    private final String esVersion;

    EsDownload(String esVersion) {
        this.esVersion = esVersion;
    }

    public synchronized File getReleaseArchive() throws EsInstallationUnavailableException {
        // This mirrors the directory scheme used by setup_test_instance.sh
        File downloadDirectory;

        if (System.getProperty("sg.tests.es_download_cache.dir") != null) {
            downloadDirectory = new File(System.getProperty("sg.tests.es_download_cache.dir"));
        } else {
            downloadDirectory = new File(FileUtils.getUserDirectory(), "searchguard-test/download-cache/");
        }

        if (!downloadDirectory.exists()) {
            downloadDirectory.mkdirs();
        }

        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();

        if (arch.equals("amd64")) {
            arch = "x86_64";
        }

        String esArchive = "elasticsearch-" + esVersion + "-" + os + "-" + arch + ".tar.gz";

        File downloadFile = new File(downloadDirectory, esArchive);

        if (!downloadFile.exists()) {
            try {
                URL url = new URL("https://artifacts.elastic.co/downloads/elasticsearch/" + esArchive);

                try {
                    log.info("Downloading {}", url);
                    FileUtils.copyURLToFile(url, downloadFile);
                    // Avoid weird issues with truncated archives:
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                } catch (IOException e) {
                    throw new EsInstallationUnavailableException("Error while downloading " + url, e);
                }
            } catch (MalformedURLException e) {
                throw new EsInstallationUnavailableException(e);
            }
        } else {
            log.info("Using {}", downloadFile);
        }

        return downloadFile;
    }

    public EsInstallation extract(File targetDir) throws EsInstallationUnavailableException {
        return extract(targetDir, 0);
    }

    public CompletableFuture<EsInstallation> extractAsync(File targetDir) {
        CompletableFuture<EsInstallation> future = new CompletableFuture<EsInstallation>();

        executorService.submit(() -> {
            try {
                future.complete(this.extract(targetDir));
            } catch (EsInstallationUnavailableException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }
    
    private EsInstallation extract(File targetDir, int retry) throws EsInstallationUnavailableException {
        File releaseArchive = this.getReleaseArchive();

        targetDir.mkdirs();

        try {
            log.info("Extracting " + releaseArchive + " to " + targetDir);

            Process process = Runtime.getRuntime().exec(
                    new String[] { "tar", "xfz", releaseArchive.getAbsolutePath(), "-C", targetDir.getAbsolutePath(), "--strip-components", "1" });

            StringBuilder result = new StringBuilder();

            executorService.submit(() -> {
                new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach((l) -> result.append(l).append("\n"));
            });
            executorService.submit(() -> {
                new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach((l) -> result.append(l).append("\n"));
            });
            int rc = process.waitFor();

            if (rc != 0) {
                log.error("Command failed with rc {}\n{}", rc, result);
                
                if (result.indexOf("Unexpected EOF in archive") != -1) {
                    if (retry <= 3) {
                        releaseArchive.delete();
                        return extract(targetDir, retry + 1);
                    } else {
                        throw new Exception("Invalid archive: Unexpected EOF");
                    }
                } else {
                    throw new Exception("Command failed with rc " + rc);                    
                }                
            }

            return new EsInstallation(targetDir, esVersion, executorService);
        } catch (Exception e) {
            throw new EsInstallationUnavailableException("Error while extracting " + releaseArchive, e);
        }
    }

    public static class EsInstallationUnavailableException extends Exception {

        private static final long serialVersionUID = 1L;

        public EsInstallationUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }

        public EsInstallationUnavailableException(String message) {
            super(message);
        }

        public EsInstallationUnavailableException(Throwable cause) {
            super(cause);
        }

    }
}
