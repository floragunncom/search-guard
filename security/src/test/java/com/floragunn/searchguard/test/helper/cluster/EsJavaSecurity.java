/*
 * Includes code from the following Apache 2 licensed files from Elasticsearch 7.10.2:
 * 
 * - /server/src/main/java/org/elasticsearch/bootstrap/Security.java: 
 *    https://github.com/elastic/elasticsearch/blob/7.10/server/src/main/java/org/elasticsearch/bootstrap/Security.java
 * - /server/src/main/java/org/elasticsearch/bootstrap/ESPolicy.java:
 *    https://github.com/elastic/elasticsearch/blob/7.10/server/src/main/java/org/elasticsearch/bootstrap/ESPolicy.java
 * - /test/framework/src/main/java/org/elasticsearch/bootstrap/BootstrapForTesting.java:
 *    https://github.com/elastic/elasticsearch/blob/7.10/test/framework/src/main/java/org/elasticsearch/bootstrap/BootstrapForTesting.java
 * 
 * Original license notice for all of the noted files:
 *
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * 
 */

package com.floragunn.searchguard.test.helper.cluster;

import static org.opensearch.bootstrap.FilePermissionUtils.addDirectoryPath;
import static org.opensearch.bootstrap.FilePermissionUtils.addSingleFilePath;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.Permissions;
import java.security.Policy;
import java.security.URIParameter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opensearch.bootstrap.BootstrapInfo;
import org.opensearch.bootstrap.FilePermissionUtils;
import org.opensearch.bootstrap.JarHell;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;

public class EsJavaSecurity {
    static Policy getBaseEsSecurityPolicy() {
        return readPolicy(BootstrapInfo.class.getResource("security.policy"), getCodebases());
    }

    static Policy getSgPluginSecurityPolicy() {
        return readPolicy(EsJavaSecurity.class.getResource("/sg-plugin-security.policy"), getCodebases());
    }

    /**
     * Return a map from codebase name to codebase url of jar codebases used by ES core.
     */
    @SuppressForbidden(reason = "find URL path")
    static Map<String, URL> getCodebaseJarMap(Set<URL> urls) {
        Map<String, URL> codebases = new LinkedHashMap<>(); // maintain order
        for (URL url : urls) {
            try {
                String fileName = PathUtils.get(url.toURI()).getFileName().toString();
                if (fileName.endsWith(".jar") == false) {
                    // tests :(
                    continue;
                }
                codebases.put(fileName, url);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return codebases;
    }

    /**
     * Reads and returns the specified {@code policyFile}.
     * <p>
     * Jar files listed in {@code codebases} location will be provided to the policy file via
     * a system property of the short name: e.g. <code>${codebase.joda-convert-1.2.jar}</code>
     * would map to full URL.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Policy readPolicy(URL policyFile, Map<String, URL> codebases) {
        try {
            List<String> propertiesSet = new ArrayList<>();
            try {
                // set codebase properties
                for (Map.Entry<String, URL> codebase : codebases.entrySet()) {
                    String name = codebase.getKey();
                    URL url = codebase.getValue();

                    // We attempt to use a versionless identifier for each codebase. This assumes a specific version
                    // format in the jar filename. While we cannot ensure all jars in all plugins use this format, nonconformity
                    // only means policy grants would need to include the entire jar filename as they always have before.
                    String property = "codebase." + name;
                    String aliasProperty = "codebase." + name.replaceFirst("-\\d+\\.\\d+.*\\.jar", "");
                    if (aliasProperty.equals(property) == false) {
                        propertiesSet.add(aliasProperty);
                        System.setProperty(aliasProperty, url.toString());                        
                    }
                    propertiesSet.add(property);
                    System.setProperty(property, url.toString());
                }
                return Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));
            } finally {
                // clear codebase properties
                for (String property : propertiesSet) {
                    System.clearProperty(property);
                }
            }
        } catch (NoSuchAlgorithmException | URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }

    /** Adds access to classpath jars/classes for jar hell scan, etc */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Permissions getClasspathPermissions() {
        try {
            Permissions perms = new Permissions();

            // add permissions to everything in classpath
            // really it should be covered by lib/, but there could be e.g. agents or similar configured)
            for (URL url : JarHell.parseClassPath()) {
                Path path;
                try {
                    path = PathUtils.get(url.toURI());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
                // resource itself
                if (Files.isDirectory(path)) {
                    addDirectoryPath(perms, "class.path", path, "read,readlink", false);
                } else {
                    addSingleFilePath(perms, path, "read,readlink");
                }
            }

            return perms;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Permissions getMiscPermissions() {

        try {
            Permissions perms = new Permissions();
            Path javaTmpDir = PathUtils.get(Objects.requireNonNull(System.getProperty("java.io.tmpdir"), "please set ${java.io.tmpdir} in pom.xml"));
            FilePermissionUtils.addDirectoryPath(perms, "java.io.tmpdir", javaTmpDir, "read,readlink,write,delete", false);
            perms.add(new RuntimePermission("getStackWalkerWithClassReference"));
            return perms;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static Map<String, URL> getCodebases() {
        // read test-framework permissions
        Map<String, URL> codebases = getCodebaseJarMap(JarHell.parseClassPath());
        // when testing server, the main elasticsearch code is not yet in a jar, so we need to manually add it
        addClassCodebase(codebases, "elasticsearch", "org.opensearch.plugins.PluginsService");
        if (System.getProperty("tests.gradle") == null) {
            // intellij and eclipse don't package our internal libs, so we need to set the codebases for them manually
            addClassCodebase(codebases, "plugin-classloader", "org.opensearch.plugins.ExtendedPluginsClassLoader");
            addClassCodebase(codebases, "elasticsearch-nio", "org.opensearch.nio.ChannelFactory");
            addClassCodebase(codebases, "elasticsearch-secure-sm", "org.opensearch.secure_sm.SecureSM");
            addClassCodebase(codebases, "elasticsearch-rest-client", "org.opensearch.client.RestClient");
        }

        return codebases;
    }

    /** Add the codebase url of the given classname to the codebases map, if the class exists. */
    private static void addClassCodebase(Map<String, URL> codebases, String name, String classname) {
        try {
            Class<?> clazz = EsJavaSecurity.class.getClassLoader().loadClass(classname);
            URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
            if (location.toString().endsWith(".jar") == false) {
                if (codebases.put(name, location) != null) {
                    throw new IllegalStateException("Already added " + name + " codebase for testing");
                }
            }
        } catch (ClassNotFoundException e) {
            // no class, fall through to not add. this can happen for any tests that do not include
            // the given class. eg only core tests include plugin-classloader
        }
    }

}
