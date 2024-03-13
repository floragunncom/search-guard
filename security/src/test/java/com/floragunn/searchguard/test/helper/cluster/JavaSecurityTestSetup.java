/*
 * Copyright 2021 floragunn GmbH
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

import java.io.FilePermission;
import java.net.SocketPermission;
import java.security.AccessControlException;
import java.security.Permission;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.secure_sm.SecureSM;
import org.junit.rules.ExternalResource;

/**
 * Provides a simplified Java Security Manager environment for JUnit tests.
 * 
 * This is far from perfect, but good enough to notice most missing doPriviledged() blocks, etc.
 * 
 * In order to run tests without this, set -Dsg.test-java-security.enabled=false
 * 
 * Helpful VM args for privilege debugging:
 * 
 * - -Djava.security.debug=access,failure,domain
 * - -Djava.security.debug=access,domain,permission=java.net.RuntimePermission
 * 
 * Note: When using java.security.debug=access,failure, don't be confused by file access control failures caused by Lucene. 
 * During startup, Lucene seems to do a very liberal scan of your whole root dir and just ignores dirs it cannot access.
 * 
 * Note: This does not work with multi threaded unit tests. If you want to have concurrency, use forking instead.
 * 
 * TODO:
 * 
 * - This needs a copy of plugin-security.policy in test/resources. We should find a way to avoid this redunancy.
 * - Lots of more polishing. In parts, this is quite messy.
 */
public class JavaSecurityTestSetup extends ExternalResource {
    private static final Logger log = LogManager.getLogger(JavaSecurityTestSetup.class);
    private static ReentrantLock lock = new ReentrantLock();
    private static Policy baseSystemPolicy = Policy.getPolicy();
    private static Policy baseEsPolicy = EsJavaSecurity.getBaseEsSecurityPolicy();
    private static Policy sgPluginPolicy = EsJavaSecurity.getSgPluginSecurityPolicy();
    private static Permissions classPathPermissions = EsJavaSecurity.getClasspathPermissions();
    private static Permissions miscPermissions = EsJavaSecurity.getMiscPermissions();
    private boolean enabled = System.getProperty("sg.test-java-security.enabled", "true").equals("true");

    static {
        try {
            JvmInfo.jvmInfo();
        } catch (AccessControlException e) {
            // If we get this, we are already properly initialized
        }
        try {
            BootstrapInfo.init();
        } catch (AccessControlException e) {
            // If we get this, we are already properly initialized
        }
        try {
            BootstrapInfo.isNativesAvailable();
        } catch (AccessControlException e) {
            // If we get this, we are already properly initialized
        }
    }

    public JavaSecurityTestSetup() {
        if (enabled) {
            if (lock.isLocked()) {
                try {
                    if (!lock.tryLock(10, TimeUnit.SECONDS)) {
                        log.warn("***** Multithreaded use of TestJavaSecurityManagement is not possible. Waiting for current owner to finish: "
                                + lock);
                        lock.lock();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            Policy.setPolicy(new TestPolicy());
            System.setSecurityManager(SecureSM.createTestSecureSM());
            log.info("JavaSecurityTestSetup has been installed");
        }
    }

    @Override
    protected void after() {
        try {
            if (enabled) {
                System.setSecurityManager(null);
                Policy.setPolicy(baseSystemPolicy);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    static class TestPolicy extends Policy {

        private static final Pattern JAR_PATTERN = Pattern.compile(".*/(.*?)(-[0-9]+\\.[0-9]+(\\.[0-9]+)?(\\.[^\\.]+)?)?\\.jar$");

        @Override
        public boolean implies(ProtectionDomain protectionDomain, Permission permission) {
            if (protectionDomain == null) {
                return true;
            }
            
            if (permission instanceof FilePermission) {
                FilePermission filePermission = ((FilePermission) permission);

                if (filePermission.getName().contains("data")) {
                    // Special case for ES data access
                    return true;
                }

                if (filePermission.getName().contains("/target/test-classes/") || filePermission.getName().contains("/config")
                        || filePermission.getName().contains("/sgconfig")) {
                    // Special case for reading cobfig files from test data; we might want to clean this up a bit
                    return true;
                }

                if (filePermission.getName().contains("/search-guard-suite")) {
                    // In some cases the local cluster seems to start using just the project dir as cwd. Allow this for now, but we should fix the local cluster to use a temp dir 
                    return true;
                }

                if (filePermission.getName().contains("/modules") || filePermission.getName().contains("/plugins")) {
                    return true;
                }
            }

            if (baseEsPolicy.implies(protectionDomain, permission)) {
                return true;
            }

            if (classPathPermissions.implies(permission)) {
                return true;
            }

            if (miscPermissions.implies(permission)) {
                return true;
            }

            if (permission instanceof SocketPermission) {
                // TODO make finer
                return true;
            }

            if (baseSystemPolicy != null && baseSystemPolicy.implies(protectionDomain, permission)) {
                return true;
            }

            String protectionDomainKey = getProtectionDomainKey(protectionDomain);

            if (permission instanceof SocketPermission && log.isTraceEnabled()) {
                log.trace(permission + " " + protectionDomainKey + " " + protectionDomain.getCodeSource().getLocation() + " "
                        + protectionDomain.getClassLoader());
            }

            if ("search-guard-plugin".equals(protectionDomainKey)) {
                if (sgPluginPolicy.implies(protectionDomain, permission)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                // We trust trust all other libraries. The most important point in this code is that the Search Guard code is properly trust checked.
                // As the final result depends on the intersection of all stack frames, this should be sufficient for most cases.
                return true;
            }
        }

        private String getProtectionDomainKey(ProtectionDomain protectionDomain) {
            if (protectionDomain.getCodeSource() == null || protectionDomain.getCodeSource().getLocation() == null) {
                return "test-classes";
            }
            
            String uri = protectionDomain.getCodeSource().getLocation().toExternalForm();

            if (uri.contains("/org/elasticsearch/") && uri.endsWith(".jar")) {
                return "es";
            }

            if (uri.contains("/org/apache/lucene/") && uri.endsWith(".jar")) {
                return "lucene";
            }

            if (uri.contains("/io/netty/") && uri.endsWith(".jar")) {
                return "netty";
            }

            Matcher jarPatternMatcher = JAR_PATTERN.matcher(uri);

            if (jarPatternMatcher.matches()) {
                return jarPatternMatcher.group(1) + ".jar";
            }

            if (uri.endsWith("/target/test-classes/")) {
                return "test-classes";
            }

            if (uri.endsWith("/target/classes/")) {
                return "search-guard-plugin";
            }

            if (uri.contains("eclipse/configuration")) {
                return "test-runner";
            }

            return uri;
        }
    }

}
