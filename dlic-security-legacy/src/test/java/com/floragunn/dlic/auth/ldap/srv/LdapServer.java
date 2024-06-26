/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 *
 * Based on https://github.com/inbloom/ldap-in-memory 
 * 
 * 
 */


package com.floragunn.dlic.auth.ldap.srv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.io.CharStreams;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.schema.Schema;
import com.unboundid.ldif.LDIFReader;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustStoreTrustManager;

public abstract class LdapServer {
    public static LdapServer createTls(String... ldifFiles) {

        int tries = 0;
        while (tries < 10) {
            try {
                LdapServer server = new TlsLdapServer();
                server.start(ldifFiles);
                return server;
            } catch (Exception e) {
                tries++;
                System.out.println("Unable to start ldap server, try again ... " + e);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    return null;
                }
            }
        }
        System.out.println("Unable to start ldap server, return null");
        return null;
    }

    public static LdapServer createStartTls(String... ldifFiles) {

        int tries = 0;
        while (tries < 10) {
            try {
                LdapServer server = new StartTlsLdapServer();
                server.start(ldifFiles);
                return server;
            } catch (Exception e) {
                tries++;
                System.out.println("Unable to start ldap server, try again ... " + e);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    return null;
                }
            }
        }
        System.out.println("Unable to start ldap server, return null");
        return null;
    }

    public static LdapServer createPlainText(String... ldifFiles) {
        int tries = 0;
        while (tries < 10) {
            try {
                LdapServer server = new PlainTextLdapServer();
                server.start(ldifFiles);
                return server;
            } catch (Exception e) {
                tries++;
                System.out.println("Unable to start ldap server, try again ... " + e);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    return null;
                }
            }
        }
        System.out.println("Unable to start ldap server, return null");
        return null;
    }

    private final static Logger LOG = LoggerFactory.getLogger(LdapServer.class);

    private static final int LOCK_TIMEOUT = 60;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final String LOCK_TIMEOUT_MSG = "Unable to obtain lock due to timeout after " + LOCK_TIMEOUT + " " + TIME_UNIT.toString();
    private static final String SERVER_NOT_STARTED = "The LDAP server is not started.";
    private static final String SERVER_ALREADY_STARTED = "The LDAP server is already started.";


    private RestrictedInMemoryDirectoryServer server;
    private final AtomicBoolean isStarted = new AtomicBoolean(Boolean.FALSE);
    private final ReentrantLock serverStateLock = new ReentrantLock();
    
    private int port = -1;

    public LdapServer() {
    }

    public boolean isStarted() {
        return this.isStarted.get();
    }

    public int getPort() {
        return this.port;
    }    
    
    public String hostAndPort() {
        return "localhost:" + this.port;
    }
    
    public int  start(String... ldifFiles) throws Exception {
        boolean hasLock = false;
        try {
            hasLock = serverStateLock.tryLock(LdapServer.LOCK_TIMEOUT, LdapServer.TIME_UNIT);
            if (hasLock) {
                int retVal = doStart(ldifFiles);
                this.isStarted.set(Boolean.TRUE);
                return retVal;
            } else {
                throw new IllegalStateException(LdapServer.LOCK_TIMEOUT_MSG);
            }
        } catch (InterruptedException ioe) {
            //lock interrupted
            LOG.error(ioe.getMessage(), ioe);
        } finally {
            if (hasLock) {
                serverStateLock.unlock();
            }
        }
        
        return -1;
    }

    private int doStart(String... ldifFiles) throws Exception {
        if (isStarted.get()) {
            throw new IllegalStateException(LdapServer.SERVER_ALREADY_STARTED);
        }
        return configureAndStartServer(ldifFiles);
    }
    
    abstract InMemoryListenerConfig getInMemoryListenerConfig() throws LDAPException;
    
    private final String loadFile(final String file) throws IOException {
        String ldif;
        
        try (final Reader reader = new InputStreamReader(this.getClass().getResourceAsStream("/ldap/" + file),StandardCharsets.UTF_8)) {
            ldif = CharStreams.toString(reader);
        }
        

        ldif = ldif.replace("${hostname}", "localhost");
        ldif = ldif.replace("${port}", String.valueOf(port));
        return ldif;
        
    }

    private synchronized int configureAndStartServer(String... ldifFiles) throws Exception {
        InMemoryListenerConfig listenerConfig = getInMemoryListenerConfig();

        port = listenerConfig.getListenPort();
        
        Schema schema = Schema.getDefaultStandardSchema();

        final String rootObjectDN = "o=TEST";
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(new DN(rootObjectDN));

        config.setSchema(schema);  //schema can be set on the rootDN too, per javadoc.
        config.setListenerConfigs(Collections.singletonList(listenerConfig));
        config.setEnforceAttributeSyntaxCompliance(false);
        config.setEnforceSingleStructuralObjectClass(false);

        //config.setLDAPDebugLogHandler(DEBUG_HANDLER);
        //config.setAccessLogHandler(DEBUG_HANDLER);
        //config.addAdditionalBindCredentials(configuration.getBindDn(), configuration.getPassword());
        
        //config.setMaxConnections(1);

        server = new RestrictedInMemoryDirectoryServer(config);
        
        try {
            /* Clear entries from server. */
            server.clear();
            server.startListening();
            return loadLdifFiles(ldifFiles);
        } catch (LDAPException ldape) {
            if (ldape.getMessage().contains("java.net.BindException")) {
                throw new BindException(ldape.getMessage());
            }
            throw ldape;
        }

    }

    public void stop() {
        boolean hasLock = false;
        try {
            hasLock = serverStateLock.tryLock(LdapServer.LOCK_TIMEOUT, LdapServer.TIME_UNIT);
            if (hasLock) {
                if (!isStarted.get()) {
                    throw new IllegalStateException(LdapServer.SERVER_NOT_STARTED);
                }
                LOG.info("Shutting down in-Memory Ldap Server.");
                server.shutDown(true);
            } else {
                throw new IllegalStateException(LdapServer.LOCK_TIMEOUT_MSG);
            }
        } catch (InterruptedException ioe) {
            //lock interrupted
            LOG.debug(ExceptionUtils.getStackTrace(ioe));
        } finally {
            if (hasLock) {
                serverStateLock.unlock();
            }
        }
    }

    private int loadLdifFiles(String... ldifFiles) throws Exception {
        int ldifLoadCount = 0;
        for (String ldif : ldifFiles) {
            ldifLoadCount++;
            try (LDIFReader r = new LDIFReader(new BufferedReader(new StringReader(loadFile(ldif))))){
                Entry entry = null;
                while ((entry = r.readEntry()) != null) {
                    server.add(entry);
                    ldifLoadCount++;
                }
            } catch(Exception e) {
                LOG.error(e.toString(), e);
                throw e;
            }
        }
        return ldifLoadCount;
    }

    
    
    /*private static class DebugHandler extends Handler {
        private final static Logger LOG = LoggerFactory.getLogger(DebugHandler.class);

        @Override
        public void publish(LogRecord logRecord) {
           //LOG.debug(ToStringBuilder.reflectionToString(logRecord, ToStringStyle.MULTI_LINE_STYLE));
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() throws SecurityException {

        }
    }
    
    private static final DebugHandler DEBUG_HANDLER = new DebugHandler();*/
    
    
}

class TlsLdapServer extends LdapServer {

    @Override
    InMemoryListenerConfig getInMemoryListenerConfig() throws LDAPException {
        return InMemoryListenerConfig.createLDAPSConfig("ldaps", SocketUtils.findAvailableTcpPort(), createSSLServerSocketFactory());
    }

    private SSLServerSocketFactory createSSLServerSocketFactory() {
        try {
            String serverKeyStorePath = FileHelper.getAbsoluteFilePathFromClassPath("ldap/node-0-keystore.jks").toFile().getAbsolutePath();
            SSLUtil serverSSLUtil = new SSLUtil(new KeyStoreKeyManager(serverKeyStorePath, "changeit".toCharArray()),
                    new TrustStoreTrustManager(serverKeyStorePath));

            return serverSSLUtil.createSSLServerSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Error while creating SSLServerSocketFactory");
        }
    }
}

class StartTlsLdapServer extends LdapServer {

    @Override
    InMemoryListenerConfig getInMemoryListenerConfig() throws LDAPException {
        return InMemoryListenerConfig.createLDAPConfig("ldap", null, SocketUtils.findAvailableTcpPort(), createSSLSocketFactory());
    }

    private SSLSocketFactory createSSLSocketFactory() {
        try {
            String serverKeyStorePath = FileHelper.getAbsoluteFilePathFromClassPath("ldap/node-0-keystore.jks").toFile().getAbsolutePath();
            SSLUtil serverSSLUtil = new SSLUtil(new KeyStoreKeyManager(serverKeyStorePath, "changeit".toCharArray()),
                    new TrustStoreTrustManager(serverKeyStorePath));

            return serverSSLUtil.createSSLSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Error while creating SSLSocketFactory");
        }
    }
}

class PlainTextLdapServer extends LdapServer {

    @Override
    InMemoryListenerConfig getInMemoryListenerConfig() throws LDAPException {
        return InMemoryListenerConfig.createLDAPConfig("ldap", null, SocketUtils.findAvailableTcpPort(), null);
    }
}
