/*
 * Based on Apache 2 licensed https://github.com/pingidentity/ldapsdk/blob/master/src/com/unboundid/ldap/listener/InMemoryDirectoryServer.java
 * 
 * Modifications Copyright 2020-2021 floragunn GmbH
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
/*
 * Copyright 2011-2020 Ping Identity Corporation
 * All Rights Reserved.
 */
/*
 * Copyright 2011-2020 Ping Identity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright (C) 2011-2020 Ping Identity Corporation
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License (GPLv2 only)
 * or the terms of the GNU Lesser General Public License (LGPLv2.1 only)
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */
package com.floragunn.searchguard.enterprise.auth.ldap;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;

import com.unboundid.asn1.ASN1OctetString;
import com.unboundid.ldap.listener.AccessLogRequestHandler;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryDirectoryServerPassword;
import com.unboundid.ldap.listener.InMemoryDirectoryServerSnapshot;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.listener.InMemoryPasswordEncoder;
import com.unboundid.ldap.listener.InMemoryRequestHandler;
import com.unboundid.ldap.listener.JSONAccessLogRequestHandler;
import com.unboundid.ldap.listener.LDAPDebuggerRequestHandler;
import com.unboundid.ldap.listener.LDAPListener;
import com.unboundid.ldap.listener.LDAPListenerClientConnection;
import com.unboundid.ldap.listener.LDAPListenerConfig;
import com.unboundid.ldap.listener.LDAPListenerRequestHandler;
import com.unboundid.ldap.listener.ReadOnlyInMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.StartTLSRequestHandler;
import com.unboundid.ldap.listener.ToCodeRequestHandler;
import com.unboundid.ldap.listener.interceptor.InMemoryOperationInterceptorRequestHandler;
import com.unboundid.ldap.protocol.AbandonRequestProtocolOp;
import com.unboundid.ldap.protocol.AddRequestProtocolOp;
import com.unboundid.ldap.protocol.AddResponseProtocolOp;
import com.unboundid.ldap.protocol.BindRequestProtocolOp;
import com.unboundid.ldap.protocol.BindResponseProtocolOp;
import com.unboundid.ldap.protocol.CompareRequestProtocolOp;
import com.unboundid.ldap.protocol.CompareResponseProtocolOp;
import com.unboundid.ldap.protocol.DeleteRequestProtocolOp;
import com.unboundid.ldap.protocol.DeleteResponseProtocolOp;
import com.unboundid.ldap.protocol.ExtendedRequestProtocolOp;
import com.unboundid.ldap.protocol.ExtendedResponseProtocolOp;
import com.unboundid.ldap.protocol.LDAPMessage;
import com.unboundid.ldap.protocol.ModifyDNRequestProtocolOp;
import com.unboundid.ldap.protocol.ModifyDNResponseProtocolOp;
import com.unboundid.ldap.protocol.ModifyRequestProtocolOp;
import com.unboundid.ldap.protocol.ModifyResponseProtocolOp;
import com.unboundid.ldap.protocol.SearchRequestProtocolOp;
import com.unboundid.ldap.protocol.SearchResultDoneProtocolOp;
import com.unboundid.ldap.protocol.UnbindRequestProtocolOp;
import com.unboundid.ldap.sdk.AddRequest;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.BindResult;
import com.unboundid.ldap.sdk.Control;
import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.ExtendedRequest;
import com.unboundid.ldap.sdk.ExtendedResult;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPResult;
import com.unboundid.ldap.sdk.PLAINBindRequest;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.extensions.StartTLSExtendedRequest;
import com.unboundid.ldap.sdk.schema.Schema;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;
import com.unboundid.ldif.LDIFWriter;
import com.unboundid.util.ByteStringBuffer;
import com.unboundid.util.Debug;
import com.unboundid.util.Mutable;
import com.unboundid.util.StaticUtils;
import com.unboundid.util.ThreadSafety;
import com.unboundid.util.ThreadSafetyLevel;
import com.unboundid.util.Validator;

/**
 * A InMemoryDirectoryServer with modifications for testing.
 * 
 * Differences:
 * 
 * - If a StartTLS capable listener is configured, this won't accept request issued before a Start TLS has been performed.
 * - It can optionally delay the execution of bind requests. This is useful for stress-testing the connection pool.
 * 
 * Additionally, this variation no longer implements FullLDAPInterface as this is not necessary for our testing purposes.
 */
@Mutable()
@ThreadSafety(level = ThreadSafetyLevel.COMPLETELY_THREADSAFE)
public final class TestInMemoryDirectoryServer {
    // The in-memory request handler that will be used for the server.
    private final InMemoryRequestHandler inMemoryHandler;

    // The set of listeners that have been configured for this server, mapped by
    // listener name.
    private final Map<String, LDAPListener> listeners;

    // The set of configurations for all the LDAP listeners to be used.
    private final Map<String, LDAPListenerConfig> ldapListenerConfigs;

    // The set of client socket factories associated with each of the listeners.
    private final Map<String, SocketFactory> clientSocketFactories;
    
    // A read-only representation of the configuration used to create this
    // in-memory directory server.
    private final ReadOnlyInMemoryDirectoryServerConfig config;

    /**
     * Creates a new instance of an in-memory directory server with the provided
     * configuration.
     *
     * @param  cfg  The configuration to use for the server.  It must not be
     *              {@code null}.
     *
     * @throws  LDAPException  If a problem occurs while trying to initialize the
     *                         directory server with the provided configuration.
     */
    public TestInMemoryDirectoryServer(InMemoryDirectoryServerConfig cfg, Duration bindRequestDelay) throws LDAPException {
        Validator.ensureNotNull(cfg);

        config = new ReadOnlyInMemoryDirectoryServerConfig(cfg);
        inMemoryHandler = new InMemoryRequestHandler(config);

        LDAPListenerRequestHandler requestHandler = inMemoryHandler;

        if (config.getAccessLogHandler() != null) {
            requestHandler = new AccessLogRequestHandler(config.getAccessLogHandler(), requestHandler);
        }

        if (config.getJSONAccessLogHandler() != null) {
            requestHandler = new JSONAccessLogRequestHandler(config.getJSONAccessLogHandler(), requestHandler);
        }

        if (config.getLDAPDebugLogHandler() != null) {
            requestHandler = new LDAPDebuggerRequestHandler(config.getLDAPDebugLogHandler(), requestHandler);
        }

        if (config.getCodeLogPath() != null) {
            try {
                requestHandler = new ToCodeRequestHandler(config.getCodeLogPath(), config.includeRequestProcessingInCodeLog(), requestHandler);
            } catch (final IOException ioe) {
                Debug.debugException(ioe);
                throw new LDAPException(ResultCode.LOCAL_ERROR, ioe);
            }
        }

        if (!config.getOperationInterceptors().isEmpty()) {
            requestHandler = new InMemoryOperationInterceptorRequestHandler(config.getOperationInterceptors(), requestHandler);
        }

        final List<InMemoryListenerConfig> listenerConfigs = config.getListenerConfigs();

        listeners = new LinkedHashMap<>(StaticUtils.computeMapCapacity(listenerConfigs.size()));
        ldapListenerConfigs = new LinkedHashMap<>(StaticUtils.computeMapCapacity(listenerConfigs.size()));
        clientSocketFactories = new LinkedHashMap<>(StaticUtils.computeMapCapacity(listenerConfigs.size()));

        for (final InMemoryListenerConfig c : listenerConfigs) {
            final String name = StaticUtils.toLowerCase(c.getListenerName());

            LDAPListenerRequestHandler listenerRequestHandler;
            
            if (c.getStartTLSSocketFactory() == null) {
                listenerRequestHandler = requestHandler;
            } else {
                listenerRequestHandler = new PlaintextRequestRejectingRequestHandler(
                        new StartTLSRequestHandler(c.getStartTLSSocketFactory(), requestHandler), null);
            }
            
            if (bindRequestDelay != null) {
                listenerRequestHandler = new DelayedBindRequestHandler(listenerRequestHandler, bindRequestDelay);
            }

            final LDAPListenerConfig listenerCfg = new LDAPListenerConfig(c.getListenPort(), listenerRequestHandler);
            listenerCfg.setMaxConnections(config.getMaxConnections());
            listenerCfg.setExceptionHandler(config.getListenerExceptionHandler());
            listenerCfg.setListenAddress(c.getListenAddress());
            listenerCfg.setServerSocketFactory(c.getServerSocketFactory());

            ldapListenerConfigs.put(name, listenerCfg);

            if (c.getClientSocketFactory() != null) {
                clientSocketFactories.put(name, c.getClientSocketFactory());
            }
        }
    }

    /**
     * Attempts to start listening for client connections on all configured
     * listeners.  Any listeners that are already running will be unaffected.
     *
     * @throws  LDAPException  If a problem occurs while attempting to create any
     *                         of the configured listeners.  Even if an exception
     *                         is thrown, then as many listeners as possible will
     *                         be started.
     */
    public synchronized void startListening() throws LDAPException {
        final ArrayList<String> messages = new ArrayList<>(listeners.size());

        for (final Map.Entry<String, LDAPListenerConfig> cfgEntry : ldapListenerConfigs.entrySet()) {
            final String name = cfgEntry.getKey();

            if (listeners.containsKey(name)) {
                // This listener is already running.
                continue;
            }

            final LDAPListenerConfig listenerConfig = cfgEntry.getValue();
            final LDAPListener listener = new LDAPListener(listenerConfig);

            try {
                listener.startListening();
                listenerConfig.setListenPort(listener.getListenPort());
                listeners.put(name, listener);
            } catch (final Exception e) {
                Debug.debugException(e);
                messages.add(StaticUtils.getExceptionMessage(e));
            }
        }

        if (!messages.isEmpty()) {
            throw new LDAPException(ResultCode.LOCAL_ERROR, StaticUtils.concatenateStrings(messages));
        }
    }

    /**
     * Attempts to start listening for client connections on the specified
     * listener.  If the listener is already running, then it will be unaffected.
     *
     * @param  listenerName  The name of the listener to be started.  It must not
     *                       be {@code null}.
     *
     * @throws  LDAPException  If a problem occurs while attempting to start the
     *                         requested listener.
     */
    public synchronized void startListening(final String listenerName) throws LDAPException {
        // If the listener is already running, then there's nothing to do.
        final String name = StaticUtils.toLowerCase(listenerName);
        if (listeners.containsKey(name)) {
            return;
        }

        // Get the configuration to use for the listener.
        final LDAPListenerConfig listenerConfig = ldapListenerConfigs.get(name);
        if (listenerConfig == null) {
            throw new LDAPException(ResultCode.PARAM_ERROR, "No such listener: " + listenerName);
        }

        final LDAPListener listener = new LDAPListener(listenerConfig);

        try {
            listener.startListening();
            listenerConfig.setListenPort(listener.getListenPort());
            listeners.put(name, listener);
        } catch (final Exception e) {
            Debug.debugException(e);
            throw new LDAPException(ResultCode.LOCAL_ERROR, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        shutDown(true);
    }

    /**
     * Closes all connections that are currently established to the server.  This
     * has no effect on the ability to accept new connections.
     *
     * @param  sendNoticeOfDisconnection  Indicates whether to send the client a
     *                                    notice of disconnection unsolicited
     *                                    notification before closing the
     *                                    connection.
     */
    public synchronized void closeAllConnections(final boolean sendNoticeOfDisconnection) {
        for (final LDAPListener l : listeners.values()) {
            try {
                l.closeAllConnections(sendNoticeOfDisconnection);
            } catch (final Exception e) {
                Debug.debugException(e);
            }
        }
    }

    /**
     * Shuts down all configured listeners.  Any listeners that are already
     * stopped will be unaffected.
     *
     * @param  closeExistingConnections  Indicates whether to close all existing
     *                                   connections, or merely to stop accepting
     *                                   new connections.
     */
    public synchronized void shutDown(final boolean closeExistingConnections) {
        for (final LDAPListener l : listeners.values()) {
            try {
                l.shutDown(closeExistingConnections);
            } catch (final Exception e) {
                Debug.debugException(e);
            }
        }

        listeners.clear();
    }

    /**
     * Shuts down the specified listener.  If there is no such listener defined,
     * or if the specified listener is not running, then no action will be taken.
     *
     * @param  listenerName              The name of the listener to be shut down.
     *                                   It must not be {@code null}.
     * @param  closeExistingConnections  Indicates whether to close all existing
     *                                   connections, or merely to stop accepting
     *                                   new connections.
     */
    public synchronized void shutDown(final String listenerName, final boolean closeExistingConnections) {
        final String name = StaticUtils.toLowerCase(listenerName);
        final LDAPListener listener = listeners.remove(name);
        if (listener != null) {
            listener.shutDown(closeExistingConnections);
        }
    }

    /**
     * Attempts to restart all listeners defined in the server.  All running
     * listeners will be stopped, and all configured listeners will be started.
     *
     * @throws  LDAPException  If a problem occurs while attempting to restart any
     *                         of the listeners.  Even if an exception is thrown,
     *                         as many listeners as possible will be started.
     */
    public synchronized void restartServer() throws LDAPException {
        shutDown(true);

        try {
            Thread.sleep(100L);
        } catch (final Exception e) {
            Debug.debugException(e);

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        startListening();
    }

    /**
     * Attempts to restart the specified listener.  If it is running, it will be
     * stopped.  It will then be started.
     *
     * @param  listenerName  The name of the listener to be restarted.  It must
     *                       not be {@code null}.
     *
     * @throws  LDAPException  If a problem occurs while attempting to restart the
     *                         specified listener.
     */
    public synchronized void restartListener(final String listenerName) throws LDAPException {
        shutDown(listenerName, true);

        try {
            Thread.sleep(100L);
        } catch (final Exception e) {
            Debug.debugException(e);

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        startListening(listenerName);
    }

    /**
     * Retrieves a read-only representation of the configuration used to create
     * this in-memory directory server instance.
     *
     * @return  A read-only representation of the configuration used to create
     *          this in-memory directory server instance.
     */
    public ReadOnlyInMemoryDirectoryServerConfig getConfig() {
        return config;
    }

    /**
     * Retrieves the in-memory request handler that is used to perform the real
     * server processing.
     *
     * @return  The in-memory request handler that is used to perform the real
     *          server processing.
     */
    InMemoryRequestHandler getInMemoryRequestHandler() {
        return inMemoryHandler;
    }

    /**
     * Creates a point-in-time snapshot of the information contained in this
     * in-memory directory server instance.  It may be restored using the
     * {@link #restoreSnapshot} method.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @return  The snapshot created based on the current content of this
     *          in-memory directory server instance.
     */
    public InMemoryDirectoryServerSnapshot createSnapshot() {
        return inMemoryHandler.createSnapshot();
    }

    /**
     * Restores the this in-memory directory server instance to match the content
     * it held at the time the snapshot was created.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  snapshot  The snapshot to be restored.  It must not be
     *                   {@code null}.
     */
    public void restoreSnapshot(final InMemoryDirectoryServerSnapshot snapshot) {
        inMemoryHandler.restoreSnapshot(snapshot);
    }

    /**
     * Retrieves the list of base DNs configured for use by the server.
     *
     * @return  The list of base DNs configured for use by the server.
     */
    public List<DN> getBaseDNs() {
        return inMemoryHandler.getBaseDNs();
    }

    /**
     * Attempts to establish a client connection to the server.  If multiple
     * listeners are configured, then it will attempt to establish a connection to
     * the first configured listener that is running.
     *
     * @return  The client connection that has been established.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to
     *                         create the connection.
     */
    public LDAPConnection getConnection() throws LDAPException {
        return getConnection(null, null);
    }

    /**
     * Attempts to establish a client connection to the server.
     *
     * @param  options  The connection options to use when creating the
     *                  connection.  It may be {@code null} if a default set of
     *                  options should be used.
     *
     * @return  The client connection that has been established.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to
     *                         create the connection.
     */
    public LDAPConnection getConnection(final LDAPConnectionOptions options) throws LDAPException {
        return getConnection(null, options);
    }

    /**
     * Attempts to establish a client connection to the specified listener.
     *
     * @param  listenerName  The name of the listener to which to establish the
     *                       connection.  It may be {@code null} if a connection
     *                       should be established to the first available
     *                       listener.
     *
     * @return  The client connection that has been established.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to
     *                         create the connection.
     */
    public LDAPConnection getConnection(final String listenerName) throws LDAPException {
        return getConnection(listenerName, null);
    }

    /**
     * Attempts to establish a client connection to the specified listener.
     *
     * @param  listenerName  The name of the listener to which to establish the
     *                       connection.  It may be {@code null} if a connection
     *                       should be established to the first available
     *                       listener.
     * @param  options       The set of LDAP connection options to use for the
     *                       connection that is created.
     *
     * @return  The client connection that has been established.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to
     *                         create the connection.
     */
    public synchronized LDAPConnection getConnection(final String listenerName, final LDAPConnectionOptions options) throws LDAPException {
        final LDAPListenerConfig listenerConfig;
        final SocketFactory clientSocketFactory;

        if (listenerName == null) {
            final String name = getFirstListenerName();
            if (name == null) {
                throw new LDAPException(ResultCode.CONNECT_ERROR, "no listeners");
            }

            listenerConfig = ldapListenerConfigs.get(name);
            clientSocketFactory = clientSocketFactories.get(name);
        } else {
            final String name = StaticUtils.toLowerCase(listenerName);
            if (!listeners.containsKey(name)) {
                throw new LDAPException(ResultCode.CONNECT_ERROR, "listener not running: " + listenerName);
            }

            listenerConfig = ldapListenerConfigs.get(name);
            clientSocketFactory = clientSocketFactories.get(name);
        }

        String hostAddress;
        final InetAddress listenAddress = listenerConfig.getListenAddress();
        if ((listenAddress == null) || (listenAddress.isAnyLocalAddress())) {
            try {
                hostAddress = LDAPConnectionOptions.DEFAULT_NAME_RESOLVER.getLocalHost().getHostAddress();
            } catch (final Exception e) {
                Debug.debugException(e);
                hostAddress = "127.0.0.1";
            }
        } else {
            hostAddress = listenAddress.getHostAddress();
        }

        return new LDAPConnection(clientSocketFactory, options, hostAddress, listenerConfig.getListenPort());
    }

    /**
     * Attempts to establish a connection pool to the server with the specified
     * maximum number of connections.
     *
     * @param  maxConnections  The maximum number of connections to maintain in
     *                         the connection pool.  It must be greater than or
     *                         equal to one.
     *
     * @return  The connection pool that has been created.
     *
     * @throws  LDAPException  If a problem occurs while attempting to create the
     *                         connection pool.
     */
    public LDAPConnectionPool getConnectionPool(final int maxConnections) throws LDAPException {
        return getConnectionPool(null, null, 1, maxConnections);
    }

    /**
     * Attempts to establish a connection pool to the server with the provided
     * settings.
     *
     * @param  listenerName        The name of the listener to which the
     *                             connections should be established.
     * @param  options             The connection options to use when creating
     *                             connections for use in the pool.  It may be
     *                             {@code null} if a default set of options should
     *                             be used.
     * @param  initialConnections  The initial number of connections to establish
     *                             in the connection pool.  It must be greater
     *                             than or equal to one.
     * @param  maxConnections      The maximum number of connections to maintain
     *                             in the connection pool.  It must be greater
     *                             than or equal to the initial number of
     *                             connections.
     *
     * @return  The connection pool that has been created.
     *
     * @throws  LDAPException  If a problem occurs while attempting to create the
     *                         connection pool.
     */
    public LDAPConnectionPool getConnectionPool(final String listenerName, final LDAPConnectionOptions options, final int initialConnections,
            final int maxConnections) throws LDAPException {
        final LDAPConnection conn = getConnection(listenerName, options);
        return new LDAPConnectionPool(conn, initialConnections, maxConnections);
    }

    /**
     * Retrieves the configured listen address for the first active listener, if
     * defined.
     *
     * @return  The configured listen address for the first active listener, or
     *          {@code null} if that listener does not have an
     *          explicitly-configured listen address or there are no active
     *          listeners.
     */
    public InetAddress getListenAddress() {
        return getListenAddress(null);
    }

    /**
     * Retrieves the configured listen address for the specified listener, if
     * defined.
     *
     * @param  listenerName  The name of the listener for which to retrieve the
     *                       listen address.  It may be {@code null} in order to
     *                       obtain the listen address for the first active
     *                       listener.
     *
     * @return  The configured listen address for the specified listener, or
     *          {@code null} if there is no such listener or the listener does not
     *          have an explicitly-configured listen address.
     */
    public synchronized InetAddress getListenAddress(final String listenerName) {
        final String name;
        if (listenerName == null) {
            name = getFirstListenerName();
        } else {
            name = StaticUtils.toLowerCase(listenerName);
        }

        final LDAPListenerConfig listenerCfg = ldapListenerConfigs.get(name);
        if (listenerCfg == null) {
            return null;
        } else {
            return listenerCfg.getListenAddress();
        }
    }

    /**
     * Retrieves the configured listen port for the first active listener.
     *
     * @return  The configured listen port for the first active listener, or -1 if
     *          there are no active listeners.
     */
    public int getListenPort() {
        return getListenPort(null);
    }

    /**
     * Retrieves the configured listen port for the specified listener, if
     * available.
     *
     * @param  listenerName  The name of the listener for which to retrieve the
     *                       listen port.  It may be {@code null} in order to
     *                       obtain the listen port for the first active
     *                       listener.
     *
     * @return  The configured listen port for the specified listener, or -1 if
     *          there is no such listener or the listener is not active.
     */
    public synchronized int getListenPort(final String listenerName) {
        final String name;
        if (listenerName == null) {
            name = getFirstListenerName();
        } else {
            name = StaticUtils.toLowerCase(listenerName);
        }

        final LDAPListener listener = listeners.get(name);
        if (listener == null) {
            return -1;
        } else {
            return listener.getListenPort();
        }
    }

    /**
     * Retrieves the configured client socket factory for the first active
     * listener.
     *
     * @return  The configured client socket factory for the first active
     *          listener, or {@code null} if that listener does not have an
     *          explicitly-configured socket factory or there are no active
     *          listeners.
     */
    public SocketFactory getClientSocketFactory() {
        return getClientSocketFactory(null);
    }

    /**
     * Retrieves the configured client socket factory for the specified listener,
     * if available.
     *
     * @param  listenerName  The name of the listener for which to retrieve the
     *                       client socket factory.  It may be {@code null} in
     *                       order to obtain the client socket factory for the
     *                       first active listener.
     *
     * @return  The configured client socket factory for the specified listener,
     *          or {@code null} if there is no such listener or that listener does
     *          not have an explicitly-configured client socket factory.
     */
    public synchronized SocketFactory getClientSocketFactory(final String listenerName) {
        final String name;
        if (listenerName == null) {
            name = getFirstListenerName();
        } else {
            name = StaticUtils.toLowerCase(listenerName);
        }

        return clientSocketFactories.get(name);
    }

    /**
     * Retrieves the name of the first running listener.
     *
     * @return  The name of the first running listener, or {@code null} if there
     *          are no active listeners.
     */
    private String getFirstListenerName() {
        for (final Map.Entry<String, LDAPListenerConfig> e : ldapListenerConfigs.entrySet()) {
            final String name = e.getKey();
            if (listeners.containsKey(name)) {
                return name;
            }
        }

        return null;
    }

    /**
     * Retrieves the delay in milliseconds that the server should impose before
     * beginning processing for operations.
     *
     * @return  The delay in milliseconds that the server should impose before
     *          beginning processing for operations, or 0 if there should be no
     *          delay inserted when processing operations.
     */
    public long getProcessingDelayMillis() {
        return inMemoryHandler.getProcessingDelayMillis();
    }

    /**
     * Specifies the delay in milliseconds that the server should impose before
     * beginning processing for operations.
     *
     * @param  processingDelayMillis  The delay in milliseconds that the server
     *                                should impose before beginning processing
     *                                for operations.  A value less than or equal
     *                                to zero may be used to indicate that there
     *                                should be no delay.
     */
    public void setProcessingDelayMillis(final long processingDelayMillis) {
        inMemoryHandler.setProcessingDelayMillis(processingDelayMillis);
    }

    /**
     * Retrieves the number of entries currently held in the server.  The count
     * returned will not include entries which are part of the changelog.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @return  The number of entries currently held in the server.
     */
    public int countEntries() {
        return countEntries(false);
    }

    /**
     * Retrieves the number of entries currently held in the server, optionally
     * including those entries which are part of the changelog.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  includeChangeLog  Indicates whether to include entries that are
     *                           part of the changelog in the count.
     *
     * @return  The number of entries currently held in the server.
     */
    public int countEntries(final boolean includeChangeLog) {
        return inMemoryHandler.countEntries(includeChangeLog);
    }

    /**
     * Retrieves the number of entries currently held in the server whose DN
     * matches or is subordinate to the provided base DN.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  baseDN  The base DN to use for the determination.
     *
     * @return  The number of entries currently held in the server whose DN
     *          matches or is subordinate to the provided base DN.
     *
     * @throws  LDAPException  If the provided string cannot be parsed as a valid
     *                         DN.
     */
    public int countEntriesBelow(final String baseDN) throws LDAPException {
        return inMemoryHandler.countEntriesBelow(baseDN);
    }

    /**
     * Removes all entries currently held in the server.  If a changelog is
     * enabled, then all changelog entries will also be cleared but the base
     * "cn=changelog" entry will be retained.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     */
    public void clear() {
        inMemoryHandler.clear();
    }

    /**
     * Reads entries from the specified LDIF file and adds them to the server,
     * optionally clearing any existing entries before beginning to add the new
     * entries.  If an error is encountered while adding entries from LDIF then
     * the server will remain populated with the data it held before the import
     * attempt (even if the {@code clear} is given with a value of {@code true}).
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  clear  Indicates whether to remove all existing entries prior to
     *                adding entries read from LDIF.
     * @param  path   The path to the LDIF file from which the entries should be
     *                read.  It must not be {@code null}.
     *
     * @return  The number of entries read from LDIF and added to the server.
     *
     * @throws  LDAPException  If a problem occurs while reading entries or adding
     *                         them to the server.
     */
    public int importFromLDIF(final boolean clear, final String path) throws LDAPException {
        return importFromLDIF(clear, new File(path));
    }

    /**
     * Reads entries from the specified LDIF file and adds them to the server,
     * optionally clearing any existing entries before beginning to add the new
     * entries.  If an error is encountered while adding entries from LDIF then
     * the server will remain populated with the data it held before the import
     * attempt (even if the {@code clear} is given with a value of {@code true}).
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  clear     Indicates whether to remove all existing entries prior to
     *                   adding entries read from LDIF.
     * @param  ldifFile  The LDIF file from which the entries should be read.  It
     *                   must not be {@code null}.
     *
     * @return  The number of entries read from LDIF and added to the server.
     *
     * @throws  LDAPException  If a problem occurs while reading entries or adding
     *                         them to the server.
     */
    public int importFromLDIF(final boolean clear, final File ldifFile) throws LDAPException {
        final LDIFReader reader;
        try {
            reader = new LDIFReader(ldifFile);

            final Schema schema = getSchema();
            if (schema != null) {
                reader.setSchema(schema);
            }
        } catch (final Exception e) {
            Debug.debugException(e);
            throw new LDAPException(ResultCode.LOCAL_ERROR, e);
        }

        return importFromLDIF(clear, reader);
    }

    /**
     * Reads entries from the provided LDIF reader and adds them to the server,
     * optionally clearing any existing entries before beginning to add the new
     * entries.  If an error is encountered while adding entries from LDIF then
     * the server will remain populated with the data it held before the import
     * attempt (even if the {@code clear} is given with a value of {@code true}).
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  clear   Indicates whether to remove all existing entries prior to
     *                 adding entries read from LDIF.
     * @param  reader  The LDIF reader to use to obtain the entries to be
     *                 imported.
     *
     * @return  The number of entries read from LDIF and added to the server.
     *
     * @throws  LDAPException  If a problem occurs while reading entries or adding
     *                         them to the server.
     */
    public int importFromLDIF(final boolean clear, final LDIFReader reader) throws LDAPException {
        return inMemoryHandler.importFromLDIF(clear, reader);
    }

    /**
     * Writes the current contents of the server in LDIF form to the specified
     * file.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  path                   The path of the file to which the LDIF
     *                                entries should be written.
     * @param  excludeGeneratedAttrs  Indicates whether to exclude automatically
     *                                generated operational attributes like
     *                                entryUUID, entryDN, creatorsName, etc.
     * @param  excludeChangeLog       Indicates whether to exclude entries
     *                                contained in the changelog.
     *
     * @return  The number of entries written to LDIF.
     *
     * @throws  LDAPException  If a problem occurs while writing entries to LDIF.
     */
    public int exportToLDIF(final String path, final boolean excludeGeneratedAttrs, final boolean excludeChangeLog) throws LDAPException {
        final LDIFWriter ldifWriter;
        try {
            ldifWriter = new LDIFWriter(path);
        } catch (final Exception e) {
            Debug.debugException(e);
            throw new LDAPException(ResultCode.LOCAL_ERROR, e);
        }

        return exportToLDIF(ldifWriter, excludeGeneratedAttrs, excludeChangeLog, true);
    }

    /**
     * Writes the current contents of the server in LDIF form using the provided
     * LDIF writer.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  ldifWriter             The LDIF writer to use when writing the
     *                                entries.  It must not be {@code null}.
     * @param  excludeGeneratedAttrs  Indicates whether to exclude automatically
     *                                generated operational attributes like
     *                                entryUUID, entryDN, creatorsName, etc.
     * @param  excludeChangeLog       Indicates whether to exclude entries
     *                                contained in the changelog.
     * @param  closeWriter            Indicates whether the LDIF writer should be
     *                                closed after all entries have been written.
     *
     * @return  The number of entries written to LDIF.
     *
     * @throws  LDAPException  If a problem occurs while writing entries to LDIF.
     */
    public int exportToLDIF(final LDIFWriter ldifWriter, final boolean excludeGeneratedAttrs, final boolean excludeChangeLog,
            final boolean closeWriter) throws LDAPException {
        return inMemoryHandler.exportToLDIF(ldifWriter, excludeGeneratedAttrs, excludeChangeLog, closeWriter);
    }

    /**
     * Reads LDIF change records from the specified LDIF file and applies them
     * to the data in the server.  Any LDIF records without a changetype will be
     * treated as add change records.  If an error is encountered while attempting
     * to apply the requested changes, then the server will remain populated with
     * the data it held before this method was called, even if earlier changes
     * could have been applied successfully.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  path   The path to the LDIF file from which the LDIF change
     *                records should be read.  It must not be {@code null}.
     *
     * @return  The number of changes applied from the LDIF file.
     *
     * @throws  LDAPException  If a problem occurs while reading change records
     *                         or applying them to the server.
     */
    public int applyChangesFromLDIF(final String path) throws LDAPException {
        return applyChangesFromLDIF(new File(path));
    }

    /**
     * Reads LDIF change records from the specified LDIF file and applies them
     * to the data in the server.  Any LDIF records without a changetype will be
     * treated as add change records.  If an error is encountered while attempting
     * to apply the requested changes, then the server will remain populated with
     * the data it held before this method was called, even if earlier changes
     * could have been applied successfully.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  ldifFile  The LDIF file from which the LDIF change records should
     *                   be read.  It must not be {@code null}.
     *
     * @return  The number of changes applied from the LDIF file.
     *
     * @throws  LDAPException  If a problem occurs while reading change records
     *                         or applying them to the server.
     */
    public int applyChangesFromLDIF(final File ldifFile) throws LDAPException {
        final LDIFReader reader;
        try {
            reader = new LDIFReader(ldifFile);

            final Schema schema = getSchema();
            if (schema != null) {
                reader.setSchema(schema);
            }
        } catch (final Exception e) {
            Debug.debugException(e);
            throw new LDAPException(ResultCode.LOCAL_ERROR, e);
        }

        return applyChangesFromLDIF(reader);
    }

    /**
     * Reads LDIF change records from the provided LDIF reader file and applies
     * them to the data in the server.  Any LDIF records without a changetype will
     * be treated as add change records.  If an error is encountered while
     * attempting to apply the requested changes, then the server will remain
     * populated with the data it held before this method was called, even if
     * earlier changes could have been applied successfully.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  reader  The LDIF reader to use to obtain the change records to be
     *                 applied.
     *
     * @return  The number of changes applied from the LDIF file.
     *
     * @throws  LDAPException  If a problem occurs while reading change records
     *                         or applying them to the server.
     */
    public int applyChangesFromLDIF(final LDIFReader reader) throws LDAPException {
        return inMemoryHandler.applyChangesFromLDIF(reader);
    }

    /**
     * {@inheritDoc}
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     */
    public LDAPResult add(final Entry entry) throws LDAPException {
        return add(new AddRequest(entry));
    }

    /**
     * {@inheritDoc}
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     */
    public LDAPResult add(final String... ldifLines) throws LDIFException, LDAPException {
        return add(new AddRequest(ldifLines));
    }

    /**
     * {@inheritDoc}
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     */
    public LDAPResult add(final AddRequest addRequest) throws LDAPException {
        return inMemoryHandler.add(addRequest);
    }
    
    /**
     * {@inheritDoc}
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     */
    public Schema getSchema() throws LDAPException {
        return inMemoryHandler.getSchema();
    }

    /**
     * Attempts to add all of the provided entries to the server.  If a problem is
     * encountered while attempting to add any of the provided entries, then the
     * server will remain populated with the data it held before this method was
     * called.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     *
     * @param  entries  The entries to be added to the server.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to add
     *                         any of the provided entries.
     */
    public void addEntries(final Entry... entries) throws LDAPException {
        addEntries(Arrays.asList(entries));
    }

    /**
     * Attempts to add all of the provided entries to the server.  If a problem is
     * encountered while attempting to add any of the provided entries, then the
     * server will remain populated with the data it held before this method was
     * called.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     *
     * @param  entries  The entries to be added to the server.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to add
     *                         any of the provided entries.
     */
    public void addEntries(final List<? extends Entry> entries) throws LDAPException {
        inMemoryHandler.addEntries(entries);
    }

    /**
     * Attempts to add a set of entries provided in LDIF form in which each
     * element of the provided array is a line of the LDIF representation, with
     * empty strings as separators between entries (as you would have for blank
     * lines in an LDIF file).  If a problem is encountered while attempting to
     * add any of the provided entries, then the server will remain populated with
     * the data it held before this method was called.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether add operations are allowed in
     * the server.
     *
     * @param  ldifEntryLines  The lines comprising the LDIF representation of the
     *                         entries to be added.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to add
     *                         any of the provided entries.
     */
    public void addEntries(final String... ldifEntryLines) throws LDAPException {
        final ByteStringBuffer buffer = new ByteStringBuffer();
        for (final String line : ldifEntryLines) {
            buffer.append(line);
            buffer.append(StaticUtils.EOL_BYTES);
        }

        final ArrayList<Entry> entryList = new ArrayList<>(10);
        try (LDIFReader reader = new LDIFReader(buffer.asInputStream())) {

            final Schema schema = getSchema();
            if (schema != null) {
                reader.setSchema(schema);
            }

            while (true) {
                try {
                    final Entry entry = reader.readEntry();
                    if (entry == null) {
                        break;
                    } else {
                        entryList.add(entry);
                    }
                } catch (final Exception e) {
                    Debug.debugException(e);
                    throw new LDAPException(ResultCode.PARAM_ERROR, e);
                }
            }

            addEntries(entryList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes a simple bind request with the provided DN and password.  Note
     * that the bind processing will verify that the provided credentials are
     * valid, but it will not alter the server in any way.
     *
     * @param  bindDN    The bind DN for the bind operation.
     * @param  password  The password for the simple bind operation.
     *
     * @return  The result of processing the bind operation.
     *
     * @throws  LDAPException  If the server rejects the bind request, or if a
     *                         problem occurs while sending the request or reading
     *                         the response.
     */
    public BindResult bind(final String bindDN, final String password) throws LDAPException {
        return bind(new SimpleBindRequest(bindDN, password));
    }

    /**
     * Processes the provided bind request.  Only simple and SASL PLAIN bind
     * requests are supported.  Note that the bind processing will verify that the
     * provided credentials are valid, but it will not alter the server in any
     * way.
     *
     * @param  bindRequest  The bind request to be processed.  It must not be
     *                      {@code null}.
     *
     * @return  The result of processing the bind operation.
     *
     * @throws  LDAPException  If the server rejects the bind request, or if a
     *                         problem occurs while sending the request or reading
     *                         the response.
     */
    public BindResult bind(final BindRequest bindRequest) throws LDAPException {
        final ArrayList<Control> requestControlList = new ArrayList<>(bindRequest.getControlList());
        requestControlList.add(new Control("1.3.6.1.4.1.30221.2.5.18", false));
        
        final BindRequestProtocolOp bindOp;
        if (bindRequest instanceof SimpleBindRequest) {
            final SimpleBindRequest r = (SimpleBindRequest) bindRequest;
            bindOp = new BindRequestProtocolOp(r.getBindDN(), r.getPassword().getValue());
        } else if (bindRequest instanceof PLAINBindRequest) {
            final PLAINBindRequest r = (PLAINBindRequest) bindRequest;

            // Create the byte array that should comprise the credentials.
            final byte[] authZIDBytes = StaticUtils.getBytes(r.getAuthorizationID());
            final byte[] authNIDBytes = StaticUtils.getBytes(r.getAuthenticationID());
            final byte[] passwordBytes = r.getPasswordBytes();

            final byte[] credBytes = new byte[2 + authZIDBytes.length + authNIDBytes.length + passwordBytes.length];
            System.arraycopy(authZIDBytes, 0, credBytes, 0, authZIDBytes.length);

            int pos = authZIDBytes.length + 1;
            System.arraycopy(authNIDBytes, 0, credBytes, pos, authNIDBytes.length);

            pos += authNIDBytes.length + 1;
            System.arraycopy(passwordBytes, 0, credBytes, pos, passwordBytes.length);

            bindOp = new BindRequestProtocolOp(null, "PLAIN", new ASN1OctetString(credBytes));
        } else {
            throw new LDAPException(ResultCode.AUTH_METHOD_NOT_SUPPORTED);
        }

        final LDAPMessage responseMessage = inMemoryHandler.processBindRequest(1, bindOp, requestControlList);
        final BindResponseProtocolOp bindResponse = responseMessage.getBindResponseProtocolOp();

        final BindResult bindResult = new BindResult(new LDAPResult(responseMessage.getMessageID(), ResultCode.valueOf(bindResponse.getResultCode()),
                bindResponse.getDiagnosticMessage(), bindResponse.getMatchedDN(), bindResponse.getReferralURLs(), responseMessage.getControls()));

        switch (bindResponse.getResultCode()) {
        case ResultCode.SUCCESS_INT_VALUE:
            return bindResult;
        default:
            throw new LDAPException(bindResult);
        }
    }


    /**
     * Attempts to delete the specified entry and all entries below it from the
     * server.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether compare operations are
     * allowed in the server.
     *
     * @param  baseDN  The DN of the entry to remove, along with all of its
     *                 subordinates.
     *
     * @return  The number of entries removed from the server, or zero if the
     *          specified entry was not found.
     *
     * @throws  LDAPException  If a problem is encountered while attempting to
     *                         remove the entries.
     */
    public int deleteSubtree(final String baseDN) throws LDAPException {
        return inMemoryHandler.deleteSubtree(baseDN);
    }

    /**
     * Processes an extended request with the provided request OID.  Note that
     * because some types of extended operations return unusual result codes under
     * "normal" conditions, the server may not always throw an exception for a
     * failed extended operation like it does for other types of operations.  It
     * will throw an exception under conditions where there appears to be a
     * problem with the connection or the server to which the connection is
     * established, but there may be many circumstances in which an extended
     * operation is not processed correctly but this method does not throw an
     * exception.  In the event that no exception is thrown, it is the
     * responsibility of the caller to interpret the result to determine whether
     * the operation was processed as expected.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether extended operations are
     * allowed in the server.
     *
     * @param  requestOID  The OID for the extended request to process.  It must
     *                     not be {@code null}.
     *
     * @return  The extended result object that provides information about the
     *          result of the request processing.  It may or may not indicate that
     *          the operation was successful.
     *
     * @throws  LDAPException  If a problem occurs while sending the request or
     *                         reading the response.
     */
    public ExtendedResult processExtendedOperation(final String requestOID) throws LDAPException {
        Validator.ensureNotNull(requestOID);

        return processExtendedOperation(new ExtendedRequest(requestOID));
    }

    /**
     * Processes an extended request with the provided request OID and value.
     * Note that because some types of extended operations return unusual result
     * codes under "normal" conditions, the server may not always throw an
     * exception for a failed extended operation like it does for other types of
     * operations.  It will throw an exception under conditions where there
     * appears to be a problem with the connection or the server to which the
     * connection is established, but there may be many circumstances in which an
     * extended operation is not processed correctly but this method does not
     * throw an exception.  In the event that no exception is thrown, it is the
     * responsibility of the caller to interpret the result to determine whether
     * the operation was processed as expected.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether extended operations are
     * allowed in the server.
     *
     * @param  requestOID    The OID for the extended request to process.  It must
     *                       not be {@code null}.
     * @param  requestValue  The encoded value for the extended request to
     *                       process.  It may be {@code null} if there does not
     *                       need to be a value for the requested operation.
     *
     * @return  The extended result object that provides information about the
     *          result of the request processing.  It may or may not indicate that
     *          the operation was successful.
     *
     * @throws  LDAPException  If a problem occurs while sending the request or
     *                         reading the response.
     */
    public ExtendedResult processExtendedOperation(final String requestOID, final ASN1OctetString requestValue) throws LDAPException {
        Validator.ensureNotNull(requestOID);

        return processExtendedOperation(new ExtendedRequest(requestOID, requestValue));
    }

    /**
     * Processes the provided extended request.  Note that because some types of
     * extended operations return unusual result codes under "normal" conditions,
     * the server may not always throw an exception for a failed extended
     * operation like it does for other types of operations.  It will throw an
     * exception under conditions where there appears to be a problem with the
     * connection or the server to which the connection is established, but there
     * may be many circumstances in which an extended operation is not processed
     * correctly but this method does not throw an exception.  In the event that
     * no exception is thrown, it is the responsibility of the caller to interpret
     * the result to determine whether the operation was processed as expected.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections, and regardless of whether extended operations are
     * allowed in the server.
     *
     * @param  extendedRequest  The extended request to be processed.  It must not
     *                          be {@code null}.
     *
     * @return  The extended result object that provides information about the
     *          result of the request processing.  It may or may not indicate that
     *          the operation was successful.
     *
     * @throws  LDAPException  If a problem occurs while sending the request or
     *                         reading the response.
     */
    public ExtendedResult processExtendedOperation(final ExtendedRequest extendedRequest) throws LDAPException {
        Validator.ensureNotNull(extendedRequest);

        final ArrayList<Control> requestControlList = new ArrayList<>(extendedRequest.getControlList());
        requestControlList.add(new Control("1.3.6.1.4.1.30221.2.5.18", false));

        final LDAPMessage responseMessage = inMemoryHandler.processExtendedRequest(1,
                new ExtendedRequestProtocolOp(extendedRequest.getOID(), extendedRequest.getValue()), requestControlList);

        final ExtendedResponseProtocolOp extendedResponse = responseMessage.getExtendedResponseProtocolOp();

        final ResultCode rc = ResultCode.valueOf(extendedResponse.getResultCode());

        final String[] referralURLs;
        final List<String> referralURLList = extendedResponse.getReferralURLs();
        if ((referralURLList == null) || referralURLList.isEmpty()) {
            referralURLs = StaticUtils.NO_STRINGS;
        } else {
            referralURLs = new String[referralURLList.size()];
            referralURLList.toArray(referralURLs);
        }

        final Control[] responseControls;
        final List<Control> controlList = responseMessage.getControls();
        if ((controlList == null) || controlList.isEmpty()) {
            responseControls = StaticUtils.NO_CONTROLS;
        } else {
            responseControls = new Control[controlList.size()];
            controlList.toArray(responseControls);
        }

        final ExtendedResult extendedResult = new ExtendedResult(responseMessage.getMessageID(), rc, extendedResponse.getDiagnosticMessage(),
                extendedResponse.getMatchedDN(), referralURLs, extendedResponse.getResponseOID(), extendedResponse.getResponseValue(),
                responseControls);

        if ((extendedResult.getOID() == null) && (extendedResult.getValue() == null)) {
            switch (rc.intValue()) {
            case ResultCode.OPERATIONS_ERROR_INT_VALUE:
            case ResultCode.PROTOCOL_ERROR_INT_VALUE:
            case ResultCode.BUSY_INT_VALUE:
            case ResultCode.UNAVAILABLE_INT_VALUE:
            case ResultCode.OTHER_INT_VALUE:
            case ResultCode.SERVER_DOWN_INT_VALUE:
            case ResultCode.LOCAL_ERROR_INT_VALUE:
            case ResultCode.ENCODING_ERROR_INT_VALUE:
            case ResultCode.DECODING_ERROR_INT_VALUE:
            case ResultCode.TIMEOUT_INT_VALUE:
            case ResultCode.NO_MEMORY_INT_VALUE:
            case ResultCode.CONNECT_ERROR_INT_VALUE:
                throw new LDAPException(extendedResult);
            }
        }

        return extendedResult;
    }

 

    /**
     * Retrieves the configured list of password attributes.
     *
     * @return  The configured list of password attributes.
     */
    public List<String> getPasswordAttributes() {
        return inMemoryHandler.getPasswordAttributes();
    }

    /**
     * Retrieves the primary password encoder that has been configured for the
     * server.
     *
     * @return  The primary password encoder that has been configured for the
     *          server.
     */
    public InMemoryPasswordEncoder getPrimaryPasswordEncoder() {
        return inMemoryHandler.getPrimaryPasswordEncoder();
    }

    /**
     * Retrieves a list of all password encoders configured for the server.
     *
     * @return  A list of all password encoders configured for the server.
     */
    public List<InMemoryPasswordEncoder> getAllPasswordEncoders() {
        return inMemoryHandler.getAllPasswordEncoders();
    }

    /**
     * Retrieves a list of the passwords contained in the provided entry.
     *
     * @param  entry                 The entry from which to obtain the list of
     *                               passwords.  It must not be {@code null}.
     * @param  clearPasswordToMatch  An optional clear-text password that should
     *                               match the values that are returned.  If this
     *                               is {@code null}, then all passwords contained
     *                               in the provided entry will be returned.  If
     *                               this is non-{@code null}, then only passwords
     *                               matching the clear-text password will be
     *                               returned.
     *
     * @return  A list of the passwords contained in the provided entry,
     *          optionally restricted to those matching the provided clear-text
     *          password, or an empty list if the entry does not contain any
     *          passwords.
     */
    public List<InMemoryDirectoryServerPassword> getPasswordsInEntry(final Entry entry, final ASN1OctetString clearPasswordToMatch) {
        return inMemoryHandler.getPasswordsInEntry(entry, clearPasswordToMatch);
    }

 
    /**
     * Indicates whether the specified entry exists in the server.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn  The DN of the entry for which to make the determination.
     *
     * @return  {@code true} if the entry exists, or {@code false} if not.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public boolean entryExists(final String dn) throws LDAPException {
        return inMemoryHandler.entryExists(dn);
    }

    /**
     * Indicates whether the specified entry exists in the server and matches the
     * given filter.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn      The DN of the entry for which to make the determination.
     * @param  filter  The filter the entry is expected to match.
     *
     * @return  {@code true} if the entry exists and matches the specified filter,
     *          or {@code false} if not.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public boolean entryExists(final String dn, final String filter) throws LDAPException {
        return inMemoryHandler.entryExists(dn, filter);
    }

    /**
     * Indicates whether the specified entry exists in the server.  This will
     * return {@code true} only if the target entry exists and contains all values
     * for all attributes of the provided entry.  The entry will be allowed to
     * have attribute values not included in the provided entry.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  entry  The entry to compare against the directory server.
     *
     * @return  {@code true} if the entry exists in the server and is a superset
     *          of the provided entry, or {@code false} if not.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public boolean entryExists(final Entry entry) throws LDAPException {
        return inMemoryHandler.entryExists(entry);
    }

    /**
     * Ensures that an entry with the provided DN exists in the directory.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn  The DN of the entry for which to make the determination.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist.
     */
    public void assertEntryExists(final String dn) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntryExists(dn);
    }

    /**
     * Ensures that an entry with the provided DN exists in the directory.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn      The DN of the entry for which to make the determination.
     * @param  filter  A filter that the target entry must match.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist or does not
     *                          match the provided filter.
     */
    public void assertEntryExists(final String dn, final String filter) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntryExists(dn, filter);
    }

    /**
     * Ensures that an entry exists in the directory with the same DN and all
     * attribute values contained in the provided entry.  The server entry may
     * contain additional attributes and/or attribute values not included in the
     * provided entry.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  entry  The entry expected to be present in the directory server.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist or does not
     *                          match the provided filter.
     */
    public void assertEntryExists(final Entry entry) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntryExists(entry);
    }

    /**
     * Retrieves a list containing the DNs of the entries which are missing from
     * the directory server.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dns  The DNs of the entries to try to find in the server.
     *
     * @return  A list containing all of the provided DNs that were not found in
     *          the server, or an empty list if all entries were found.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingEntryDNs(final String... dns) throws LDAPException {
        return inMemoryHandler.getMissingEntryDNs(StaticUtils.toList(dns));
    }

    /**
     * Retrieves a list containing the DNs of the entries which are missing from
     * the directory server.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dns  The DNs of the entries to try to find in the server.
     *
     * @return  A list containing all of the provided DNs that were not found in
     *          the server, or an empty list if all entries were found.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingEntryDNs(final Collection<String> dns) throws LDAPException {
        return inMemoryHandler.getMissingEntryDNs(dns);
    }

    /**
     * Ensures that all of the entries with the provided DNs exist in the
     * directory.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dns  The DNs of the entries for which to make the determination.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If any of the target entries does not exist.
     */
    public void assertEntriesExist(final String... dns) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntriesExist(StaticUtils.toList(dns));
    }

    /**
     * Ensures that all of the entries with the provided DNs exist in the
     * directory.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dns  The DNs of the entries for which to make the determination.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If any of the target entries does not exist.
     */
    public void assertEntriesExist(final Collection<String> dns) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntriesExist(dns);
    }

    /**
     * Retrieves a list containing all of the named attributes which do not exist
     * in the target entry.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry to examine.
     * @param  attributeNames  The names of the attributes expected to be present
     *                         in the target entry.
     *
     * @return  A list containing the names of the attributes which were not
     *          present in the target entry, an empty list if all specified
     *          attributes were found in the entry, or {@code null} if the target
     *          entry does not exist.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingAttributeNames(final String dn, final String... attributeNames) throws LDAPException {
        return inMemoryHandler.getMissingAttributeNames(dn, StaticUtils.toList(attributeNames));
    }

    /**
     * Retrieves a list containing all of the named attributes which do not exist
     * in the target entry.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry to examine.
     * @param  attributeNames  The names of the attributes expected to be present
     *                         in the target entry.
     *
     * @return  A list containing the names of the attributes which were not
     *          present in the target entry, an empty list if all specified
     *          attributes were found in the entry, or {@code null} if the target
     *          entry does not exist.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingAttributeNames(final String dn, final Collection<String> attributeNames) throws LDAPException {
        return inMemoryHandler.getMissingAttributeNames(dn, attributeNames);
    }

    /**
     * Ensures that the specified entry exists in the directory with all of the
     * specified attributes.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry to examine.
     * @param  attributeNames  The names of the attributes that are expected to be
     *                         present in the provided entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist or does not
     *                          contain all of the specified attributes.
     */
    public void assertAttributeExists(final String dn, final String... attributeNames) throws LDAPException, AssertionError {
        inMemoryHandler.assertAttributeExists(dn, StaticUtils.toList(attributeNames));
    }

    /**
     * Ensures that the specified entry exists in the directory with all of the
     * specified attributes.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry to examine.
     * @param  attributeNames  The names of the attributes that are expected to be
     *                         present in the provided entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist or does not
     *                          contain all of the specified attributes.
     */
    public void assertAttributeExists(final String dn, final Collection<String> attributeNames) throws LDAPException, AssertionError {
        inMemoryHandler.assertAttributeExists(dn, attributeNames);
    }

    /**
     * Retrieves a list of all provided attribute values which are missing from
     * the specified entry.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry to examine.
     * @param  attributeName    The attribute expected to be present in the target
     *                          entry with the given values.
     * @param  attributeValues  The values expected to be present in the target
     *                          entry.
     *
     * @return  A list containing all of the provided values which were not found
     *          in the entry, an empty list if all provided attribute values were
     *          found, or {@code null} if the target entry does not exist.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingAttributeValues(final String dn, final String attributeName, final String... attributeValues) throws LDAPException {
        return inMemoryHandler.getMissingAttributeValues(dn, attributeName, StaticUtils.toList(attributeValues));
    }

    /**
     * Retrieves a list of all provided attribute values which are missing from
     * the specified entry.  The target attribute may or may not contain
     * additional values.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry to examine.
     * @param  attributeName    The attribute expected to be present in the target
     *                          entry with the given values.
     * @param  attributeValues  The values expected to be present in the target
     *                          entry.
     *
     * @return  A list containing all of the provided values which were not found
     *          in the entry, an empty list if all provided attribute values were
     *          found, or {@code null} if the target entry does not exist.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     */
    public List<String> getMissingAttributeValues(final String dn, final String attributeName, final Collection<String> attributeValues)
            throws LDAPException {
        return inMemoryHandler.getMissingAttributeValues(dn, attributeName, attributeValues);
    }

    /**
     * Ensures that the specified entry exists in the directory with all of the
     * specified values for the given attribute.  The attribute may or may not
     * contain additional values.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry to examine.
     * @param  attributeName    The name of the attribute to examine.
     * @param  attributeValues  The set of values which must exist for the given
     *                          attribute.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist, does not
     *                          contain the specified attribute, or that attribute
     *                          does not have all of the specified values.
     */
    public void assertValueExists(final String dn, final String attributeName, final String... attributeValues) throws LDAPException, AssertionError {
        inMemoryHandler.assertValueExists(dn, attributeName, StaticUtils.toList(attributeValues));
    }

    /**
     * Ensures that the specified entry exists in the directory with all of the
     * specified values for the given attribute.  The attribute may or may not
     * contain additional values.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry to examine.
     * @param  attributeName    The name of the attribute to examine.
     * @param  attributeValues  The set of values which must exist for the given
     *                          attribute.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry does not exist, does not
     *                          contain the specified attribute, or that attribute
     *                          does not have all of the specified values.
     */
    public void assertValueExists(final String dn, final String attributeName, final Collection<String> attributeValues)
            throws LDAPException, AssertionError {
        inMemoryHandler.assertValueExists(dn, attributeName, attributeValues);
    }

    /**
     * Ensures that the specified entry does not exist in the directory.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn  The DN of the entry expected to be missing.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry is found in the server.
     */
    public void assertEntryMissing(final String dn) throws LDAPException, AssertionError {
        inMemoryHandler.assertEntryMissing(dn);
    }

    /**
     * Ensures that the specified entry exists in the directory but does not
     * contain any of the specified attributes.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry expected to be present.
     * @param  attributeNames  The names of the attributes expected to be missing
     *                         from the entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry is missing from the server, or
     *                          if it contains any of the target attributes.
     */
    public void assertAttributeMissing(final String dn, final String... attributeNames) throws LDAPException, AssertionError {
        inMemoryHandler.assertAttributeMissing(dn, StaticUtils.toList(attributeNames));
    }

    /**
     * Ensures that the specified entry exists in the directory but does not
     * contain any of the specified attributes.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn              The DN of the entry expected to be present.
     * @param  attributeNames  The names of the attributes expected to be missing
     *                         from the entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry is missing from the server, or
     *                          if it contains any of the target attributes.
     */
    public void assertAttributeMissing(final String dn, final Collection<String> attributeNames) throws LDAPException, AssertionError {
        inMemoryHandler.assertAttributeMissing(dn, attributeNames);
    }

    /**
     * Ensures that the specified entry exists in the directory but does not
     * contain any of the specified attribute values.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry expected to be present.
     * @param  attributeName    The name of the attribute to examine.
     * @param  attributeValues  The values expected to be missing from the target
     *                          entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry is missing from the server, or
     *                          if it contains any of the target attribute values.
     */
    public void assertValueMissing(final String dn, final String attributeName, final String... attributeValues)
            throws LDAPException, AssertionError {
        inMemoryHandler.assertValueMissing(dn, attributeName, StaticUtils.toList(attributeValues));
    }

    /**
     * Ensures that the specified entry exists in the directory but does not
     * contain any of the specified attribute values.
     * <BR><BR>
     * This method may be used regardless of whether the server is listening for
     * client connections.
     *
     * @param  dn               The DN of the entry expected to be present.
     * @param  attributeName    The name of the attribute to examine.
     * @param  attributeValues  The values expected to be missing from the target
     *                          entry.
     *
     * @throws  LDAPException  If a problem is encountered while trying to
     *                         communicate with the directory server.
     *
     * @throws  AssertionError  If the target entry is missing from the server, or
     *                          if it contains any of the target attribute values.
     */
    public void assertValueMissing(final String dn, final String attributeName, final Collection<String> attributeValues)
            throws LDAPException, AssertionError {
        inMemoryHandler.assertValueMissing(dn, attributeName, attributeValues);
    }
}

class PlaintextRequestRejectingRequestHandler extends LDAPListenerRequestHandler {
    private final LDAPListenerRequestHandler delegate;
    private final LDAPListenerClientConnection connection;

    PlaintextRequestRejectingRequestHandler(LDAPListenerRequestHandler delegate, LDAPListenerClientConnection connection) {
        this.delegate = delegate;
        this.connection = connection;
    }

    public void closeInstance() {
        delegate.closeInstance();
    }

    public void processAbandonRequest(int messageID, AbandonRequestProtocolOp request, List<Control> controls) {
        delegate.processAbandonRequest(messageID, request, controls);
    }

    public LDAPMessage processAddRequest(int messageID, AddRequestProtocolOp request, List<Control> controls) {
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new AddResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }

        return delegate.processAddRequest(messageID, request, controls);
    }

    public LDAPMessage processBindRequest(int messageID, BindRequestProtocolOp request, List<Control> controls) {
        //System.out.println(this + " " + messageID + " bind " + isTlsConnection());
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new BindResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }

        return delegate.processBindRequest(messageID, request, controls);
    }

    public LDAPMessage processCompareRequest(int messageID, CompareRequestProtocolOp request, List<Control> controls) {

        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new CompareResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }
        return delegate.processCompareRequest(messageID, request, controls);
    }

    public LDAPMessage processDeleteRequest(int messageID, DeleteRequestProtocolOp request, List<Control> controls) {
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new DeleteResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }

        return delegate.processDeleteRequest(messageID, request, controls);
    }

    public LDAPMessage processExtendedRequest(int messageID, ExtendedRequestProtocolOp request, List<Control> controls) {
        //System.out.println(this + " " + messageID + " extend " + isTlsConnection());

        if (request.getOID().equals(StartTLSExtendedRequest.STARTTLS_REQUEST_OID)) {
            return delegate.processExtendedRequest(messageID, request, controls);
        } else {
            if (!isTlsConnection()) {
                return new LDAPMessage(messageID, new ExtendedResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
            }

            return delegate.processExtendedRequest(messageID, request, controls);
        }
    }

    public LDAPMessage processModifyRequest(int messageID, ModifyRequestProtocolOp request, List<Control> controls) {
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new ModifyResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }
        return delegate.processModifyRequest(messageID, request, controls);
    }

    public LDAPMessage processModifyDNRequest(int messageID, ModifyDNRequestProtocolOp request, List<Control> controls) {
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new ModifyDNResponseProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }
        return delegate.processModifyDNRequest(messageID, request, controls);
    }

    public LDAPMessage processSearchRequest(int messageID, SearchRequestProtocolOp request, List<Control> controls) {
        if (!isTlsConnection()) {
            return new LDAPMessage(messageID, new SearchResultDoneProtocolOp(new LDAPResult(messageID, ResultCode.UNWILLING_TO_PERFORM)));
        }
        return delegate.processSearchRequest(messageID, request, controls);
    }

    public void processUnbindRequest(int messageID, UnbindRequestProtocolOp request, List<Control> controls) {
        delegate.processUnbindRequest(messageID, request, controls);
    }

    @Override
    public LDAPListenerRequestHandler newInstance(LDAPListenerClientConnection connection) throws LDAPException {
        return new PlaintextRequestRejectingRequestHandler(delegate.newInstance(connection), connection);
    }

    private boolean isTlsConnection() {
        return connection.getSocket() instanceof SSLSocket;
    }

}

class DelayedBindRequestHandler extends LDAPListenerRequestHandler {
    private final LDAPListenerRequestHandler delegate;
    private final Duration bindRequestDelay;
   

    DelayedBindRequestHandler(LDAPListenerRequestHandler delegate, Duration bindRequestDelay) {
        this.delegate = delegate;
        this.bindRequestDelay = bindRequestDelay;
    }

    public void closeInstance() {
        delegate.closeInstance();
    }

    public void processAbandonRequest(int messageID, AbandonRequestProtocolOp request, List<Control> controls) {
        delegate.processAbandonRequest(messageID, request, controls);
    }

    public LDAPMessage processAddRequest(int messageID, AddRequestProtocolOp request, List<Control> controls) {
        return delegate.processAddRequest(messageID, request, controls);
    }

    public LDAPMessage processBindRequest(int messageID, BindRequestProtocolOp request, List<Control> controls) {
        try {
            Thread.sleep(bindRequestDelay.toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return delegate.processBindRequest(messageID, request, controls);
    }

    public LDAPMessage processCompareRequest(int messageID, CompareRequestProtocolOp request, List<Control> controls) {
        return delegate.processCompareRequest(messageID, request, controls);
    }

    public LDAPMessage processDeleteRequest(int messageID, DeleteRequestProtocolOp request, List<Control> controls) {
        return delegate.processDeleteRequest(messageID, request, controls);
    }

    public LDAPMessage processExtendedRequest(int messageID, ExtendedRequestProtocolOp request, List<Control> controls) {
            return delegate.processExtendedRequest(messageID, request, controls);
    }

    public LDAPMessage processModifyRequest(int messageID, ModifyRequestProtocolOp request, List<Control> controls) {
        return delegate.processModifyRequest(messageID, request, controls);
    }

    public LDAPMessage processModifyDNRequest(int messageID, ModifyDNRequestProtocolOp request, List<Control> controls) {
        return delegate.processModifyDNRequest(messageID, request, controls);
    }

    public LDAPMessage processSearchRequest(int messageID, SearchRequestProtocolOp request, List<Control> controls) {
        return delegate.processSearchRequest(messageID, request, controls);
    }

    public void processUnbindRequest(int messageID, UnbindRequestProtocolOp request, List<Control> controls) {
        delegate.processUnbindRequest(messageID, request, controls);
    }

    @Override
    public LDAPListenerRequestHandler newInstance(LDAPListenerClientConnection connection) throws LDAPException {
        return new DelayedBindRequestHandler(delegate.newInstance(connection), bindRequestDelay);
    }

}
