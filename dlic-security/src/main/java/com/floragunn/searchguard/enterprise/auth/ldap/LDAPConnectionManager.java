/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
 */

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.net.SocketFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchsupport.PrivilegedCode;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Count;
import com.google.common.primitives.Ints;
import com.unboundid.ldap.sdk.AggregateLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.EXTERNALBindRequest;
import com.unboundid.ldap.sdk.FailoverServerSet;
import com.unboundid.ldap.sdk.FastestConnectServerSet;
import com.unboundid.ldap.sdk.FewestConnectionsServerSet;
import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.PostConnectProcessor;
import com.unboundid.ldap.sdk.PruneUnneededConnectionsLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.RoundRobinServerSet;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.StartTLSPostConnectProcessor;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;

public final class LDAPConnectionManager implements Closeable, ComponentStateProvider {

    private static final Logger log = LogManager.getLogger(LDAPConnectionManager.class);
    private final LDAPConnectionPool pool;
    private final TLSConfig tlsConfig;
    private final int poolMinSize;
    private final int poolMaxSize;
    private final ConnectionStrategy connectionStrategy;
    private final ComponentState componentState = new ComponentState(0, null, "ldap_connection_pool", LDAPConnectionManager.class);
    
    public static enum ConnectionStrategy {
        FEWEST, FAILOVER, FASTEST, ROUNDROBIN;
    }

    public LDAPConnectionManager(DocNode config, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        this.tlsConfig = vNode.get("tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parseInclStartTlsSupport);

        List<String> ldapHosts = vNode.get("hosts").required().asList().ofStrings();
        this.connectionStrategy = vNode.get("connection_strategy").withDefault(ConnectionStrategy.ROUNDROBIN).asEnum(ConnectionStrategy.class);

        String bindDn = vNode.get("bind_dn").asString();
        String password = vNode.get("password").asString();

        final BindRequest bindRequest;
        if (bindDn != null && password != null && password.length() > 0) {
            bindRequest = new SimpleBindRequest(bindDn, password);
        } else if (tlsConfig != null && tlsConfig.getClientCertAuthConfig() != null) {
            bindRequest = new EXTERNALBindRequest();
        } else {
            bindRequest = new SimpleBindRequest();
        }

        LDAPConnectionOptions opts = new LDAPConnectionOptions();

        if (tlsConfig != null && tlsConfig.isHostnameVerificationEnabled()) {
            opts.setSSLSocketVerifier(new HostNameSSLSocketVerifier(false));
        }

        Duration connectTimeout = vNode.get("connect_timeout").asDuration();

        if (connectTimeout != null) {
            opts.setConnectTimeoutMillis((int) connectTimeout.toMillis());
        }

        Duration responseTimeout = vNode.get("response_timeout").asDuration();

        if (responseTimeout != null) {
            opts.setResponseTimeoutMillis(responseTimeout.toMillis());
        }

        boolean followReferrals = vNode.get("follow_referrals").withDefault(true).asBoolean();
        opts.setFollowReferrals(followReferrals);

        this.poolMinSize = vNode.get("connection_pool.min_size").withDefault(3).asInt();
        this.poolMaxSize = vNode.get("connection_pool.max_size").withDefault(10).asInt();

        boolean createIfNecessary;
        long maxWaitTimeMillis; //0L is the default which means no blocking at all

        boolean blocking = vNode.get("connection_pool.blocking").withDefault(false).asBoolean();

        if (blocking) {
            //pool enabled in blocking mode
            maxWaitTimeMillis = Long.MAX_VALUE;
            createIfNecessary = false;
        } else {
            //pool enabled in non blocking mode
            maxWaitTimeMillis = 0L;
            createIfNecessary = true;
        }

        LDAPConnectionPoolHealthCheck healthChecks = vNode.get("connection_pool").by((n) -> getHealthChecks(n));
        Duration healthCheckInterval = vNode.get("connection_pool.health_check_interval").asDuration();

        validationErrors.throwExceptionForPresentErrors();

        if (context.isExternalResourceCreationEnabled()) {
            try {
                pool = PrivilegedCode.execute(
                        () -> new LDAPConnectionPool(createServerSet(ldapHosts, opts), bindRequest, poolMinSize, poolMaxSize, null, false),
                        LDAPException.class);
            } catch (LDAPException e) {
                log.error("Error while creating pool", e);
                throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
            }

            pool.setCreateIfNecessary(createIfNecessary);
            pool.setMaxWaitTimeMillis(maxWaitTimeMillis);

            if (healthChecks != null) {
                pool.setHealthCheck(healthChecks);

                if (healthCheckInterval != null) {
                    pool.setHealthCheckIntervalMillis(healthCheckInterval.toMillis());
                }
            }
            
            componentState.setConfigProperty("min_size", poolMinSize);
            componentState.setConfigProperty("max_size", poolMaxSize);
            
            componentState.addMetrics("current_available_connections", new Count.Live(() -> (long) pool.getCurrentAvailableConnections()));
            componentState.addMetrics("connections_closed_defunct", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumConnectionsClosedDefunct()));
            componentState.addMetrics("connections_closed_expired", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumConnectionsClosedExpired()));
            componentState.addMetrics("connections_closed_unneeded", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumConnectionsClosedUnneeded()));
            componentState.addMetrics("failed_checkouts", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumFailedCheckouts()));
            componentState.addMetrics("failed_connection_attempts", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumFailedConnectionAttempts()));
            componentState.addMetrics("released_back_to_pool", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumReleasedValid()));
            componentState.addMetrics("successful_checkouts_from_pool", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumSuccessfulCheckouts()));
            componentState.addMetrics("successful_checkouts_from_pool_after_wait", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumSuccessfulCheckoutsAfterWaiting()));
            componentState.addMetrics("successful_checkouts_from_pool_without_wait", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumSuccessfulCheckoutsWithoutWaiting()));
            componentState.addMetrics("successful_checkouts_new_connection", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumSuccessfulCheckoutsNewConnection()));
            componentState.addMetrics("successful_connection_attempts", new Count.Live(() -> pool.getConnectionPoolStatistics().getNumSuccessfulConnectionAttempts()));

            
            componentState.setInitialized();
        } else {
            pool = null;
        }
    }

    private LDAPConnectionPoolHealthCheck getHealthChecks(DocNode config) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);
        List<LDAPConnectionPoolHealthCheck> healthChecks = new ArrayList<>();

        boolean validationEnabled = vNode.get("validation.enabled").withDefault(false).asBoolean();

        String entryDN = vNode.get("validation.dn").asString(); //null means root dse
        long maxResponseTime = vNode.get("validation.max_response_time").withDefault(30000L).asLong();
        boolean invokeOnCreate = vNode.get("validation.on_create").withDefault(false).asBoolean();
        boolean invokeAfterAuthentication = vNode.get("validation.after_authentication").withDefault(false).asBoolean();
        boolean invokeOnCheckout = vNode.get("validation.on_checkout").withDefault(false).asBoolean();
        boolean invokeOnRelease = vNode.get("validation.on_release").withDefault(false).asBoolean();
        boolean invokeForBackgroundChecks = vNode.get("validation.for_background_checks").withDefault(true).asBoolean();
        boolean invokeOnException = vNode.get("validation.on_exception").withDefault(false).asBoolean();

        if (validationEnabled) {
            healthChecks.add(new GetEntryLDAPConnectionPoolHealthCheck(entryDN, maxResponseTime, invokeOnCreate, invokeAfterAuthentication,
                    invokeOnCheckout, invokeOnRelease, invokeForBackgroundChecks, invokeOnException));
        }

        boolean pruningEnabled = vNode.get("pruning.enabled").withDefault(false).asBoolean();

        int minAvailableConnections = vNode.get("pruning.min_available_connections").withDefault(poolMaxSize).asInt();
        long minDurationMillisExceedingMinAvailableConnections = vNode.get("pruning.min_duration_millis_exceeding_min_available_connections")
                .withDefault(0L).asLong();

        if (pruningEnabled) {
            healthChecks.add(new PruneUnneededConnectionsLDAPConnectionPoolHealthCheck(minAvailableConnections,
                    minDurationMillisExceedingMinAvailableConnections));
        }

        validationErrors.throwExceptionForPresentErrors();

        if (healthChecks.size() == 1) {
            return healthChecks.get(0);
        } else if (healthChecks.size() > 1) {
            return new AggregateLDAPConnectionPoolHealthCheck(healthChecks);
        } else {
            return null;
        }
    }

    private ServerSet createServerSet(final Collection<String> ldapStrings, LDAPConnectionOptions opts) throws LDAPException {
        final List<String> ldapHosts = new ArrayList<>();
        final List<Integer> ldapPorts = new ArrayList<>();

        for (String ldapString : ldapStrings) {

            if (ldapString == null || (ldapString = ldapString.trim()).isEmpty()) {
                continue;
            }

            int port = this.tlsConfig != null ? 636 : 389;

            if (ldapString.startsWith("ldap://")) {
                ldapString = ldapString.replace("ldap://", "");
            }

            if (ldapString.startsWith("ldaps://")) {
                ldapString = ldapString.replace("ldaps://", "");
                port = 636;
            }

            final String[] split = ldapString.split(":");

            if (split.length > 1) {
                port = Integer.parseInt(split[1]);
            }

            ldapHosts.add(split[0]);
            ldapPorts.add(port);
        }

        if (tlsConfig != null) {
            if (!tlsConfig.isStartTlsEnabled()) {
                return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), tlsConfig.getRestrictedSSLSocketFactory(), opts,
                        null, null);
            } else {
                return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), null, opts, null,
                        new StartTLSPostConnectProcessor(tlsConfig.getRestrictedSSLSocketFactory()));
            }
        }

        return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), null, opts, null, null);
    }

    private ServerSet newServerSetImpl(final String[] addresses, final int[] ports, final SocketFactory socketFactory,
            final LDAPConnectionOptions connectionOptions, final BindRequest bindRequest, final PostConnectProcessor postConnectProcessor)
            throws LDAPException {

        switch (connectionStrategy) {
        case FAILOVER:
            return new PrivilegedServerSet(new FailoverServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor));
        case FASTEST:
            return new PrivilegedServerSet(new FastestConnectServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor));
        case FEWEST:
            return new PrivilegedServerSet(new FewestConnectionsServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor));
        case ROUNDROBIN:
            return new PrivilegedServerSet(new RoundRobinServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor));
        default:
            throw new RuntimeException("Unexpected connectionStrategy " + connectionStrategy);
        }
    }

    public LDAPConnection getConnection() throws AuthenticatorUnavailableException {
        try {
            return pool.getConnection();
        } catch (LDAPException e) {
            throw new AuthenticatorUnavailableException("Error while creating connection to LDAP server", e).details(LDAP.getDetailsFrom(e));
        }
    }

    @Override
    public void close() throws IOException {
        if (pool != null) {
            pool.close();
        }
    }

    public LDAPConnectionPool getPool() {
        return pool;
    }
    
    /**
     * Thin wrapper for ServerSet implementations which executes the getConnection() calls as privileged blocks. This is necessary due to the ES connection policy.
     */
    static class PrivilegedServerSet extends ServerSet {
        private final ServerSet delegate;
        
        public PrivilegedServerSet(ServerSet delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public boolean includesAuthentication() {
            return delegate.includesAuthentication();
        }

        @Override
        public boolean includesPostConnectProcessing() {
            return delegate.includesPostConnectProcessing();
        }

        @Override
        public LDAPConnection getConnection() throws LDAPException {
            return PrivilegedCode.execute(() -> delegate.getConnection(), LDAPException.class);
        }

        @Override
        public LDAPConnection getConnection(LDAPConnectionPoolHealthCheck healthCheck) throws LDAPException {
            return PrivilegedCode.execute(() -> delegate.getConnection(healthCheck), LDAPException.class);
        }

        @Override
        public void toString(StringBuilder buffer) {
            delegate.toString(buffer);
            buffer.append("(wrapped by PrivilegedServerSet)");
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
