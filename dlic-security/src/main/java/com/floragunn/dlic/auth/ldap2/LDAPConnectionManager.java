package com.floragunn.dlic.auth.ldap2;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfig;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator.SSLConfigException;
import com.google.common.primitives.Ints;
import com.unboundid.ldap.sdk.AggregateLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.DereferencePolicy;
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
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.RoundRobinServerSet;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.ServerSet;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.StartTLSPostConnectProcessor;

public final class LDAPConnectionManager implements Closeable{

    private static final Logger log = LogManager.getLogger(LDAPConnectionManager.class);
    private final LDAPConnectionPool pool;
    private final SSLConfig sslConfig;
    private final LDAPUserSearcher userSearcher;
    private final Settings settings;

    
    public LDAPConnectionManager(Settings settings, Path configPath) throws LDAPException, SSLConfigException {
        
        this.sslConfig = new SettingsBasedSSLConfigurator(settings, configPath, "").buildSSLConfig();
        this.settings = settings;

        List<String> ldapStrings = this.settings.getAsList(ConfigConstants.LDAP_HOSTS,
                Collections.singletonList("localhost"));
        
        String bindDn = settings.get(ConfigConstants.LDAP_BIND_DN, null);
        String password = settings.get(ConfigConstants.LDAP_PASSWORD, null);

        if (password != null && password.length() == 0) {
            password = null;
        }
        
        final BindRequest bindRequest;
        if (bindDn != null && password != null && password.length() > 0) {
            bindRequest = new SimpleBindRequest(bindDn, password);
        } else if (sslConfig != null && sslConfig.isClientCertAuthenticationEnabled()) {
            bindRequest = new EXTERNALBindRequest();
        } else {
            bindRequest = new SimpleBindRequest();
        }
        
        LDAPConnectionOptions opts = new LDAPConnectionOptions();
        
        int connectTimeout = settings.getAsInt(ConfigConstants.LDAP_CONNECT_TIMEOUT, opts.getConnectTimeoutMillis()); // 0 means wait infinitely
        long responseTimeout = settings.getAsLong(ConfigConstants.LDAP_RESPONSE_TIMEOUT, opts.getResponseTimeoutMillis()); // 0 means wait infinitely
        
        opts.setConnectTimeoutMillis(connectTimeout);
        opts.setResponseTimeoutMillis(responseTimeout);
        opts.setFollowReferrals(true);
        
        final int poolMinSize = this.settings.getAsInt(ConfigConstants.LDAP_POOL_MIN_SIZE, 3);
        final int poolMaxSize = this.settings.getAsInt(ConfigConstants.LDAP_POOL_MAX_SIZE, 10);
        boolean createIfNecessary;
        long maxWaitTimeMillis; //0L is the default which means no blocking at all
        
        if(this.settings.getAsBoolean("pool.enabled", false)) {
            log.warn("LDAP connection pool can no longer be disabled");
        }
        
        if ("blocking".equals(this.settings.get(ConfigConstants.LDAP_POOL_TYPE))){
            //pool enabled in blocking mode
            maxWaitTimeMillis = Long.MAX_VALUE;
            createIfNecessary = false;
        } else {
          //pool enabled in non blocking mode
            maxWaitTimeMillis = 0L;
            createIfNecessary = true;
        }

        pool = new LDAPConnectionPool(createServerSet(ldapStrings, opts), bindRequest, poolMinSize, poolMaxSize);
        pool.setCreateIfNecessary(createIfNecessary);
        pool.setMaxWaitTimeMillis(maxWaitTimeMillis);
        
        configureHealthChecks(poolMaxSize);
        
        userSearcher = new LDAPUserSearcher(this, settings);
    }
    
    private void configureHealthChecks(int poolMaxSize) {
        if (this.settings.getAsBoolean("pool.health_check.enabled", false)) {

            final List<LDAPConnectionPoolHealthCheck> healthChecks = new ArrayList<>();

            if (this.settings.getAsBoolean("pool.health_check.validation.enabled", true)) {

                final String entryDN = this.settings.get("pool.health_check.validation.dn", null); //null means root dse
                final long maxResponseTime = this.settings.getAsLong("pool.health_check.validation.max_response_time", 0L); //means default of 30 sec
                final boolean invokeOnCreate = this.settings.getAsBoolean("pool.health_check.validation.on_create", false);
                final boolean invokeAfterAuthentication = this.settings.getAsBoolean("pool.health_check.validation.after_authentication", false);
                final boolean invokeOnCheckout = this.settings.getAsBoolean("pool.health_check.validation.on_checkout", false);
                final boolean invokeOnRelease = this.settings.getAsBoolean("pool.health_check.validation.on_release", false);
                final boolean invokeForBackgroundChecks = this.settings.getAsBoolean("pool.health_check.validation.for_background_checks", true);
                final boolean invokeOnException = this.settings.getAsBoolean("pool.health_check.validation.on_exception", false);

                final GetEntryLDAPConnectionPoolHealthCheck gehc = new GetEntryLDAPConnectionPoolHealthCheck(entryDN, maxResponseTime, invokeOnCreate,
                        invokeAfterAuthentication, invokeOnCheckout, invokeOnRelease, invokeForBackgroundChecks, invokeOnException);
                healthChecks.add(gehc);

            }

            if (this.settings.getAsBoolean("pool.health_check.pruning.enabled", false)) {

                final int minAvailableConnections = this.settings.getAsInt("pool.health_check.pruning.min_available_connections", poolMaxSize);
                final long minDurationMillisExceedingMinAvailableConnections = this.settings
                        .getAsLong("pool.health_check.pruning.min_duration_millis_exceeding_min_available_connections", 0L);

                final PruneUnneededConnectionsLDAPConnectionPoolHealthCheck puchc = new PruneUnneededConnectionsLDAPConnectionPoolHealthCheck(
                        minAvailableConnections, minDurationMillisExceedingMinAvailableConnections);
                healthChecks.add(puchc);

            }

            if (healthChecks.size() == 1) {
                pool.setHealthCheck(healthChecks.get(0));
            } else if (healthChecks.size() > 1) {
                AggregateLDAPConnectionPoolHealthCheck aghc = new AggregateLDAPConnectionPoolHealthCheck(healthChecks);
                pool.setHealthCheck(aghc);
            }

            pool.setHealthCheckIntervalMillis(this.settings.getAsLong("pool.health_check.interval_millis", pool.getHealthCheckIntervalMillis()));
        }

    }

    private ServerSet createServerSet(final Collection<String> ldapStrings, LDAPConnectionOptions opts) throws LDAPException {
        final List<String> ldapHosts = new ArrayList<>();
        final List<Integer> ldapPorts = new ArrayList<>();
        
        if(ldapStrings == null || ldapStrings.isEmpty()) {
            ldapHosts.add("localhost");
            ldapPorts.add(this.sslConfig != null?636:389);
        } else {
            for(String ldapString:ldapStrings) {
                
                if(ldapString == null || (ldapString = ldapString.trim()).isEmpty()) {
                    continue;
                }
                
                int port = this.sslConfig != null ? 636:389;
                
                if(ldapString.startsWith("ldap://")) {
                    ldapString = ldapString.replace("ldap://", "");
                    //log err
                }
                
                if(ldapString.startsWith("ldaps://")) {
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
        }

        if(sslConfig != null && !sslConfig.isStartTlsEnabled()) {
            final SSLSocketFactory sf = sslConfig.getRestrictedSSLSocketFactory();
            return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), sf, opts, null, null);
        }
        
        if(sslConfig != null && sslConfig.isStartTlsEnabled()) {
            final SSLSocketFactory sf = sslConfig.getRestrictedSSLSocketFactory();
            return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), null, opts, null, new StartTLSPostConnectProcessor(sf));
        }
        
        return newServerSetImpl(ldapHosts.toArray(new String[0]), Ints.toArray(ldapPorts), null, opts, null, null);
    }
    
    private ServerSet newServerSetImpl(final String[] addresses, final int[] ports, final SocketFactory socketFactory,
            final LDAPConnectionOptions connectionOptions, final BindRequest bindRequest, final PostConnectProcessor postConnectProcessor) throws LDAPException {

        final String impl = settings.get(ConfigConstants.LDAP_CONNECTION_STRATEGY, "roundrobin").toLowerCase();

        if ("fewest".equals(impl)) {
            return new FewestConnectionsServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor);
        }

        if ("failover".equals(impl)) {
            return new FailoverServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor);
        }

        if ("fastest".equals(impl)) {
            return new FastestConnectServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor);
        }

        if ("roundrobin".equals(impl)) {
            return new RoundRobinServerSet(addresses, ports, socketFactory, connectionOptions, bindRequest, postConnectProcessor);
        }
        
        throw new LDAPException(ResultCode.NOT_SUPPORTED, ConfigConstants.LDAP_CONNECTION_STRATEGY+": "+impl+" not supported");
    }
    
    public LDAPConnection getConnection() throws LDAPException {
        return pool.getConnection();
    }
    
    public void checkDnPassword(String dn, String password) throws LDAPException {
        pool.bindAndRevertAuthentication(new SimpleBindRequest(dn, password));
    }
    
    public void checkDnPassword(String dn, byte[] password) throws LDAPException {
        pool.bindAndRevertAuthentication(new SimpleBindRequest(dn, password));
    }
    
    public List<SearchResultEntry> search(LDAPConnection con, final String baseDN, final SearchScope scope,
            final ParametrizedFilter filter) throws LDAPException {
            SearchRequest sr = new SearchRequest(baseDN, scope, filter.toString(), SearchRequest.ALL_OPERATIONAL_ATTRIBUTES, SearchRequest.ALL_USER_ATTRIBUTES);
            sr.setDerefPolicy(DereferencePolicy.ALWAYS);
            SearchResult searchResult = con.search(sr);
            return searchResult.getSearchEntries();
    }
    
    public SearchResultEntry lookup(LDAPConnection con, final String dn) throws LDAPException {
        return con.getEntry(dn, SearchRequest.ALL_OPERATIONAL_ATTRIBUTES, SearchRequest.ALL_USER_ATTRIBUTES);
    }
    
    public SearchResultEntry exists(LDAPConnection con, String name) throws LDAPException {
        return userSearcher.exists(con, name);
    }

    @Override
    public void close() throws IOException {
        if(pool != null) {
            pool.close();
        }
    }
}
