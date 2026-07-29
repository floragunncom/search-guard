package com.floragunn.signals.accounts;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.SignalsInitializationException;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.support.LuckySisyphos;

public class AccountRegistry {
    private final static Logger log = LogManager.getLogger(AccountRegistry.class);
    private volatile Map<String, Account> accounts = null;
    private final SignalsSettings settings;

    public AccountRegistry(SignalsSettings settings) {
        this.settings = settings;
    }

    public void init(Client client) throws SignalsInitializationException {
        try {
            if (this.accounts == null) {
                this.updateAtomic(client);
            }
        } catch (Exception e) {
            log.error("Error while initializing AccountRegistry", e);
            throw new SignalsInitializationException("Error while initializing AccountRegistry", e);
        }
    }

    public void updateAtomic(Client client) throws IOException {
        ThreadContext threadContext = client.threadPool().getThreadContext();

        User user = threadContext.getTransient(ConfigConstants.SG_USER);
        Object remoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        Object origin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
        final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();


        try (StoredContext ctx = threadContext.stashContext()) {

            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            threadContext.putTransient(ConfigConstants.SG_USER, user);
            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, remoteAddress);
            threadContext.putTransient(ConfigConstants.SG_ORIGIN, origin);

            originalResponseHeaders.entrySet().forEach(
                    h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))
            );

            Map<String, Account> tmp = new HashMap<>();


            SearchResponse searchResponse = LuckySisyphos
                    .tryHard(() -> client.prepareSearch(settings.getStaticSettings().getIndexNames().getAccounts())
                            .setSource(new SearchSourceBuilder()).setSize(10 * 1000).get());

            try {
                for (SearchHit hit : searchResponse.getHits()) {

                    try {
                        ScopedAccountId scopedAccountId = ScopedAccountId.parse(hit.getId());
                        String id = scopedAccountId.id;
                        String accountType = scopedAccountId.type;

                        Account account = Account.parse(accountType, id, hit.getSourceAsString());
                        account.setTenant(scopedAccountId.tenant);
                        tmp.put(hit.getId(), account);
                    } catch (Exception e) {
                        log.error("Error while parsing " + hit, e);
                    }
                }

                accounts = Collections.unmodifiableMap(tmp);
                log.debug("Loaded {} accounts", accounts.size());
            } finally {
                searchResponse.decRef();
            }
        }
    }

    public <T extends Account> T lookupAccount(String id, Class<T> accountClass) throws NoSuchAccountException {
        return lookupAccount(null, id, accountClass);
    }

    public <T extends Account> T lookupAccount(String tenant, String id, Class<T> accountClass) throws NoSuchAccountException {
        if (this.accounts == null) {
            throw new IllegalStateException("AccountRegistry is not intialized yet");
        }

        if (id == null) {
            id = "default";
        }

        Account.Factory<T> accountFactory = Account.factoryRegistry.get(accountClass);

        if (accountFactory == null) {
            throw new NoSuchAccountException("Illegal account class: " + accountClass);
        }

        String scopedId = Account.scopedId(tenant, accountFactory.getType(), id);
        Account result = accounts.get(scopedId);

        if (result == null && tenant != null) {
            result = accounts.get(Account.scopedId(null, accountFactory.getType(), id));
        }

        if (result == null) {
            throw new NoSuchAccountException("Account does not exist: " + scopedId, null);
        }

        if (!accountClass.isAssignableFrom(result.getClass())) {
            throw new NoSuchAccountException(
                    "Account " + id + " is not of type " + accountClass.getSimpleName() + ". Found: " + result.getClass().getSimpleName(), null);
        }

        return accountClass.cast(result);
    }

    public Account lookupAccount(String id, String accountType) throws NoSuchAccountException {
        return lookupAccountExact(null, id, accountType);
    }

    public Account lookupAccountExact(String tenant, String id, String accountType) throws NoSuchAccountException {
        if (this.accounts == null) {
            throw new IllegalStateException("AccountRegistry is not intialized yet");
        }

        if (id == null) {
            id = "default";
        }

        String scopedId = Account.scopedId(tenant, accountType, id);
        Account result = accounts.get(scopedId);

        if (result == null) {
            throw new NoSuchAccountException("Account does not exist: " + scopedId, null);
        }

        return result;
    }

    private static class ScopedAccountId {
        private final String tenant;
        private final String type;
        private final String id;

        private ScopedAccountId(String tenant, String type, String id) {
            this.tenant = tenant;
            this.type = type;
            this.id = id;
        }

        private static ScopedAccountId parse(String scopedId) {
            String[] parts = scopedId.split("/", -1);

            if (parts.length == 2) {
                return new ScopedAccountId(null, parts[0], parts[1]);
            } else if (parts.length == 3) {
                return new ScopedAccountId(parts[0], parts[1], parts[2]);
            } else {
                throw new IllegalArgumentException("Illegal scopedId: " + scopedId);
            }
        }
    }
}
