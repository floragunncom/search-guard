package com.floragunn.dlic.auth.ldap2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.dlic.auth.ldap.util.ConfigConstants;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

public final class LDAPUserSearcher {
    protected static final Logger log = LogManager.getLogger(LDAPUserSearcher.class);

    private static final int ZERO_PLACEHOLDER = 0;
    private static final String DEFAULT_USERBASE = "";
    private static final String DEFAULT_USERSEARCH_PATTERN = "(sAMAccountName={0})";

    private final Settings settings;
    private final List<Map.Entry<String, Settings>> userBaseSettings;
    private final LDAPConnectionManager lcm;

    LDAPUserSearcher(LDAPConnectionManager lcm, Settings settings) {
        this.lcm = lcm;
        this.settings = settings;
        this.userBaseSettings = getUserBaseSettings(settings);
    }

    static List<Map.Entry<String, Settings>> getUserBaseSettings(Settings settings) {
        Map<String, Settings> userBaseSettingsMap = new HashMap<>(
                settings.getGroups(ConfigConstants.LDAP_AUTHCZ_USERS));

        if (!userBaseSettingsMap.isEmpty()) {
            if (settings.hasValue(ConfigConstants.LDAP_AUTHC_USERBASE)) {
                throw new RuntimeException(
                        "Both old-style and new-style configuration defined for LDAP authentication backend: "
                                + settings);
            }

            return Utils.getOrderedBaseSettings(userBaseSettingsMap);
        } else {
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_BASE,
                    settings.get(ConfigConstants.LDAP_AUTHC_USERBASE, DEFAULT_USERBASE));
            settingsBuilder.put(ConfigConstants.LDAP_AUTHCZ_SEARCH,
                    settings.get(ConfigConstants.LDAP_AUTHC_USERSEARCH, DEFAULT_USERSEARCH_PATTERN));

            return Collections.singletonList(Pair.of("_legacyConfig", settingsBuilder.build()));
        }
    }

    SearchResultEntry exists(LDAPConnection con, String user) throws LDAPException {

        if (settings.getAsBoolean(ConfigConstants.LDAP_FAKE_LOGIN_ENABLED, false)
                || settings.getAsBoolean(ConfigConstants.LDAP_SEARCH_ALL_BASES, false)
                || settings.hasValue(ConfigConstants.LDAP_AUTHC_USERBASE)) {
            return existsSearchingAllBases(con,user);
        } else {
            return existsSearchingUntilFirstHit(con,user);
        }

    }

    private SearchResultEntry existsSearchingUntilFirstHit(LDAPConnection con, String user) throws LDAPException {
        final String username = user;

        for (Map.Entry<String, Settings> entry : userBaseSettings) {
            Settings baseSettings = entry.getValue();
            
            ParametrizedFilter pf = new ParametrizedFilter(baseSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_USERSEARCH_PATTERN));
            pf.setParameter(ZERO_PLACEHOLDER, username);

            List<SearchResultEntry> result = lcm.search(con,
                    baseSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_USERBASE),
                    SearchScope.SUB,
                    pf);

            if (log.isDebugEnabled()) {
                log.debug("Results for LDAP search for " + user + " in base " + entry.getKey() + ":\n" + result);
            }

            if (result != null && result.size() >= 1) {
                return result.get(0);
            }
        }

        return null;
    }

    private SearchResultEntry existsSearchingAllBases(LDAPConnection con, String user) throws LDAPException {
        final String username = user;
        Set<SearchResultEntry> result = new HashSet<>();

        for (Map.Entry<String, Settings> entry : userBaseSettings) {
            Settings baseSettings = entry.getValue();
            
            ParametrizedFilter pf = new ParametrizedFilter(baseSettings.get(ConfigConstants.LDAP_AUTHCZ_SEARCH, DEFAULT_USERSEARCH_PATTERN));
            pf.setParameter(ZERO_PLACEHOLDER, username);

            List<SearchResultEntry> foundEntries = lcm.search(con,
                    baseSettings.get(ConfigConstants.LDAP_AUTHCZ_BASE, DEFAULT_USERBASE),
                    SearchScope.SUB,
                    pf);

            if (log.isDebugEnabled()) {
                log.debug("Results for LDAP search for " + user + " in base " + entry.getKey() + ":\n" + result);
            }

            if (foundEntries != null) {
                result.addAll(foundEntries);
            }
        }

        if (result.isEmpty()) {
            log.debug("No user " + username + " found");
            return null;
        }

        if (result.size() > 1) {
            log.debug("More than one user for '" + username + "' found");
            return null;
        }

        return result.iterator().next();
    }

}
