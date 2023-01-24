/*
  * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.unboundid.ldap.sdk.LDAPBindException;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPSearchException;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SearchScope;
import java.util.Arrays;
import java.util.stream.Collectors;

public class LDAP {
    static SearchScope getSearchScope(String name) throws ConfigValidationException {
        for (SearchScope searchScope : SearchScope.values()) {
            if (searchScope.getName().equalsIgnoreCase(name)) {
                return searchScope;
            }
        }

        throw new ConfigValidationException(new InvalidAttributeValue(null, name,
                Arrays.asList(SearchScope.values()).stream().map((s) -> s.getName()).collect(Collectors.joining("|"))));
    }

    static OrderedImmutableMap<String, Object> getDetailsFrom(LDAPException e) {
        OrderedImmutableMap.Builder<String, Object> result = new OrderedImmutableMap.Builder<>();

        if (e.getResultCode() != null) {
            result.put("ldap_rc", e.getResultCode().toString());
        }

        if (e.getMatchedDN() != null) {
            result.put("matched_dn", e.getMatchedDN());
        }

        if (e.getDiagnosticMessage() != null) {
            result.put("diagnostic_message", e.getDiagnosticMessage());
        }

        if (e.getReferralURLs() != null && e.getReferralURLs().length > 0) {
            result.put("referral_urls", Arrays.asList(e.getReferralURLs()).toString());
        }

        if (e instanceof LDAPBindException && ((LDAPBindException) e).getBindResult() != null) {
            result.put("bind_result", ((LDAPBindException) e).getBindResult().toString());
        }

        if (e instanceof LDAPSearchException) {
            LDAPSearchException searchException = (LDAPSearchException) e;

            result.put("entry_count", searchException.getEntryCount());
            result.put("reference_count", searchException.getReferenceCount());
        }

        return result.build();
    }

    static String getBetterErrorMessage(LDAPException e) {
        ResultCode resultCode = e.getResultCode();

        if (resultCode == null) {
            if (e.getExceptionMessage() != null) {
                return e.getExceptionMessage();
            } else {
                return e.getMessage();
            }
        }

        switch (resultCode.intValue()) {
        case ResultCode.NO_SUCH_OBJECT_INT_VALUE:
            return e.getMessage() + "\nPlease verify that the base_dn setting is correct.";
        default:
            if (e.getExceptionMessage() != null) {
                return e.getExceptionMessage();
            } else {
                return e.getMessage();
            }
        }
    }
}
