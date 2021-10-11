package com.floragunn.searchguard.test.helper.certificate.utils;

import com.google.common.base.Strings;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.RFC4519Style;

import java.util.function.Function;

@FunctionalInterface
public interface DnGenerator extends Function<String, X500Name> {

    DnGenerator rootDn = dn -> createDn(dn, "root");
    DnGenerator nodeDn = dn -> createDn(dn, "node");
    DnGenerator clientDn = dn -> createDn(dn, "client");

    static X500Name createDn(String dn, String role) {
        if (Strings.isNullOrEmpty(dn)) {
            throw new RuntimeException(String.format("No DN specified for %s certificate", role));
        }
        try {
            return new X500Name(RFC4519Style.INSTANCE, dn);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format("Invalid DN specified for %s certificate: %s", role, dn), e);
        }
    }
}
