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
package com.floragunn.searchguard.test.helper.certificate.utils;

import com.google.common.base.Strings;
import java.util.function.Function;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.RFC4519Style;

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
