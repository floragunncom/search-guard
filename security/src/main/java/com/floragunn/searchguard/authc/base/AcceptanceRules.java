/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.base;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.support.IPAddressCollection;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.AuthCredentials;

import inet.ipaddr.IPAddress;

public class AcceptanceRules {

    private final Criteria accept;
    private final Criteria skip;

    public AcceptanceRules(AcceptanceRules.Criteria accept, AcceptanceRules.Criteria skip) {
        this.accept = accept;
        this.skip = skip;
    }

    public boolean accept(RequestMetaData<?> requestMetaData) {
        IPAddress directIpAddress = requestMetaData.getDirectIpAddress();

        if (accept != null && accept.directIps != null) {
            if (!accept.directIps.contains(directIpAddress)) {
                return false;
            }
        }

        if (skip != null && skip.directIps != null) {
            if (skip.directIps.contains(directIpAddress)) {
                return false;
            }
        }

        IPAddress originatingIpAddress = requestMetaData.getOriginatingIpAddress();

        if (accept != null && accept.originatingIps != null) {
            if (!accept.originatingIps.contains(originatingIpAddress)) {
                return false;
            }
        }

        if (skip != null && skip.originatingIps != null) {
            if (skip.originatingIps.contains(originatingIpAddress)) {
                return false;
            }
        }

        if (accept != null && accept.trustedIps) {
            if (!requestMetaData.isTrustedProxy()) {
                return false;
            }
        }

        String dn = requestMetaData.getClientCertSubject();

        if (dn == null && ((accept != null && accept.clientCerts != null) || (skip != null && skip.clientCerts != null))) {
            return false;
        }

        if (accept != null && accept.clientCerts != null) {
            if (!accept.clientCerts.matches(dn)) {
                return false;
            }
        }

        if (skip != null && skip.clientCerts != null) {
            if (skip.clientCerts.matches(dn)) {
                return false;
            }
        }

        return true;
    }

    public boolean accept(AuthCredentials authCredentials) {
        if (accept != null && accept.users != null) {
            if (!accept.users.matches(authCredentials.getName())) {
                return false;
            }
        }

        if (skip != null && skip.users != null) {
            if (skip.users.matches(authCredentials.getName())) {
                return false;
            }
        }

        return true;
    }

    public static class Criteria {
        private final IPAddressCollection originatingIps;
        private final IPAddressCollection directIps;
        private final Pattern users;
        private final Pattern clientCerts;
        private final boolean trustedIps;

        public Criteria(IPAddressCollection originatingIps, IPAddressCollection directIps, boolean trustedIps, Pattern users, Pattern clientCerts) {
            this.originatingIps = originatingIps;
            this.directIps = directIps;
            this.users = users;
            this.clientCerts = clientCerts;
            this.trustedIps = trustedIps;
        }

        public static Criteria parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
            IPAddressCollection originatingIps = vNode.get("originating_ips").by(IPAddressCollection::parse);
            IPAddressCollection directIps = vNode.get("ips").by(IPAddressCollection::parse);
            boolean trustedIps = vNode.get("trusted_ips").withDefault(false).asBoolean();

            Pattern users = vNode.get("users").by(Pattern::parse);
            Pattern clientCerts = vNode.get("client_certs").by(Pattern::parse);

            validationErrors.throwExceptionForPresentErrors();

            return new Criteria(originatingIps, directIps, trustedIps, users, clientCerts);
        }

        public Pattern getUsers() {
            return users;
        }

        public IPAddressCollection getOriginatingIps() {
            return originatingIps;
        }

        public IPAddressCollection getDirectIps() {
            return directIps;
        }

        public Pattern getClientCerts() {
            return clientCerts;
        }

        public boolean isTrustedIps() {
            return trustedIps;
        }
    }
}
