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

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Metadata;
import com.floragunn.codova.documents.Metadata.Attribute;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.support.IPAddressCollection;
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

    public static class Criteria implements Document<Criteria> {
        public static final Metadata<Criteria> META = Metadata.create(Criteria.class, "Acceptance rules criteria", Criteria::parse, //
                Attribute.list("originating_ips", String.class,
                        "Matches the actual IP address of the client where request originates from. You can specify CIDR expressions like 10.10.10.0/24"),
                Attribute.list("ips", String.class,
                        "Matches the direct IP address of the host connecting to the node. You can specify CIDR expressions like 10.10.10.0/24"),
                Attribute.optional("trusted_ips", Boolean.class, "Matches only trusted IPs according to network.trusted_proxies").defaultValue(false),
                Attribute.list("users", String.class, "Matches the user names"),
                Attribute.list("client_certs", String.class, "Matches the DNs of client certificates"));

        private final DocNode source;
        private final IPAddressCollection originatingIps;
        private final IPAddressCollection directIps;
        private final Pattern users;
        private final Pattern clientCerts;
        private final boolean trustedIps;

        public Criteria(DocNode source, IPAddressCollection originatingIps, IPAddressCollection directIps, boolean trustedIps, Pattern users,
                Pattern clientCerts) {
            this.source = source;
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

            return new Criteria(docNode, originatingIps, directIps, trustedIps, users, clientCerts);
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

        @Override
        public Object toBasicObject() {
            return source;
        }
    }
}
