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

import inet.ipaddr.IPAddress;

public class IPAddressAcceptanceRules {
    
    public static final IPAddressAcceptanceRules ANY = new IPAddressAcceptanceRules(null, null);

    private final Criteria accept;
    private final Criteria skip;

    public IPAddressAcceptanceRules(IPAddressAcceptanceRules.Criteria accept, IPAddressAcceptanceRules.Criteria skip) {
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

        return true;
    }

    public static class Criteria {
        private final IPAddressCollection originatingIps;
        private final IPAddressCollection directIps;
        private final boolean trustedIps;

        public Criteria(IPAddressCollection originatingIps, IPAddressCollection directIps, boolean trustedIps) {
            this.originatingIps = originatingIps;
            this.directIps = directIps;
            this.trustedIps = trustedIps;
        }

        public static Criteria parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);
            IPAddressCollection originatingIps = vNode.get("originating_ips").by(IPAddressCollection::parse);
            IPAddressCollection directIps = vNode.get("ips").by(IPAddressCollection::parse);
            boolean trustedIps = vNode.get("trusted_ips").withDefault(false).asBoolean();

            validationErrors.throwExceptionForPresentErrors();

            return new Criteria(originatingIps, directIps, trustedIps);
        }

        public IPAddressCollection getOriginatingIps() {
            return originatingIps;
        }

        public IPAddressCollection getDirectIps() {
            return directIps;
        }

        public boolean isTrustedIps() {
            return trustedIps;
        }
    }
}
