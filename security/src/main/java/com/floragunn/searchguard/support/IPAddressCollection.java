/*
 * 
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard.support;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressTrie;
import inet.ipaddr.ipv6.IPv6Address;
import inet.ipaddr.ipv6.IPv6AddressTrie;

public class IPAddressCollection implements Document<IPAddressCollection> {
    private static final Logger log = LogManager.getLogger(IPAddressCollection.class);

    private final List<String> source;
    private final IPv4AddressTrie ipv4trie;
    private final IPv6AddressTrie ipv6trie;

    public IPAddressCollection(IPv4AddressTrie ipv4trie, IPv6AddressTrie ipv6trie, List<String> source) {
        this.ipv4trie = ipv4trie.size() > 0 ? ipv4trie : null;
        this.ipv6trie = ipv6trie.size() > 0 ? ipv6trie : null;
        this.source = source;
    }

    public boolean contains(IPAddress ipAddress) {
        if (ipv4trie != null) {
            IPv4Address ipv4Address = ipAddress.toIPv4();

            if (ipv4Address != null && ipv4trie.elementsContaining(ipv4Address) != null) {
                return true;
            }
        }

        if (ipv6trie != null) {
            IPv6Address ipv6Address = ipAddress.toIPv6();

            if (ipv6Address != null && ipv6trie.elementsContaining(ipv6Address) != null) {
                return true;
            }
        }

        return false;
    }

    public static IPAddressCollection parse(DocNode docNode) throws ConfigValidationException {
        if (docNode.isNull()) {
            return null;
        } else if (docNode.isString()) {
            return parse(Collections.singletonList(docNode.toString()));
        } else if (docNode.isList()) {
            return parse(docNode.toListOfStrings());
        } else {
            throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A list of IP addresses or netmasks in CIDR notation"));
        }
    }

    public static IPAddressCollection parse(List<String> ipStringList) throws ConfigValidationException {
        IPv4AddressTrie ipv4trie = new IPv4AddressTrie();
        IPv6AddressTrie ipv6trie = new IPv6AddressTrie();
        ValidationErrors validationErrors = new ValidationErrors();

        int i = 0;

        for (String string : ipStringList) {
            try {
                IPAddressString ipAddressString = new IPAddressString(string);

                ipAddressString.validate();

                IPAddress ipAddress = ipAddressString.getAddress();

                if (ipAddress instanceof IPv4Address) {
                    ipv4trie.add((IPv4Address) ipAddress);
                } else if (ipAddress instanceof IPv6Address) {
                    ipv6trie.add((IPv6Address) ipAddress);
                }

            } catch (AddressStringException e) {
                validationErrors.add(new InvalidAttributeValue(String.valueOf(i), string, "IP address or netmask in CIDR notation").cause(e));
                log.info("Configuration error; invalid ip address:" + string, e);
            }

            i++;
        }

        validationErrors.throwExceptionForPresentErrors();

        return new IPAddressCollection(ipv4trie, ipv6trie, ipStringList);
    }

    @Override
    public String toString() {
        if (ipv4trie == null && ipv6trie == null) {
            return "Empty IPAddressCollection";
        } else if (ipv4trie == null) {
            return ipv6trie.toString();
        } else if (ipv6trie == null) {
            return ipv4trie.toString();
        } else {
            return "IPAddressCollection [ipv4trie=" + ipv4trie + ", ipv6trie=" + ipv6trie + "]";
        }

    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public List<String> getSource() {
        return source;
    }

}
