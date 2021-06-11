package com.floragunn.searchguard.support;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressTrie;
import inet.ipaddr.ipv6.IPv6Address;
import inet.ipaddr.ipv6.IPv6AddressTrie;

public class IPAddressCollection {
    private static final Logger log = LogManager.getLogger(IPAddressCollection.class);

    private IPv4AddressTrie ipv4trie;
    private IPv6AddressTrie ipv6trie;

    IPAddressCollection(IPv4AddressTrie ipv4trie, IPv6AddressTrie ipv6trie) {
        this.ipv4trie = ipv4trie.size() > 0 ? ipv4trie : null;
        this.ipv6trie = ipv6trie.size() > 0 ? ipv6trie : null;
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

    public static IPAddressCollection create(List<String> ipStringList) {
        IPv4AddressTrie ipv4trie = new IPv4AddressTrie();
        IPv6AddressTrie ipv6trie = new IPv6AddressTrie();

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
                log.error("Configuration error; invalid ip address:" + string, e);
            }
        }

        return new IPAddressCollection(ipv4trie, ipv6trie);
    }

    @Override
    public String toString() {
        if (ipv4trie == null  && ipv6trie == null) {
            return "Empty IPAddressCollection";
        } else if (ipv4trie == null) {
            return ipv6trie.toString();
        } else if (ipv6trie == null) {
            return ipv4trie.toString();
        } else {
            return "IPAddressCollection [ipv4trie=" + ipv4trie + ", ipv6trie=" + ipv6trie + "]";
        }
        
    }

}
