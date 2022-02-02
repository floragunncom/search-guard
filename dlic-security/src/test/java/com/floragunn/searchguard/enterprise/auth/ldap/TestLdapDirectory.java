/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
 * 
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.floragunn.searchguard.test.helper.cluster.EsClientProvider.UserCredentialsHolder;
import com.google.common.collect.ImmutableList;
import com.unboundid.ldap.sdk.Attribute;

public class TestLdapDirectory {
    public static final Entry ROOT = new Entry("o=TEST").dc("TEST").objectClass("top", "domain");
    public static final Entry PEOPLE = new Entry("ou=people,o=TEST").ou("people").objectClass("top", "organizationalUnit");
    public static final Entry GROUPS = new Entry("ou=groups,o=TEST").ou("groups").objectClass("top", "organizationalUnit");

    public static final ImmutableList<Entry> BASE = ImmutableList.of(ROOT, PEOPLE, GROUPS);

    public static class Entry implements UserCredentialsHolder {
        private final String dn;
        private List<Attribute> attributes = new ArrayList<>();
        private String uid;
        private String password;

        public Entry(String dn) {
            this.dn = dn;
        }

        public String getDn() {
            return dn;
        }

        /**
         * Common name
         */
        public Entry cn(String... cn) {
            attributes.add(new Attribute("cn", cn));
            return this;
        }

        /**
         * Domain component
         */
        public Entry dc(String... dc) {
            attributes.add(new Attribute("dc", dc));
            return this;
        }

        /**
         * Organizational unit
         */
        public Entry ou(String... ou) {
            attributes.add(new Attribute("ou", ou));
            return this;
        }

        /** 
         * Surename
         */
        public Entry sn(String... sn) {
            attributes.add(new Attribute("sn", sn));
            return this;
        }

        public Entry uid(String... uid) {
            attributes.add(new Attribute("uid", uid));
            this.uid = uid[0];
            return this;
        }

        public Entry objectClass(String... objectclass) {
            attributes.add(new Attribute("objectclass", objectclass));
            
            if (Arrays.asList(objectclass).contains("inetOrgPerson")) {
                // Make the schema happy
                sn("Test");
            }
            
            return this;
        }

        public Entry ref(String... ref) {
            attributes.add(new Attribute("ref", ref));
            return this;
        }

        public Entry userpassword(String... userpassword) {
            attributes.add(new Attribute("userpassword", userpassword));
            this.password = userpassword[0];
            return this;
        }

        public Entry uniqueMember(String... uniqueMember) {
            attributes.add(new Attribute("uniquemember", uniqueMember));
            return this;
        }

        public Entry uniqueMember(Entry... uniqueMember) {
            attributes.add(new Attribute("uniquemember", Arrays.asList(uniqueMember).stream().map((m) -> m.getDn()).collect(Collectors.toList())));
            return this;
        }

        public Entry attr(String name, String... values) {
            attributes.add(new Attribute(name, values));
            return this;
        }

        com.unboundid.ldap.sdk.Entry build() {
            return new com.unboundid.ldap.sdk.Entry(dn, attributes.toArray(new Attribute[attributes.size()]));
        }

        @Override
        public String getName() {
            return uid;
        }

        @Override
        public String getPassword() {
            return password;
        }

    }
}
