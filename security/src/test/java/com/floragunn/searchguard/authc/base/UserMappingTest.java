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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.base.UserMapping;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class UserMappingTest {
    @Test
    public void userName_from() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.from", "oidc_id_token.sub"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();
        AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);

        Assert.assertEquals("sub_value", mappedCredentials.getUsername());
    }

    @Test
    public void userName_invalidPath() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.from", "oidc_id_token.suf"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();

        try {
            AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);
            Assert.fail("Mapping succeeded even though it should have failed: " + mappedCredentials);
        } catch (CredentialsException e) {
            Assert.assertEquals("No user name found", e.getMessage());
        }
    }

    @Test
    public void userName_static() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.static", "static_value"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();
        AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);

        Assert.assertEquals("static_value", mappedCredentials.getUsername());
    }

    @Test
    public void userName_fromBackend() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.from_backend", "ldap_user_entry.nick_name"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();
        AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);
        Assert.assertEquals("n/a", mappedCredentials.getUsername());

        AuthCredentials backendCredentials = mappedCredentials.userMappingAttribute("ldap_user_entry",
                ImmutableMap.of("nick_name", Arrays.asList("cave")));

        User user = userMapping.map(backendCredentials);

        Assert.assertEquals("cave", user.getName());
    }

    @Test
    public void userName_fromBackend_multiValue_fail() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.from_backend", "ldap_user_entry.nick_name"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();
        AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);
        Assert.assertEquals("n/a", mappedCredentials.getUsername());

        AuthCredentials backendCredentials = mappedCredentials.userMappingAttribute("ldap_user_entry",
                ImmutableMap.of("nick_name", Arrays.asList("cave", "man")));

        try {
            User user = userMapping.map(backendCredentials);
            Assert.fail("Mapping succeeded even though it should have failed: " + user);
        } catch (CredentialsException e) {
            Assert.assertEquals("More than one candidate for the user name was found", e.getMessage());
        }
    }

    @Test
    public void userName_fromBackend_multiValue() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("user_name.from_backend", "ldap_user_entry.nick_name[0]"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("oidc_id_token", ImmutableMap.of("sub", "sub_value", "foo", "foo_value")).build();
        AuthCredentials mappedCredentials = userMapping.mapCredentials(baseCredentials);
        Assert.assertEquals("n/a", mappedCredentials.getUsername());

        AuthCredentials backendCredentials = mappedCredentials.userMappingAttribute("ldap_user_entry",
                ImmutableMap.of("nick_name", Arrays.asList("cave", "man")));

        User user = userMapping.map(backendCredentials);

        Assert.assertEquals("cave", user.getName());
    }

    @Test
    public void roles_from_combiled_with_static() throws Exception {
        UserMapping userMapping = UserMapping.parse(DocNode.of("roles.from", "ldap_user_entry.roles", "roles.static", "static_role"), null);
        AuthCredentials baseCredentials = AuthCredentials.forUser("n/a")
                .userMappingAttribute("ldap_user_entry", ImmutableMap.of("roles", Arrays.asList("a", "b", "c"))).build();

        User user = userMapping.map(baseCredentials);

        Assert.assertEquals(ImmutableSet.of("a", "b", "c", "static_role"), user.getRoles());
    }
}
