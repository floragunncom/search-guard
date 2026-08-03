/*
 * Copyright 2026 floragunn GmbH
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
 */
package com.floragunn.searchguard.user;

import org.junit.Assert;
import org.junit.Test;

public class AuthCredentialsTest {

    @Test
    public void shouldUseStringNativeCredentialsForEqualityAfterSecretsAreCleared() {
        AuthCredentials first = AuthCredentials.forUser("user").nativeCredentials("token").build();
        AuthCredentials second = AuthCredentials.forUser("user").nativeCredentials("token").build();

        first.clearSecrets();
        second.clearSecrets();

        Assert.assertEquals(first, second);
        Assert.assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void shouldDistinguishDifferentStringNativeCredentials() {
        AuthCredentials first = AuthCredentials.forUser("user").nativeCredentials("token-1").build();
        AuthCredentials second = AuthCredentials.forUser("user").nativeCredentials("token-2").build();

        Assert.assertNotEquals(first, second);
    }
}
