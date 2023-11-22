/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.watch.common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ProxyTypeProviderTest {

    private final String proxyValue;
    private final ProxyTypeProvider.Type expectedType;

    public ProxyTypeProviderTest(String proxyValue, ProxyTypeProvider.Type expectedType) {
        this.proxyValue = proxyValue;
        this.expectedType = expectedType;
    }

    @Parameters
    public static Collection<Object[]> testParameters() {
        return Arrays.asList(new Object[][] {
                { null, ProxyTypeProvider.Type.USE_DEFAULT_PROXY },
                { "", ProxyTypeProvider.Type.USE_DEFAULT_PROXY },
                { "default", ProxyTypeProvider.Type.USE_DEFAULT_PROXY },
                {"dEfAUlT", ProxyTypeProvider.Type.USE_DEFAULT_PROXY },
                { "none", ProxyTypeProvider.Type.USE_NO_PROXY },
                { "NoNe", ProxyTypeProvider.Type.USE_NO_PROXY },
                { "http://127.0.0.1", ProxyTypeProvider.Type.USE_INLINE_PROXY },
                { "hTTp://127.0.0.1", ProxyTypeProvider.Type.USE_INLINE_PROXY },
                { "https://127.0.0.1", ProxyTypeProvider.Type.USE_INLINE_PROXY },
                { "hTTpS://127.0.0.1", ProxyTypeProvider.Type.USE_INLINE_PROXY },
                { "id", ProxyTypeProvider.Type.USE_STORED_PROXY },
                { "adefault", ProxyTypeProvider.Type.USE_STORED_PROXY }
        });
    }

    @Test
    public void shouldDetermineProxyTypeBasedOnProxyValue() {
        ProxyTypeProvider.Type determinedType = ProxyTypeProvider.determineTypeBasedOnValue(proxyValue);

        assertThat(determinedType, equalTo(expectedType));
    }
}
