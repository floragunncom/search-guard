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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import org.apache.http.HttpHost;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

@RunWith(MockitoJUnitRunner.class)
public class HttpProxyConfigTest {

    @Mock
    private HttpProxyHostRegistry httpProxyHostRegistry;

    @Test
    public void createHttpProxyConfig_shouldThrowError_proxyWithGivenIdDoesNotExists_andStrictValidationIsUsed() {
        String proxyId = "1";
        Mockito.when(httpProxyHostRegistry.findHttpProxyHost(proxyId)).thenReturn(Optional.empty());
        ValidatingDocNode config = new ValidatingDocNode(DocNode.of("proxy", proxyId), new ValidationErrors());

        ConfigValidationException exception =
                (ConfigValidationException) assertThatThrown(() ->
                        HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.STRICT),
                        instanceOf(ConfigValidationException.class)
                );

        assertThat(exception.getMessage(), containsString("Http proxy '" + proxyId + "' not found."));
    }

    @Test
    public void createHttpProxyConfig_shouldNotThrowError_proxyWithGivenIdDoesNotExists_andLenientValidationIsUsed() throws ConfigValidationException {
        String proxyId = "1";
        Mockito.when(httpProxyHostRegistry.findHttpProxyHost(proxyId)).thenReturn(Optional.empty());
        ValidatingDocNode config = new ValidatingDocNode(DocNode.of("proxy", proxyId), new ValidationErrors());

        HttpProxyConfig createdConfig = HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.LENIENT);

        assertThat(createdConfig.getProxy(), nullValue());
        assertThat(createdConfig.getType(), equalTo(ProxyTypeProvider.Type.USE_STORED_PROXY));
    }

    @Test
    public void createHttpProxyConfig_proxyConfigIsCreatedUsingGivenProxyId() throws ConfigValidationException {
        String proxyId = "1";
        String proxyUri = "http://1.1.1.1:1";
        Mockito.when(httpProxyHostRegistry.findHttpProxyHost(proxyId)).thenReturn(Optional.of(HttpHost.create(proxyUri)));
        ValidatingDocNode config = new ValidatingDocNode(DocNode.of("proxy", proxyId), new ValidationErrors());

        HttpProxyConfig createdConfig = HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.STRICT);

        assertThat(createdConfig.getProxy().toURI(), equalTo(proxyUri));
        assertThat(createdConfig.getType(), equalTo(ProxyTypeProvider.Type.USE_STORED_PROXY));
    }

    @Test
    public void createHttpProxyConfig_proxyConfigIsCreatedUsingGivenProxyUri() throws ConfigValidationException {
        String proxyUri = "http://1.2.3.4:5";
        ValidatingDocNode config = new ValidatingDocNode(DocNode.of("proxy", proxyUri), new ValidationErrors());

        HttpProxyConfig createdConfig = HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.STRICT);

        assertThat(createdConfig.getProxy().toURI(), equalTo(proxyUri));
        assertThat(createdConfig.getType(), equalTo(ProxyTypeProvider.Type.USE_INLINE_PROXY));
        Mockito.verifyNoInteractions(httpProxyHostRegistry);

        config = new ValidatingDocNode(DocNode.of("proxy", "default"), new ValidationErrors());

        createdConfig = HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.STRICT);

        assertThat(createdConfig.getProxy(), nullValue());
        assertThat(createdConfig.getType(), equalTo(ProxyTypeProvider.Type.USE_DEFAULT_PROXY));
        Mockito.verifyNoInteractions(httpProxyHostRegistry);

        config = new ValidatingDocNode(DocNode.of("proxy", "none"), new ValidationErrors());

        createdConfig = HttpProxyConfig.create(config, httpProxyHostRegistry, ValidationLevel.STRICT);

        assertThat(createdConfig.getProxy(), nullValue());
        assertThat(createdConfig.getType(), equalTo(ProxyTypeProvider.Type.USE_NO_PROXY));
        Mockito.verifyNoInteractions(httpProxyHostRegistry);
    }
}
