/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.sgconf;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class StaticResourcesTests extends SingleClusterTest {
    
    @Before
    public void reset() {
        DynamicConfigFactory.resetStatics();
    }
    
    @Test
    public void testStaticResources() throws Exception {
        setup();
        
        RestHelper rh = nonSslRestHelper();
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("_search", encodeBasicHeader("static_role_user", "nagilum"))).getStatusCode());
    }
    
    @Test
    public void testNoStaticResources() throws Exception {
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_UNSUPPORTED_LOAD_STATIC_RESOURCES, false).build();
        setup(settings);
        
        RestHelper rh = nonSslRestHelper();
        HttpResponse res;
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, (res = rh.executeGetRequest("_search", encodeBasicHeader("static_role_user", "nagilum"))).getStatusCode());
    }
}
