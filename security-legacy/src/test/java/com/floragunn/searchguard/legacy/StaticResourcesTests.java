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

package com.floragunn.searchguard.legacy;

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class StaticResourcesTests extends SingleClusterTest {
  
    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Test
    public void testStaticResources() throws Exception {
        setup();
        
        RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, (rh.executeGetRequest("_search", encodeBasicHeader("static_role_user", "nagilum"))).getStatusCode());
    }
}
