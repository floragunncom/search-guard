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

package com.floragunn.searchguard;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.tools.SearchGuardAdmin;

public class SgAdminInvalidConfigsTests extends SingleClusterTest {
    
    @Test
    public void testSgAdminDuplicateKey() throws Exception {
        setup();
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        List<String> argsAsList = new ArrayList<>();
        argsAsList.add("-ts");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore.jks").toFile().getAbsolutePath());
        argsAsList.add("-ks");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"kirk-keystore.jks").toFile().getAbsolutePath());
        argsAsList.add("-p");
        argsAsList.add(String.valueOf(clusterInfo.nodePort));
        argsAsList.add("-cn");
        argsAsList.add(clusterInfo.clustername);
        argsAsList.add("-cd");
        argsAsList.add(new File("./src/test/resources/invalid_dupkey").getAbsolutePath());
        argsAsList.add("-nhnv");
        
        
        int returnCode  = SearchGuardAdmin.execute(argsAsList.toArray(new String[0]));
        Assert.assertNotEquals(0, returnCode);
        
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_OK, (rh.executeGetRequest("_searchguard/health?pretty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }
    
    @Test
    public void testSgAdminDuplicateKeyReload() throws Exception {
        testSgAdminDuplicateKey();
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        List<String> argsAsList = new ArrayList<>();
        argsAsList.add("-ts");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore.jks").toFile().getAbsolutePath());
        argsAsList.add("-ks");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"kirk-keystore.jks").toFile().getAbsolutePath());
        argsAsList.add("-p");
        argsAsList.add(String.valueOf(clusterInfo.nodePort));
        argsAsList.add("-cn");
        argsAsList.add(clusterInfo.clustername);
        argsAsList.add("-rl");
        argsAsList.add("-nhnv");
        
        
        int returnCode  = SearchGuardAdmin.execute(argsAsList.toArray(new String[0]));
        Assert.assertEquals(0, returnCode);
        
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_OK, (rh.executeGetRequest("_searchguard/health?pretty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }
    
    @Test
    public void testSgAdminDuplicateKeySingleFile() throws Exception {
        setup();
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        List<String> argsAsList = new ArrayList<>();
        argsAsList.add("-ts");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore.jks").toFile().getAbsolutePath());
        argsAsList.add("-ks");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"kirk-keystore.jks").toFile().getAbsolutePath());
        argsAsList.add("-p");
        argsAsList.add(String.valueOf(clusterInfo.nodePort));
        argsAsList.add("-cn");
        argsAsList.add(clusterInfo.clustername);
        argsAsList.add("-f");
        argsAsList.add(new File("./src/test/resources/invalid_dupkey/sg_roles_mapping.yml").getAbsolutePath());
        argsAsList.add("-t");
        argsAsList.add("rolesmapping");
        argsAsList.add("-nhnv");
        
        
        int returnCode  = SearchGuardAdmin.execute(argsAsList.toArray(new String[0]));
        Assert.assertNotEquals(0, returnCode);
        
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_OK, (rh.executeGetRequest("_searchguard/health?pretty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }
    
    @Test
    public void testSgAdminDuplicateKeyReloadSingleFile() throws Exception {
        testSgAdminDuplicateKeySingleFile();
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        List<String> argsAsList = new ArrayList<>();
        argsAsList.add("-ts");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore.jks").toFile().getAbsolutePath());
        argsAsList.add("-ks");
        argsAsList.add(FileHelper.getAbsoluteFilePathFromClassPath(prefix+"kirk-keystore.jks").toFile().getAbsolutePath());
        argsAsList.add("-p");
        argsAsList.add(String.valueOf(clusterInfo.nodePort));
        argsAsList.add("-cn");
        argsAsList.add(clusterInfo.clustername);
        argsAsList.add("-rl");
        argsAsList.add("-nhnv");
        
        
        int returnCode  = SearchGuardAdmin.execute(argsAsList.toArray(new String[0]));
        Assert.assertEquals(0, returnCode);
        
        RestHelper rh = nonSslRestHelper();
        
        Assert.assertEquals(HttpStatus.SC_OK, (rh.executeGetRequest("_searchguard/health?pretty")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }
}
