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

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.Strings;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ConfigTests {
    
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    
    @Test
    public void testEmptyConfig() throws Exception {
        Assert.assertTrue(SgDynamicConfiguration.empty().deepClone() != SgDynamicConfiguration.empty());
    }
    
    @Test
    public void testParseSg67Config() throws Exception {

        check("./legacy/sgconfig_v6/sg_action_groups.yml", CType.ACTIONGROUPS);
        check("./sgconfig/sg_action_groups.yml", CType.ACTIONGROUPS);
        
        check("./legacy/sgconfig_v6/sg_config.yml", CType.CONFIG);
        check("./sgconfig/sg_config.yml", CType.CONFIG);
        
        check("./legacy/sgconfig_v6/sg_roles.yml", CType.ROLES);
        check("./sgconfig/sg_roles.yml", CType.ROLES);
        
        check("./legacy/sgconfig_v6/sg_internal_users.yml", CType.INTERNALUSERS);
        check("./sgconfig/sg_internal_users.yml", CType.INTERNALUSERS);
        
        check("./legacy/sgconfig_v6/sg_roles_mapping.yml", CType.ROLESMAPPING);
        check("./sgconfig/sg_roles_mapping.yml", CType.ROLESMAPPING);
        
        check("./sgconfig/sg_tenants.yml", CType.TENANTS);

        check("./sgconfig/sg_blocks.yml", CType.BLOCKS);
    }

    private void check(String file, CType cType) throws Exception {
        JsonNode jsonNode = YAML.readTree(FileUtils.readFileToString(new File(file), "UTF-8"));
        int configVersion = 1;
        
        if(jsonNode.get("_sg_meta") != null) {
            Assert.assertEquals(jsonNode.get("_sg_meta").get("type").asText(), cType.toLCString());
            configVersion = jsonNode.get("_sg_meta").get("config_version").asInt();
        }
        
        SgDynamicConfiguration<?> dc = load(file, cType);
        Assert.assertNotNull(dc);
        String jsonSerialize = DefaultObjectMapper.objectMapper.writeValueAsString(dc);
        SgDynamicConfiguration<?> conf = SgDynamicConfiguration.fromJson(jsonSerialize, null, cType, configVersion, 0, 0, 0);
        SgDynamicConfiguration.fromJson(Strings.toString(conf), null, cType, configVersion, 0, 0, 0);
    }
    
    private SgDynamicConfiguration<?> load(String file, CType cType) throws Exception {
        JsonNode jsonNode = YAML.readTree(FileUtils.readFileToString(new File(file), "UTF-8"));
        int configVersion = 1;
        
        if(jsonNode.get("_sg_meta") != null) {
            Assert.assertEquals(jsonNode.get("_sg_meta").get("type").asText(), cType.toLCString());
            configVersion = jsonNode.get("_sg_meta").get("config_version").asInt();
        }
        
        return SgDynamicConfiguration.fromNode(jsonNode, cType, configVersion, 0l, 0l, 0l);
    }
}
