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

package com.floragunn.searchguard.support;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ConfigHelper {
    
    private static final Logger LOGGER = LogManager.getLogger(ConfigHelper.class);
    
    public static <T> void uploadFile(Client tc, String filepath, String index, CType<T> cType) throws Exception {
        LOGGER.info("Will update '" + cType + "' with " + filepath);
        
        try (Reader reader = new FileReader(filepath)) {

            SgDynamicConfiguration<T> config = SgDynamicConfiguration.from(reader, cType, Format.YAML, null);
            
            String res = tc
                    .index(new IndexRequest(index).id(cType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                            .source(cType.toLCString(),toBytesReference(config))).actionGet().getId();

            if (!cType.toLCString().equals(res)) {
                throw new Exception("   FAIL: Configuration for '" + cType.toLCString()
                        + "' failed for unknown reasons. Pls. consult logfile of elasticsearch");
            }
        } catch (Exception e) {
            throw e;
        }
    }
    
    private static BytesReference toBytesReference(ToXContent toXContent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder);
    }
    
    private static <T> SgDynamicConfiguration<T> fromYamlReader(Reader yamlReader, CType<T> ctype, int version) throws IOException, ConfigValidationException {
        try {
            return SgDynamicConfiguration.from(yamlReader, ctype, Format.YAML, null);
        } finally {
            if(yamlReader != null) {
                yamlReader.close();
            }
        }
    }
    
    public static <T> SgDynamicConfiguration<T> fromYamlFile(String filepath, CType<T> ctype, int version) throws IOException, ConfigValidationException {
        return fromYamlReader(new FileReader(filepath), ctype, version);
    }
    
    public static <T> SgDynamicConfiguration<T> fromYamlString(String yamlString, CType<T> ctype, int version) throws IOException, ConfigValidationException {
        return fromYamlReader(new StringReader(yamlString), ctype, version);
    }

}
