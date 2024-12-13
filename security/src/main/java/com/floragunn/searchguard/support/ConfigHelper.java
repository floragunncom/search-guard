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
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ConfigHelper {
    
    private static final Logger LOGGER = LogManager.getLogger(ConfigHelper.class);
    
    public static void uploadFile(Client tc, String filepath, String index, CType cType, int configVersion) throws Exception {
        LOGGER.info("Will update '" + cType + "' with " + filepath);

        ConfigHelper.fromYamlFile(filepath, cType, configVersion);
        
        try (Reader reader = new FileReader(filepath)) {

            final String res = tc
                    .index(new IndexRequest(index).type(configVersion==1?"sg":"_doc").id(cType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                            .source(cType.toLCString(), readXContent(reader, XContentType.YAML))).actionGet().getId();

            if (!cType.toLCString().equals(res)) {
                throw new Exception("   FAIL: Configuration for '" + cType.toLCString()
                        + "' failed for unknown reasons. Pls. consult logfile of elasticsearch");
            }
        } catch (Exception e) {
            throw e;
        }
    }
    
    public static BytesReference readXContent(final Reader reader, final XContentType xContentType) throws IOException {
        BytesReference retVal;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, reader);
            parser.nextToken();
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.copyCurrentStructure(parser);
            retVal = BytesReference.bytes(builder);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        return retVal;
    }
    
    public static <T> SgDynamicConfiguration<T> fromYamlReader(Reader yamlReader, CType ctype, int version) throws IOException {
        try {
            return SgDynamicConfiguration.fromNode(DefaultObjectMapper.YAML_MAPPER.readTree(yamlReader), ctype, version, 0, 0, 0);
        } finally {
            if(yamlReader != null) {
                yamlReader.close();
            }
        }
    }
    
    public static <T> SgDynamicConfiguration<T> fromYamlFile(String filepath, CType ctype, int version) throws IOException {
        return fromYamlReader(new FileReader(filepath), ctype, version);
    }
    
    public static <T> SgDynamicConfiguration<T> fromYamlString(String yamlString, CType ctype, int version) throws IOException {
        return fromYamlReader(new StringReader(yamlString), ctype, version);
    }

}