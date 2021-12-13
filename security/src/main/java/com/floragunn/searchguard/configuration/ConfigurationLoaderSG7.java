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

package com.floragunn.searchguard.configuration;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.MultiGetResponse.Failure;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;

public class ConfigurationLoaderSG7 {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final Client client;
    private final String searchguardIndex;
    private final ClusterService cs;
    private final Settings settings;
    private final ComponentState componentState;

    ConfigurationLoaderSG7(final Client client, ThreadPool threadPool, final Settings settings, ClusterService cs, ComponentState componentState) {
        super();
        this.client = client;
        this.settings = settings;
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.cs = cs;
        this.componentState = componentState;
        log.debug("Index is: {}", searchguardIndex);
    }

    Map<CType, SgDynamicConfiguration<?>> load(final CType[] events, long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(events.length);
        final Map<CType, SgDynamicConfiguration<?>> rs = new HashMap<>(events.length);
        Map<CType, ComponentState> typeToStateMap = new EnumMap<>(CType.class);
        
        for (CType ctype : events) {
            typeToStateMap.put(ctype, componentState.getOrCreatePart("config", ctype.toLCString()));
        }
        
        loadAsync(events, new ConfigCallback() {

            @Override
            public void success(SgDynamicConfiguration<?> dConf) {
                if (latch.getCount() <= 0) {
                    log.error("Latch already counted down (for {} of {})  (index={})", dConf.getCType().toLCString(), Arrays.toString(events),
                            searchguardIndex);
                }

                rs.put(dConf.getCType(), dConf);
                latch.countDown();
                if (log.isDebugEnabled()) {
                    log.debug("Received config for {} (of {}) with current latch value={}", dConf.getCType().toLCString(), Arrays.toString(events),
                            latch.getCount());
                }
                
                ComponentState configState = typeToStateMap.get(dConf.getCType());
                configState.setInitialized();
                configState.setConfigVersion(dConf.getDocVersion());
            }

            @Override
            public void singleFailure(Failure failure) {
                log.error("Failure {} retrieving configuration for {} (index={})", failure == null ? null : failure.getMessage(),
                        Arrays.toString(events), searchguardIndex);
                
                typeToStateMap.get(CType.fromString(failure.getId())).setFailed(failure.getMessage());
                typeToStateMap.get(CType.fromString(failure.getId())).setDetailJson(Strings.toString(failure));
            }

            @Override
            public void noData(String id, String type) {
                //when index was created with ES 6 there are no separate tenants. So we load just empty ones.
                //when index was created with ES 7 and type not "sg" (ES 6 type) there are no rolemappings anymore.
                
                if (log.isTraceEnabled()) {
                    log.trace("noData(" + id + ", " + type + ")");
                    log.trace("index creation version: " + cs.state().getMetadata().index(searchguardIndex).getCreationVersion());
                }
                
                if (cs.state().getMetadata().index(searchguardIndex).getCreationVersion().before(Version.V_7_0_0) || "sg".equals(type)) {
                    //created with SG 6
                    //skip tenants

                    if (log.isDebugEnabled()) {
                        log.debug("Skip tenants because we not yet migrated to ES 7 (index was created with ES 6 and type is legacy [{}])", type);
                        log.debug("Skip blocks since they were added in v7+ ");
                    }

                    if (CType.fromString(id) == CType.TENANTS || CType.fromString(id) == CType.BLOCKS) {
                        rs.put(CType.fromString(id), SgDynamicConfiguration.empty());
                        latch.countDown();
                        return;
                    }
                }
                if ("blocks".equals(id)) {
                    log.debug("No data for SG_Block found, creating empty SG_Block.");
                    rs.put(CType.BLOCKS, SgDynamicConfiguration.empty());
                    latch.countDown();
                } else {
                    log.error("No data for {} while retrieving configuration for {}  (index={} and type={})", id, Arrays.toString(events), searchguardIndex, type);
                    latch.countDown();
                    typeToStateMap.get(CType.fromString(id)).setFailed("Document not found");
                }
            }

            @Override
            public void failure(Throwable t) {
                log.error("Exception {} while retrieving configuration for {}  (index={})", t, t.toString(), Arrays.toString(events),
                        searchguardIndex);
                componentState.setFailed(t instanceof Exception ? (Exception) t : new Exception(t));
                
                for (ComponentState subState : typeToStateMap.values()) {
                    subState.setFailed(t instanceof Exception ? (Exception) t : new Exception(t));
                }
            }

            @Override
            public void failure(Throwable t, CType ctype) {
                log.error("Exception {} while retrieving configuration for {}  (index={})", t, t.toString(), Arrays.toString(events),
                        searchguardIndex);
                typeToStateMap.get(ctype).setFailed(t instanceof Exception ? (Exception) t : new Exception(t));

            }
        });

        if (!latch.await(timeout, timeUnit)) {
            //timeout
            throw new TimeoutException("Timeout after " + timeout + "" + timeUnit + " while retrieving configuration for " + Arrays.toString(events)
                    + "(index=" + searchguardIndex + ")");
        }

        return rs;
    }

    private void loadAsync(final CType[] events, final ConfigCallback callback) {
        if (events == null || events.length == 0) {
            log.warn("No config events requested to load");
            return;
        }

        final MultiGetRequest mget = new MultiGetRequest();

        for (CType cType : events) {
            final String event = cType.toLCString();
            mget.add(searchguardIndex, event);
        }

        mget.refresh(true);
        mget.realtime(true);
        
        if (log.isTraceEnabled()) {
            log.trace("Issuing " + mget);
        }
        
        client.multiGet(mget, new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse response) {
                if (log.isTraceEnabled()) {
                    log.trace("Response for " + mget.getItems());
                }
                
                MultiGetItemResponse[] responses = response.getResponses();
                for (MultiGetItemResponse singleResponse : responses) {
                    if (singleResponse != null && !singleResponse.isFailed()) {
                        GetResponse singleGetResponse = singleResponse.getResponse();
                        if (singleGetResponse.isExists() && !singleGetResponse.isSourceEmpty()) {
                            //success
                            try {
                                final SgDynamicConfiguration<?> dConf = toConfig(singleGetResponse);
                                if (dConf != null) {
                                    callback.success(dConf.deepClone());
                                } else {
                                    callback.failure(new Exception("Cannot parse settings for " + singleGetResponse.getId()), CType.fromString(singleGetResponse.getId()));
                                }
                            } catch (Exception e) {
                                log.error(e.toString(), e);
                                callback.failure(e, CType.fromString(singleGetResponse.getId()));
                            }
                        } else {
                            //does not exist or empty source
                            callback.noData(singleGetResponse.getId(), singleGetResponse.getType());
                        }
                    } else {
                        //failure
                        callback.singleFailure(singleResponse == null ? null : singleResponse.getFailure());
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (log.isTraceEnabled()) {
                    log.trace("Failure for " + mget + ": " + e);
                }
                callback.failure(e);
            }
        });

    }

    private SgDynamicConfiguration<?> toConfig(GetResponse singleGetResponse) throws Exception {
        final BytesReference ref = singleGetResponse.getSourceAsBytesRef();
        final String id = singleGetResponse.getId();
        final long seqNo = singleGetResponse.getSeqNo();
        final long primaryTerm = singleGetResponse.getPrimaryTerm();
        final long docVersion = singleGetResponse.getVersion();

        if (ref == null || ref.length() == 0) {
            log.error("Empty or null byte reference for {}", id);
            return null;
        }

        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ref,
                XContentType.JSON)) {
            parser.nextToken();
            parser.nextToken();

            if (!id.equals((parser.currentName()))) {
                log.error("Cannot parse config for type {} because {}!={}", id, id, parser.currentName());
                return null;
            }

            parser.nextToken();

            return SgDynamicConfiguration.fromJson(new String(parser.binaryValue()), CType.fromString(id), docVersion, seqNo, primaryTerm, settings);
        }
    }
}
