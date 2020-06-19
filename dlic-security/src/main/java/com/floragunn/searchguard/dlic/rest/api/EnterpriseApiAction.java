/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.dlic.rest.api;

import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.rest.action.AbstractApiAction;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public abstract class EnterpriseApiAction extends AbstractApiAction {

    protected EnterpriseApiAction(Settings settings, Path configPath, AdminDNs adminDNs, ConfigurationRepository cl, ClusterService cs, PrincipalExtractor principalExtractor, PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog, AdminDNs adminDns, ThreadContext threadContext) {
        super(settings, configPath, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
    }

    @Override
    protected void handleDelete(final RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        final String name = request.param("name");

        if (name == null || name.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        final SgDynamicConfiguration<?> existingConfiguration = load(getConfigName(), false);

        if (isHidden(existingConfiguration, name)) {
            notFound(channel, getResourceName() + " " + name + " not found.");
            return;
        }

        if (isReserved(existingConfiguration, name)) {
            forbidden(channel, "Resource '"+ name +"' is read-only.");
            return;
        }

        boolean existed = existingConfiguration.exists(name);
        existingConfiguration.remove(name);

        if (existed) {
            saveAnUpdateConfigs(client, getConfigName(), existingConfiguration, new OnSucessActionListener<IndexResponse>(channel) {

                @Override
                public void onResponse(IndexResponse response) {
                    successResponse(channel, "'" + name + "' deleted.");
                }
            });

        } else {
            notFound(channel, getResourceName() + " " + name + " not found.");
        }
    }

    @Override
    protected void handlePutWithName(final RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        final String name = request.param("name");

        if (name == null || name.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        final SgDynamicConfiguration<?> existingConfiguration = load(getConfigName(), false);

        if (isHidden(existingConfiguration, name)) {
            forbidden(channel, "Resource '" + name + "' is not available.");
            return;
        }

        if (isReserved(existingConfiguration, name)) {
            forbidden(channel, "Resource '" + name + "' is read-only.");
            return;
        }

        if (log.isTraceEnabled() && content != null) {
            log.trace(content.toString());
        }

        boolean existed = existingConfiguration.exists(name);
        existingConfiguration.putCObject(name, DefaultObjectMapper.readTree(content, existingConfiguration.getImplementingClass()));

        saveAnUpdateConfigs(client, getConfigName(), existingConfiguration, new OnSucessActionListener<IndexResponse>(channel) {

            @Override
            public void onResponse(IndexResponse response) {
                if (existed) {
                    successResponse(channel, "'" + name + "' updated.");
                } else {
                    createdResponse(channel, "'" + name + "' created.");
                }
            }
        });

    }
}
