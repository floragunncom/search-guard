/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.datamigration880.rest;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.enterprise.femt.datamigration880.rest.GetDataMigrationStateAction.GetDataMigrationStateHandler;
import com.floragunn.searchguard.enterprise.femt.datamigration880.rest.StartDataMigrationAction.StartDataMigrationHandler;
import com.floragunn.searchguard.enterprise.femt.datamigration880.rest.StartDataMigrationAction.StartDataMigrationRequest;
import com.floragunn.searchsupport.action.RestApi;
import org.elasticsearch.plugins.ActionPlugin;

import static com.floragunn.searchsupport.action.ActionHandlerFactory.actionHandler;

/**
 * Groups all REST endpoints and action handlers related to data migration
 */
public class DataMigrationApi {

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
            .handlesPost("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0")//
            .with(StartDataMigrationAction.INSTANCE, (params, body) -> new StartDataMigrationRequest(body))//
            .name("POST /_searchguard/config/fe_multi_tenancy/data_migration/8_8_0")
            .handlesGet("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0")//
            .with(GetDataMigrationStateAction.INSTANCE)//
            .name("GET /_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

    public static final ImmutableList<ActionPlugin.ActionHandler> ACTION_HANDLERS = ImmutableList.of(
            actionHandler(StartDataMigrationAction.INSTANCE, StartDataMigrationHandler.class),
            actionHandler(GetDataMigrationStateAction.INSTANCE, GetDataMigrationStateHandler.class)
    );

}
