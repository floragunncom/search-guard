/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

/**
 * This class defines an API that can be consumed by other ES plugins to check DLS restrictions on indices. The interfaces exposed by this class are strictly informational and read only. Other plugins will
 * not be able to influence the access controls with these interfaces.
 * 
 * Plugins can obtain on instance of this class by dependency injection, i.e., by specifying DlsFlsPluginApi in any @ Inject constructor, such as constructors of TransportActions.
 *
 * Instances of this class are only available if searchguard.dlsfls.plugin_api.enabled is set to true in elasticsearch.yml.
 */
public class DlsFlsPluginApi implements ComponentStateProvider {
    public static final StaticSettings.Attribute<Boolean> ENABLED = StaticSettings.Attribute.define("searchguard.dlsfls.plugin_api.enabled")
            .withDefault(false).asBoolean();

    private final AtomicReference<DlsFlsProcessedConfig> config;
    private final DlsFlsBaseContext baseContext;
    private final ComponentState componentState = new ComponentState(0, null, "dls_fls_plugin_api", DlsFlsPluginApi.class).initialized();
    private final TimeAggregation queryTimeAggregation = new TimeAggregation.Nanoseconds();

    public DlsFlsPluginApi(AtomicReference<DlsFlsProcessedConfig> config, DlsFlsBaseContext baseContext) {
        this.config = config;
        this.baseContext = baseContext;
        this.componentState.addMetrics("query", queryTimeAggregation);
    }

    /**
     * Returns the PrivilegesEvaluationContext for the user that has initiated the current request (based on the thread context information). 
     * Returns null if the current request has not been initiated by a user. Such requests are typically executed with full permissions.
     */
    public PrivilegesEvaluationContext getCurrentPrivilegeEvaluationContext() {
        return baseContext.getPrivilegesEvaluationContext();
    }

    /**
     * Returns DLS queries for the given index. 
     * 
     * @param context A PrivilegesEvaluationContext object as obtained by getCurrentPrivilegeEvaluationContext().
     * @param index The name of the index to be checked. Note: This must be an actual index name. Aliases or data stream names are not supported here.
     * @return A DlsRestriction object. If there are no restrictions, DlsRestriction.isUnrestricted() returns true. Otherwise, DlsRestriction.toQueryBuilder() returns the query builder 
     * that need to be applied to the search.
     * @throws PrivilegesEvaluationException
     */
    public DlsRestriction getDlsRestriction(PrivilegesEvaluationContext context, String index) throws PrivilegesEvaluationException {

        DlsFlsProcessedConfig config = this.config.get();

        try (Meter meter = Meter.detail(config.getMetricsLevel(), queryTimeAggregation)) {
            return config.getDocumentAuthorization().getDlsRestriction(context, index, meter);
        }
    }

    /**
     * Returns DLS queries for the given indices. 
     * 
     * @param context A PrivilegesEvaluationContext object as obtained by getCurrentPrivilegeEvaluationContext().
     * @param index The names of the index to be checked. Note: These must be actual index names. Aliases or data stream names are not supported here.
     * @return A DlsRestriction.IndexMap which provides restrictions for the individual indices. If there are no restrictions, DlsRestriction.IndexMap.isUnrestricted() returns true. 
     * @throws PrivilegesEvaluationException
     */
    public DlsRestriction.IndexMap getDlsRestriction(PrivilegesEvaluationContext context, Collection<String> indices)
            throws PrivilegesEvaluationException {

        DlsFlsProcessedConfig config = this.config.get();

        try (Meter meter = Meter.detail(config.getMetricsLevel(), queryTimeAggregation)) {
            return config.getDocumentAuthorization().getDlsRestriction(context, indices, meter);
        }
    }

    //
    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
