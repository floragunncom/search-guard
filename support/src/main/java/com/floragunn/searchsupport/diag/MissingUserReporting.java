/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchsupport.diag;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * Feature flag for the diagnostic log messages which Search Guard writes when a component runs without a user in the
 * thread context. DLS, FLS, field masking and compliance read auditing are not applied at all in that case, which is
 * how an Elasticsearch code path that drops the thread context transients turns into a silent, fail open security
 * regression. The chunked fetch phase of Elasticsearch 9.5.2 was such a case.
 * <p>
 * Internal reads however legitimately run without a user, so the messages are noise for a normally operating cluster.
 * The flag is therefore off by default. It is dynamic, so the reporting can be switched on while a suspected loss of
 * the security context is being diagnosed and switched off again, without restarting a node:
 *
 * <pre>
 * PUT _cluster/settings
 * {"persistent": {"searchguard.diagnosis.report_missing_user.enabled": true}}
 * </pre>
 *
 * The counters which accompany the log messages are maintained regardless of this flag. They produce no output unless
 * the component state API is queried and are therefore never noisy.
 */
public class MissingUserReporting {

    public static final Setting<Boolean> ENABLED = Setting.boolSetting("searchguard.diagnosis.report_missing_user.enabled", false,
            Property.NodeScope, Property.Dynamic);

    /**
     * Permanently disabled instance for components which are constructed without a cluster service, in particular in
     * unit tests.
     */
    public static final MissingUserReporting DISABLED = new MissingUserReporting();

    private volatile boolean enabled;

    private MissingUserReporting() {
        this.enabled = false;
    }

    public MissingUserReporting(Settings settings, ClusterService clusterService) {
        this.enabled = ENABLED.get(settings != null ? settings : Settings.EMPTY);

        ClusterSettings clusterSettings = clusterService != null ? clusterService.getClusterSettings() : null;

        // The setting is registered by SearchGuardPlugin. If it is not registered - which happens for components that
        // are created outside of a regular node, such as in unit tests - the initial value is simply kept.
        if (clusterSettings != null && clusterSettings.get(ENABLED.getKey()) == ENABLED) {
            clusterSettings.addSettingsUpdateConsumer(ENABLED, (enabled) -> this.enabled = enabled);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }
}
