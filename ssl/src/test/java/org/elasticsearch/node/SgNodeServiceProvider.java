package org.elasticsearch.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SgAwarePluginsService;

import java.util.List;

class SgNodeServiceProvider extends NodeServiceProvider {

    private final List<Class<? extends Plugin>> additionalPlugins;

    public SgNodeServiceProvider(List<Class<? extends Plugin>> additionalPlugins) {
        this.additionalPlugins = additionalPlugins;
    }

    @Override
    PluginsService newPluginService(Environment environment, Settings settings) {
        return new SgAwarePluginsService(settings, additionalPlugins);
    }
}
