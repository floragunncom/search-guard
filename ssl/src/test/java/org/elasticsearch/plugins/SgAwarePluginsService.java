package org.elasticsearch.plugins;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.Collections;
import java.util.List;

public class SgAwarePluginsService extends PluginsService{

    private final List<LoadedPlugin> loadedPlugins;

    public SgAwarePluginsService(Settings settings) {
        super(settings, null, null, null);

        LoadedPlugin ssl =
                new LoadedPlugin(new PluginDescriptor(
                        "sgflx",
                        "sgflx",
                        "0.0.0",
                        Version.CURRENT,
                        "17",
                        SearchGuardSSLPlugin.class.getSimpleName(),
                        "",
                        Collections.emptyList(),
                        false,
                        PluginType.ISOLATED,
                        "",
                        false),
                        new SearchGuardSSLPlugin(settings, null)
                );

        LoadedPlugin netty =
                new LoadedPlugin(new PluginDescriptor(
                        "netty4",
                        "netty4",
                        "0.0.0",
                        Version.CURRENT,
                        "17",
                        Netty4Plugin.class.getSimpleName(),
                        "",
                        Collections.emptyList(),
                        false,
                        PluginType.ISOLATED,
                        "",
                        false),
                        new Netty4Plugin()
                );

        loadedPlugins = List.of(netty, ssl);
    }

    @Override
    protected List<LoadedPlugin> plugins() {
        return loadedPlugins;
    }
}
