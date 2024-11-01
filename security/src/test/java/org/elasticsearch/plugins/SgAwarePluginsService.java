package org.elasticsearch.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import com.floragunn.searchguard.SearchGuardPlugin;

public class SgAwarePluginsService extends PluginsService {

    private final List<LoadedPlugin> loadedPlugins = new ArrayList<>();
    private static final List<Class<? extends Plugin>> STANDARD_PLUGINS = List.of(
            Netty4Plugin.class,
            MustachePlugin.class,
            ParentJoinPlugin.class,
            PercolatorPlugin.class,
            ReindexPlugin.class);


    public SgAwarePluginsService(Settings settings, List<Class<? extends Plugin>> additionalPlugins) {
        super(settings, null, null, null);

        loadSearchGuardPlugin(settings);
        loadMainRestPlugin();
        loadPainlessPluginIfAvailable();

        for(Class<? extends Plugin> plugin: Stream.concat(STANDARD_PLUGINS.stream(), additionalPlugins.stream()).toList()) {
            try {
                loadedPlugins.add(createLoadedPlugin(plugin));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void loadSearchGuardPlugin(Settings settings) {
        loadedPlugins.add(createLoadedPlugin(new SearchGuardPlugin(settings, null)));
    }

    private void loadMainRestPlugin() {
        try {
            Class<? extends Plugin> mainRestPlugin = (Class<? extends Plugin>) Class.forName("org.elasticsearch.rest.root.MainRestPlugin");
            loadedPlugins.add(createLoadedPlugin(mainRestPlugin));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void loadPainlessPluginIfAvailable() {
        try {
            Class<? extends Plugin> painlessPlugin = (Class<? extends Plugin>) Class.forName("org.elasticsearch.painless.PainlessPlugin");
            loadedPlugins.add(createLoadedPlugin(painlessPlugin));
        } catch (ClassNotFoundException e) {
            //that's ok
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<LoadedPlugin> plugins() {
        return loadedPlugins;
    }

    private static LoadedPlugin createLoadedPlugin(Plugin plugin) {
        return new LoadedPlugin(createPluginDescriptor(plugin.getClass()),plugin);
    }
    private static LoadedPlugin createLoadedPlugin(Class<? extends Plugin> pluginClass) throws Exception {
        return new LoadedPlugin(createPluginDescriptor(pluginClass),pluginClass.getDeclaredConstructor().newInstance());
    }

    private static PluginDescriptor createPluginDescriptor(Class<? extends Plugin> pluginClass)  {
        return new PluginDescriptor(
                pluginClass.getSimpleName(),
                pluginClass.getSimpleName(),
                "0.0.0",
                Build.current().version(),
                "17",
                pluginClass.getSimpleName(),
                "",
                Collections.emptyList(),
                false,
                false,
                true,
                false);
    }

    @Override
    protected void addServerExportsService(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        // empty for tests
    }
}
