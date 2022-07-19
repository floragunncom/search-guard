package org.elasticsearch.plugins;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SgAwarePluginsService extends PluginsService {

    private final List<LoadedPlugin> loadedPlugins;
    public List<Class<? extends Plugin>> plugins = Lists.newArrayList(Netty4Plugin.class, MatrixAggregationPlugin.class,
            MustachePlugin.class, ParentJoinPlugin.class, PercolatorPlugin.class, ReindexPlugin.class);


    public SgAwarePluginsService(Settings settings) {
        super(settings, null, null, null);
        loadedPlugins = new ArrayList<LoadedPlugin>();
        LoadedPlugin sg =
                new LoadedPlugin(new PluginDescriptor(
                        "sgflx",
                        "sgflx",
                        "0.0.0",
                        Version.CURRENT,
                        "17",
                        SearchGuardPlugin.class.getSimpleName(),
                        "",
                        Collections.emptyList(),
                        false,
                        PluginType.ISOLATED,
                        "",
                        false),
                        new SearchGuardPlugin(settings, null)
                );
        loadedPlugins.add(sg);

        for(Class<? extends Plugin> plugin: plugins) {
            try {
                LoadedPlugin p =
                        new LoadedPlugin(new PluginDescriptor(
                                plugin.getSimpleName(),
                                plugin.getSimpleName(),
                                "0.0.0",
                                Version.CURRENT,
                                "17",
                                plugin.getSimpleName(),
                                "",
                                Collections.emptyList(),
                                false,
                                PluginType.ISOLATED,
                                "",
                                false),
                                plugin.getDeclaredConstructor().newInstance()
                        );
                loadedPlugins.add(p);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        try {
            Class<? extends Plugin> painlessPlugin = (Class<? extends Plugin>) Class.forName("org.elasticsearch.painless.PainlessPlugin");
            LoadedPlugin p =
                    new LoadedPlugin(new PluginDescriptor(
                            painlessPlugin.getSimpleName(),
                            painlessPlugin.getSimpleName(),
                            "0.0.0",
                            Version.CURRENT,
                            "17",
                            painlessPlugin.getSimpleName(),
                            "",
                            Collections.emptyList(),
                            false,
                            PluginType.ISOLATED,
                            "",
                            false),
                            painlessPlugin.getDeclaredConstructor().newInstance()
                    );
            loadedPlugins.add(p);
        } catch (ClassNotFoundException e) {
            //thats ok
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    protected List<LoadedPlugin> plugins() {
        return loadedPlugins;
    }
}
