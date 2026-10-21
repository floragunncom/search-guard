/*
 * Copyright 2022-2026 floragunn GmbH
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

package org.elasticsearch.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;

public class SgAwarePluginsService extends PluginsService {

    private final List<LoadedPlugin> loadedPlugins = new ArrayList<>();
    private static final List<Class<? extends Plugin>> STANDARD_PLUGINS = List.of(Netty4Plugin.class);


    public SgAwarePluginsService(Settings settings, List<Class<? extends Plugin>> additionalPlugins, PluginsLoader pluginsLoader) {
        super(settings, null, pluginsLoader);

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
        loadedPlugins.add(createLoadedPlugin(new SearchGuardSSLPlugin(settings, null)));
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
            Class<?> painlessExtensionClass = Class.forName("org.elasticsearch.painless.spi.PainlessExtension");
            LoadedPlugin loadedPainlessPlugin = createLoadedPlugin(painlessPlugin);
            painlessWhitelistKludge(loadedPainlessPlugin.instance(), painlessExtensionClass);
            loadedPlugins.add(loadedPainlessPlugin);
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
        return new LoadedPlugin(createPluginDescriptor(plugin.getClass()),plugin, SgAwarePluginsService.class.getClassLoader());
    }
    private static LoadedPlugin createLoadedPlugin(Class<? extends Plugin> pluginClass) throws Exception {
        return new LoadedPlugin(createPluginDescriptor(pluginClass),pluginClass.getDeclaredConstructor().newInstance(), SgAwarePluginsService.class.getClassLoader());
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
                false,
                PluginDescriptor.DeploymentTarget.ALL);
    }

    /**
     * Triggers loading of SPI extensions for the painless plugin.
     */
    private void painlessWhitelistKludge(Plugin plugin, Class<?> painlessExtensionClass) {
        ExtensiblePlugin painlessPlugin = (ExtensiblePlugin) plugin;
        painlessPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType.equals(painlessExtensionClass)) {
                    List<?> result = StreamSupport.stream(ServiceLoader.load(painlessExtensionClass, getClass().getClassLoader()).spliterator(), false)
                            .collect(Collectors.toList());

                    return (List<T>) result;
                } else {
                    return Collections.emptyList();
                }
            }
        });
    }
}
