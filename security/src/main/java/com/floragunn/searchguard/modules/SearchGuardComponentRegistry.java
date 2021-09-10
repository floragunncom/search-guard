/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.searchguard.modules;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.opensearch.common.settings.Settings;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.support.ReflectionHelper;

public class SearchGuardComponentRegistry<ComponentType> {

    private Class<ComponentType> componentType;
    private Map<String, ComponentType> instanceMap = new HashMap<>();
    private Map<String, ComponentFactory<? extends ComponentType>> factoryMap = new HashMap<>();
    private Map<String, Class<? extends ComponentType>> classMap = new HashMap<>();
    private Map<String, String> classNameMap = new HashMap<>();

    private Function<ComponentType, String> nameFunction;

    public SearchGuardComponentRegistry(Class<ComponentType> componentType) {
        this.componentType = componentType;
        this.nameFunction = (c) -> c.toString();
    }

    public SearchGuardComponentRegistry(Class<ComponentType> componentType, Function<ComponentType, String> nameFunction) {
        this.componentType = componentType;
        this.nameFunction = nameFunction;
    }

    public void addComponentsWithMatchingType(Collection<?> components) {
        for (Object component : components) {
            if (componentType.isAssignableFrom(component.getClass())) {
                ComponentType typedComponent = componentType.cast(component);
                String name = nameFunction.apply(typedComponent);

                if (name != null) {
                    ensureNameIsVacant(name);
                    this.instanceMap.put(name, typedComponent);
                }
            }
        }
    }

    public SearchGuardComponentRegistry<ComponentType> add(SearchGuardComponentRegistry<ComponentType> registry) {
        this.instanceMap.putAll(registry.instanceMap);
        this.classMap.putAll(registry.classMap);
        this.classNameMap.putAll(registry.classNameMap);
        this.factoryMap.putAll(registry.factoryMap);
        return this;
    }

    public SearchGuardComponentRegistry<ComponentType> seal() {
        this.instanceMap = Collections.unmodifiableMap(this.instanceMap);
        this.classMap = Collections.unmodifiableMap(this.classMap);
        this.classNameMap = Collections.unmodifiableMap(this.classNameMap);
        this.factoryMap = Collections.unmodifiableMap(this.factoryMap);

        return this;
    }

    public SearchGuardComponentRegistry<ComponentType> add(String name, ComponentFactory<? extends ComponentType> instance) {
        ensureNameIsVacant(name);
        this.factoryMap.put(name, instance);
        return this;
    }

    public SearchGuardComponentRegistry<ComponentType> add(List<String> names, ComponentFactory<? extends ComponentType> instance) {
        for (String name : names) {
            ensureNameIsVacant(name);
        }
        for (String name : names) {
            this.factoryMap.put(name, instance);
        }
        return this;
    }

    public SearchGuardComponentRegistry<ComponentType> add(String name, Class<? extends ComponentType> clazz) {
        ensureNameIsVacant(name);
        this.classMap.put(name, clazz);
        return this;
    }

    public SearchGuardComponentRegistry<ComponentType> add(String name, String className) {
        ensureNameIsVacant(name);
        this.classNameMap.put(name, className);
        return this;
    }

    public boolean has(String name) {
        return this.instanceMap.containsKey(name) || this.factoryMap.containsKey(name) || this.classMap.containsKey(name)
                || this.classNameMap.containsKey(name);
    }

    public Object getAny(String name) {
        if (this.instanceMap.containsKey(name)) {
            return this.instanceMap.get(name);
        }

        if (this.factoryMap.containsKey(name)) {
            return this.factoryMap.get(name);
        }

        if (this.classMap.containsKey(name)) {
            return this.classMap.get(name);
        }

        if (this.classNameMap.containsKey(name)) {
            return this.classNameMap.get(name);
        }

        return null;
    }

    public ComponentType getInstance(String clazzOrShortcut, Settings settings, Path configPath) throws ConfigValidationException {
        if (this.instanceMap.containsKey(clazzOrShortcut)) {
            ComponentType result = this.instanceMap.get(clazzOrShortcut);
            ReflectionHelper.addLoadedModule(result.getClass());
            return result;
        } else if (this.factoryMap.containsKey(clazzOrShortcut)) {
            ComponentType result = this.factoryMap.get(clazzOrShortcut).create(settings, configPath);
            ReflectionHelper.addLoadedModule(result.getClass());
            return result;
        } else if (this.classMap.containsKey(clazzOrShortcut)) {
            String className = this.classMap.get(clazzOrShortcut).getName();
            return ReflectionHelper.instantiateAAA(className, settings, configPath, ReflectionHelper.isEnterpriseAAAModule(className));
        } else if (this.classNameMap.containsKey(clazzOrShortcut)) {
            String className = this.classNameMap.get(clazzOrShortcut);
            return ReflectionHelper.instantiateAAA(className, settings, configPath, ReflectionHelper.isEnterpriseAAAModule(className));
        } else {
            return ReflectionHelper.instantiateAAA(clazzOrShortcut, settings, configPath, true);
        }
    }

    public ComponentType getInstance(String clazzOrShortcut, Map<String, Object> config, AuthenticationFrontend.Context context) throws ConfigValidationException, NoSuchComponentException {
        if (this.instanceMap.containsKey(clazzOrShortcut)) {
            ComponentType result = this.instanceMap.get(clazzOrShortcut);
            ReflectionHelper.addLoadedModule(result.getClass());
            return result;
        } else if (this.factoryMap.containsKey(clazzOrShortcut)) {
            throw new UnsupportedOperationException();
            //ComponentType result = this.factoryMap.get(clazzOrShortcut).create(settings, configPath);
            //ReflectionHelper.addLoadedModule(result.getClass());
            //return result;
        } else if (this.classMap.containsKey(clazzOrShortcut)) {
            String className = this.classMap.get(clazzOrShortcut).getName();
            try {
                return ReflectionHelper.instantiateAAA(className, config, context, ReflectionHelper.isEnterpriseAAAModule(className));
            } catch (ClassNotFoundException e) {
                // This should not happen
                throw new RuntimeException("Could not find class " + className + " associated with " + clazzOrShortcut + " in " + this, e);
            }
        } else if (this.classNameMap.containsKey(clazzOrShortcut)) {
            String className = this.classNameMap.get(clazzOrShortcut);
            try {
                return ReflectionHelper.instantiateAAA(className, config, context, ReflectionHelper.isEnterpriseAAAModule(className));
            } catch (ClassNotFoundException e) {
                // This should not happen
                throw new RuntimeException("Could not find class " + className + " associated with " + clazzOrShortcut + " in " + this, e);
            }
        } else {
            try {
                return ReflectionHelper.instantiateAAA(clazzOrShortcut, config, context, true);
            } catch (ClassNotFoundException e) {
                throw new NoSuchComponentException(clazzOrShortcut, e);
            }
        }
    }

    public String getClassName(String clazzOrShortcut) {
        if (this.instanceMap.containsKey(clazzOrShortcut)) {
            return this.instanceMap.get(clazzOrShortcut).getClass().getName();
        } else if (this.factoryMap.containsKey(clazzOrShortcut)) {
            return this.factoryMap.get(clazzOrShortcut).getClassName();
        } else if (this.classMap.containsKey(clazzOrShortcut)) {
            return this.classMap.get(clazzOrShortcut).getName();
        } else if (this.classNameMap.containsKey(clazzOrShortcut)) {
            return this.classNameMap.get(clazzOrShortcut);
        } else {
            return clazzOrShortcut;
        }
    }

    private void ensureNameIsVacant(String name) {
        if (this.has(name)) {
            throw new IllegalStateException("A component with name " + name + " is already defined: " + this.getAny(name));
        }
    }

    public interface ComponentFactory<ComponentType> {
        ComponentType create(Settings settings, Path configPath) throws ConfigValidationException;

        String getClassName();
    }
}
