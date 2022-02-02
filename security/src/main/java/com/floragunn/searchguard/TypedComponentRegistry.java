/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public class TypedComponentRegistry {
    private static final Logger log = LogManager.getLogger(TypedComponentRegistry.class);

    private Map<Class<Object>, Map<String, TypedComponent.Info<Object>>> content = new HashMap<>();

    public <ComponentType> ComponentType create(Class<ComponentType> componentType, String name, DocNode config,
            ConfigurationRepository.Context context) throws NoSuchComponentException, ConfigValidationException {
        Map<String, TypedComponent.Info<Object>> nameToFactoryMap = content.get(componentType);

        if (nameToFactoryMap == null) {
            throw new NoSuchComponentException(componentType + "/" + name);
        }

        TypedComponent.Info<?> info = nameToFactoryMap.get(name);

        if (info != null) {
            return componentType.cast(info.getFactory().create(config, context));
        }

        if (name.contains(".")) {
            // Fallback: Try the class name

            try {
                return componentType
                        .cast(Class.forName(name).getConstructor(DocNode.class, ConfigurationRepository.Context.class).newInstance(config, context));
            } catch (ClassNotFoundException e) {
                log.debug("Component class was not found: " + name, e);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof ConfigValidationException) {
                    throw (ConfigValidationException) e.getCause();
                } else {
                    throw new ConfigValidationException(
                            new ValidationError(null, "Could not create component " + name + " due to " + e.getCause().getMessage())
                                    .cause(e.getCause()));
                }
            } catch (NoSuchMethodException e) {
                throw new ConfigValidationException(
                        new ValidationError(null, "The component type " + name + " does not define a suitable constructor").cause(e));
            } catch (ClassCastException e) {
                throw new ConfigValidationException(
                        new ValidationError(null, "The component type " + name + " does not implement " + componentType).cause(e));
            } catch (Exception e) {
                throw new ConfigValidationException(
                        new ValidationError(null, "Could not create component " + name + " due to " + e.getMessage()).cause(e));
            }
        }

        throw new NoSuchComponentException(name, nameToFactoryMap.keySet());
    }

    public <ComponentType> String getAvailableSubTypesAsShortString(Class<ComponentType> componentType) {
        Map<String, TypedComponent.Info<Object>> nameToFactoryMap = content.get(componentType);

        if (nameToFactoryMap == null) {
            return "n/a";
        }

        return nameToFactoryMap.keySet().stream().collect(Collectors.joining("|"));
    }

    @SuppressWarnings("unchecked")
    void register(TypedComponent.Info<?> componentInfo) {
        Map<String, TypedComponent.Info<Object>> nameToFactoryMap = content.get(componentInfo.getType());

        if (nameToFactoryMap == null) {
            nameToFactoryMap = new LinkedHashMap<String, TypedComponent.Info<Object>>();
            content.put((Class<Object>) componentInfo.getType(), nameToFactoryMap);
        }

        nameToFactoryMap.put(componentInfo.getName(), (TypedComponent.Info<Object>) componentInfo);
    }

    void register(List<TypedComponent.Info<?>> componentInfos) {
        for (TypedComponent.Info<?> info : componentInfos) {
            register(info);
        }
    }

    <C extends Object> void registerInstance(Class<C> type, String subType, C instance) {
        register(type, subType, (config, context) -> instance);
    }

    <C extends Object> void register(Class<C> type, String subType, TypedComponent.Factory<C> factory) {
        register(new TypedComponent.Info<C>() {

            @Override
            public Class<C> getType() {
                return type;
            }

            @Override
            public String getName() {
                return subType;
            }

            @Override
            public TypedComponent.Factory<C> getFactory() {
                return factory;
            }

        });
    }

}
