/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.documents;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.AbstractJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingException;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class BasicJsonPathDefaultConfiguration implements com.jayway.jsonpath.Configuration.Defaults {

    public static Configuration defaultConfiguration() {
        return DEFAULT_CONFIGURATION;
    }
    
    public static Configuration listDefaultConfiguration() {
        return LIST_DEFAULT_CONFIGURATION;
    }
    
    public static Configuration.ConfigurationBuilder builder() {
        return Configuration.builder().jsonProvider(JSON_PROVIDER).mappingProvider(MAPPING_PROVIDER);
    }
    
    public static final JsonProvider JSON_PROVIDER = new AbstractJsonProvider() {

        @Override
        public String toJson(Object obj) {
            return DocWriter.writeAsString(obj);
        }

        @Override
        public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
            try {
                return DocReader.read(new InputStreamReader(jsonStream, charset));
            } catch (IOException e) {
                throw new InvalidJsonException(e);
            }
        }

        @Override
        public Object parse(String json) throws InvalidJsonException {
            try {
                return DocReader.read(json);
            } catch (JsonProcessingException e) {
                throw new InvalidJsonException(e);
            }
        }

        @Override
        public Object createMap() {
            return new LinkedHashMap<>();
        }

        @Override
        public Object createArray() {
            return new ArrayList<>();
        }
    };

    public static final MappingProvider MAPPING_PROVIDER = new MappingProvider() {

        @Override
        public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
            if (source == null) {
                return null;
            }
            if (targetType.isAssignableFrom(source.getClass())) {
                return targetType.cast(source);
            }

            if (targetType.equals(String.class)) {
                return targetType.cast(source.toString());
            }

            if (source instanceof Number) {
                Number number = (Number) source;

                if (Long.class.equals(targetType)) {
                    return targetType.cast(Long.valueOf(number.longValue()));
                } else if (Integer.class.equals(targetType)) {
                    return targetType.cast(Integer.valueOf(number.intValue()));
                } else if (Short.class.equals(targetType)) {
                    return targetType.cast(Short.valueOf(number.shortValue()));
                } else if (Byte.class.equals(targetType)) {
                    return targetType.cast(Byte.valueOf(number.byteValue()));
                } else if (Float.class.equals(targetType)) {
                    return targetType.cast(Float.valueOf(number.floatValue()));
                } else if (Double.class.equals(targetType)) {
                    return targetType.cast(Double.valueOf(number.doubleValue()));
                } else if (BigDecimal.class.equals(targetType)) {
                    if (number instanceof BigInteger) {
                        return targetType.cast(new BigDecimal((BigInteger) number));
                    } else {
                        return targetType.cast(new BigDecimal(number.toString()));
                    }
                } else if (BigInteger.class.equals(targetType)) {
                    return targetType.cast(new BigInteger(number.toString()));
                }
            } else {
                String string = source.toString();

                try {
                    if (String.class.equals(targetType)) {
                        return targetType.cast(string);
                    } else if (Long.class.equals(targetType)) {
                        return targetType.cast(Long.valueOf(string));
                    } else if (Integer.class.equals(targetType)) {
                        return targetType.cast(Integer.valueOf(string));
                    } else if (Short.class.equals(targetType)) {
                        return targetType.cast(Short.valueOf(string));
                    } else if (Byte.class.equals(targetType)) {
                        return targetType.cast(Byte.valueOf(string));
                    } else if (Float.class.equals(targetType)) {
                        return targetType.cast(Float.valueOf(string));
                    } else if (Double.class.equals(targetType)) {
                        return targetType.cast(Double.valueOf(string));
                    } else if (BigDecimal.class.equals(targetType)) {
                        return targetType.cast(new BigDecimal(string));
                    } else if (BigInteger.class.equals(targetType)) {
                        return targetType.cast(new BigInteger(string));
                    } else if (Boolean.class.equals(targetType)) {
                        return targetType.cast(Boolean.valueOf(string));
                    } else if (Character.class.equals(targetType)) {
                        if (string.length() == 0) {
                            return null;
                        } else {
                            return targetType.cast(Character.valueOf(string.charAt(0)));
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new MappingException(e);
                }
            }

            throw new MappingException("Unsupported mapping from " + source.getClass() + " to " + targetType);
        }

        @Override
        public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration) {
            throw new UnsupportedOperationException("map(TypeRef) is not supported");
        }

    };
    
    private static final Configuration DEFAULT_CONFIGURATION = builder().build();
    private static final Configuration LIST_DEFAULT_CONFIGURATION = builder().options(Option.ALWAYS_RETURN_LIST).build();

    @Override
    public JsonProvider jsonProvider() {
        return JSON_PROVIDER;
    }

    @Override
    public Set<Option> options() {
        return EnumSet.noneOf(Option.class);
    }

    @Override
    public MappingProvider mappingProvider() {
        return MAPPING_PROVIDER;
    }

}
