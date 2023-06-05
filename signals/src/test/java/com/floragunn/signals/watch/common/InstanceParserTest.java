/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.watch.common;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class InstanceParserTest {

    public static final String PARAM_ONE = "param_one";
    public static final String PARAM_TWO = "param_two";
    public static final String PARAM_THREE = "param_three";
    public static final String PARAM_FOUR = "param_four";
    public static final String PARAM_FIVE = "param_five";
    public static final String ATTRIBUTE_ENABLED = "enabled";
    public static final String ATTRIBUTE_PARAMS = "params";
    public static final String ATTRIBUTE_INSTANCES = "instances";
    private ValidationErrors validationErrors;
    private InstanceParser instanceParser;

    @Before
    public void before() {
        this.validationErrors = new ValidationErrors();
        this.instanceParser = new InstanceParser(validationErrors);
    }

    @Test
    public void shouldCreateDisabledInstanceWithoutParameters() {
        DocNode node = DocNode.EMPTY;
        ValidatingDocNode validatingNode = new ValidatingDocNode(node, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances, notNullValue());
        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithTwoParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE, PARAM_TWO));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(2));
        assertThat(instances.getParams(), contains(PARAM_ONE, PARAM_TWO));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithOneParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(1));
        assertThat(instances.getParams(), contains(PARAM_ONE));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithManyParameters() {
        ImmutableList<String> parameters = ImmutableList.of(PARAM_ONE, PARAM_THREE, PARAM_FOUR, PARAM_FIVE);
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, parameters);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(4));
        assertThat(instances.getParams(), contains(PARAM_ONE, PARAM_THREE, PARAM_FOUR, PARAM_FIVE));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithNoParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithEmptyParametersList() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.empty());
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldReportErrorWhenEnabledAttributeIsMissing() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_FIVE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances, notNullValue());
        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(validationErrors.size(), equalTo(2));
        assertThat(validationErrors.getErrors(), hasKey("instances"));
        assertThat(validationErrors.getErrors(), hasKey("enabled"));
    }

    @Test
    public void shouldReportErrorWhenParametersAreNotPlacedInList() {
        ImmutableMap<String, String> invalidParameterStructure = ImmutableMap.of(PARAM_ONE, PARAM_TWO);
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, invalidParameterStructure);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors(), hasKey("params.0"));
    }

    @Test
    public void shouldReportErrorWhenEnabledAttributeIsNotBoolean() {
        String invalidEnabledAttributeValue = "active";
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, invalidEnabledAttributeValue, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.size(), equalTo(2));
        assertThat(validationErrors.getErrors(), hasKey("instances"));
        assertThat(validationErrors.getErrors(), hasKey("enabled"));
    }

    @Test
    public void shouldDetectInvalidParameterName(){
        String invalidParameter = "1-invalid-parameter-name";
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of(invalidParameter));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(true));
        Map<String, Object> errorMap = validationErrors.toMap();
        assertThat(errorMap, hasKey("params.0"));
        List<Map<String, Object>> list = (List<Map<String, Object>>) errorMap.get("params.0");
        String errorMessage = (String) list.get(0).get("error");
        assertThat(errorMessage, equalTo("Invalid value"));
        String invalidValue = (String)list.get(0).get("value");
        assertThat(invalidValue, equalTo(invalidParameter));
    }

    @Test
    public void shouldContainValidParameterNames() {
        ImmutableList<String> validParameterNames = ImmutableList.of("c", "c4", "c_4", "_c", "C", "searchGuard", "search_Guard",
            "SEARCH_GUARD7", "___search__guard_512_", "SearchGuard717", "S0", "SG0", "_111100000", "_1S_g", "SEARCH1Guard");
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, validParameterNames);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(15));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldReportValidationErrorWhenParameterNameIsId() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of("id"));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        instanceParser.parse(validatingNode);

        assertThat(validationErrors.hasErrors(), equalTo(true));
        Map<String, Object> errorMap = validationErrors.toMap();
        assertThat(errorMap, aMapWithSize(1));
        assertThat(errorMap, hasKey("params.0"));
        Map<String, String> idParameterErrorMap = ((List<Map<String, String>>) errorMap.get("params.0")).get(0);
        assertThat(idParameterErrorMap, Matchers.hasEntry("error", "Invalid value"));
        assertThat(idParameterErrorMap, Matchers.hasEntry("value", "id"));
    }

}