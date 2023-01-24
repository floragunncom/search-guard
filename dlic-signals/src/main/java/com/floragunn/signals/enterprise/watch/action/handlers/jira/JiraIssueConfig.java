/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.signals.enterprise.watch.action.handlers.jira;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.InlineMustacheTemplate;
import com.floragunn.signals.watch.init.WatchInitializationService;
import java.io.IOException;
import java.util.Collections;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class JiraIssueConfig implements ToXContentObject {
    private InlineMustacheTemplate<String> summaryTemplate;
    private InlineMustacheTemplate<String> descriptionTemplate;
    private InlineMustacheTemplate<String> parentIssueTemplate;
    private InlineMustacheTemplate<String> priorityTemplate;
    private InlineMustacheTemplate<String> componentTemplate;
    private InlineMustacheTemplate<String> labelTemplate;

    private String issueType;

    public JiraIssueConfig(String issueType, InlineMustacheTemplate<String> summaryTemplate, InlineMustacheTemplate<String> descriptionTemplate) {
        this.issueType = issueType;
        this.summaryTemplate = summaryTemplate;
        this.descriptionTemplate = descriptionTemplate;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("type", issueType);

        if (parentIssueTemplate != null) {
            builder.field("parent", parentIssueTemplate);
        }

        builder.field("summary", summaryTemplate);
        builder.field("description", descriptionTemplate);

        if (priorityTemplate != null) {
            builder.field("priority", priorityTemplate);
        }

        if (componentTemplate != null) {
            builder.field("component", componentTemplate);
        }

        if (labelTemplate != null) {
            builder.field("label", labelTemplate);
        }

        builder.endObject();

        return builder;
    }

    JiraIssueApiCall render(WatchExecutionContext ctx, JiraAccount account, JiraAction action) throws ActionExecutionException {
        ValidationErrors validationErrors = new ValidationErrors();

        JiraIssueApiCall call = new JiraIssueApiCall();

        call.setProject(action.getProject());
        call.setIssueType(issueType);

        if (parentIssueTemplate != null) {
            call.setParent(parentIssueTemplate.get(ctx.getTemplateScriptParamsAsMap(), "parent", validationErrors));
        }

        if (summaryTemplate != null) {
            call.setSummary(summaryTemplate.get(ctx.getTemplateScriptParamsAsMap(), "summary", validationErrors));
        }

        if (descriptionTemplate != null) {
            call.setDescription(descriptionTemplate.get(ctx.getTemplateScriptParamsAsMap(), "description", validationErrors));
        }

        if (priorityTemplate != null) {
            call.setDescription(priorityTemplate.get(ctx.getTemplateScriptParamsAsMap(), "priority", validationErrors));
        }

        if (labelTemplate != null) {
            call.setLabels(Collections.singletonList(labelTemplate.get(ctx.getTemplateScriptParamsAsMap(), "label", validationErrors)));
        }

        if (componentTemplate != null) {
            call.setComponents(Collections.singletonList(componentTemplate.get(ctx.getTemplateScriptParamsAsMap(), "component", validationErrors)));
        }

        if (validationErrors.hasErrors()) {
            throw new ActionExecutionException(null, "Error while rendering Jira API call", validationErrors);
        }

        return call;

    }

    public InlineMustacheTemplate<String> getPriorityTemplate() {
        return priorityTemplate;
    }

    public void setPriorityTemplate(InlineMustacheTemplate<String> priorityTemplate) {
        this.priorityTemplate = priorityTemplate;
    }

    static JiraIssueConfig create(WatchInitializationService watchInitializationService, DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);
        ScriptService scriptService = watchInitializationService.getScriptService();

        String issueType = vJsonNode.get("type").required().asString();
        InlineMustacheTemplate<String> summaryTemplate = vJsonNode.get("summary").required()
                .byString((s) -> InlineMustacheTemplate.parse(scriptService, s));
        InlineMustacheTemplate<String> descriptionTemplate = vJsonNode.get("description").required()
                .byString((s) -> InlineMustacheTemplate.parse(scriptService, s));

        JiraIssueConfig result = new JiraIssueConfig(issueType, summaryTemplate, descriptionTemplate);

        result.priorityTemplate = vJsonNode.get("priority").byString((s) -> InlineMustacheTemplate.parse(scriptService, s));
        result.parentIssueTemplate = vJsonNode.get("parent").byString((s) -> InlineMustacheTemplate.parse(scriptService, s));
        result.componentTemplate = vJsonNode.get("component").byString((s) -> InlineMustacheTemplate.parse(scriptService, s));
        result.labelTemplate = vJsonNode.get("label").byString((s) -> InlineMustacheTemplate.parse(scriptService, s));

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public InlineMustacheTemplate<String> getSummaryTemplate() {
        return summaryTemplate;
    }

    public void setSummaryTemplate(InlineMustacheTemplate<String> summaryTemplate) {
        this.summaryTemplate = summaryTemplate;
    }

    public InlineMustacheTemplate<String> getDescriptionTemplate() {
        return descriptionTemplate;
    }

    public void setDescriptionTemplate(InlineMustacheTemplate<String> descriptionTemplate) {
        this.descriptionTemplate = descriptionTemplate;
    }

    public InlineMustacheTemplate<String> getParentIssueTemplate() {
        return parentIssueTemplate;
    }

    public void setParentIssueTemplate(InlineMustacheTemplate<String> parentIssueTemplate) {
        this.parentIssueTemplate = parentIssueTemplate;
    }

    public InlineMustacheTemplate<String> getComponentTemplate() {
        return componentTemplate;
    }

    public void setComponentTemplate(InlineMustacheTemplate<String> componentTemplate) {
        this.componentTemplate = componentTemplate;
    }

    public InlineMustacheTemplate<String> getLabelTemplate() {
        return labelTemplate;
    }

    public void setLabelTemplate(InlineMustacheTemplate<String> labelTemplate) {
        this.labelTemplate = labelTemplate;
    }

    public String getIssueType() {
        return issueType;
    }

    public void setIssueType(String issueType) {
        this.issueType = issueType;
    }
}
