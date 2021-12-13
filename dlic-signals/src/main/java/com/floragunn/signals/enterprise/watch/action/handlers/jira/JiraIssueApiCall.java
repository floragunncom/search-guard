package com.floragunn.signals.enterprise.watch.action.handlers.jira;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

public class JiraIssueApiCall implements ToXContentObject {

    private String project;
    private String parent;
    private String summary;
    private String description;
    private String issueType;
    private String priority;
    private List<String> components;
    private List<String> labels;

    private Map<Integer, Object> customFields;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field("fields");
        builder.startObject();

        builder.field("project").startObject().field("key", project).endObject();

        if (parent != null) {
            builder.field("parent").startObject().field("key", parent).endObject();
        }

        if (priority != null) {
            builder.field("priority").startObject().field("name", priority).endObject();
        }

        builder.field("summary", summary);
        builder.field("description", description);
        builder.field("issuetype").startObject().field("name", issueType).endObject();

        if (components != null && components.size() > 0) {
            builder.field("components");
            builder.startArray();
            for (String component : components) {
                builder.startObject();
                builder.field("name", component);
                builder.endObject();
            }
            builder.endArray();
        }

        if (labels != null && labels.size() > 0) {
            builder.field("labels", labels);
        }

        if (customFields != null) {
            for (Map.Entry<Integer, Object> entry : customFields.entrySet()) {
                builder.field("customfield_" + entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();

        builder.endObject();

        return builder;
    }

    public String toJson() {
        return Strings.toString(this);
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getIssueType() {
        return issueType;
    }

    public void setIssueType(String issueType) {
        this.issueType = issueType;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public Map<Integer, Object> getCustomFields() {
        return customFields;
    }

    public void setCustomFields(Map<Integer, Object> customFields) {
        this.customFields = customFields;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public List<String> getComponents() {
        return components;
    }

    public void setComponents(List<String> components) {
        this.components = components;
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

}
