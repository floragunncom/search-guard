package com.floragunn.signals;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import org.elasticsearch.cluster.metadata.Template;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SignalsTest {

    @Test
    public void shouldCreateHiddenWatchLogIndex() {
        Template template = Signals.createWathLogTemplate(".i_am_hidden_watch_log_index", 0);

        assertThat(template.settings().get("index.hidden"), equalTo("true"));
        assertThat(template.settings().size(), equalTo(1));
        assertThat(template.mappings(), nullValue());
    }

    @Test
    public void shouldCreateNonHiddenWatchLogIndex() {
        Template template = Signals.createWathLogTemplate("i_am_NON_hidden_watch_log_index", 0);

        assertThat(template.settings().get("index.hidden"), equalTo("false"));
        assertThat(template.settings().size(), equalTo(1));
        assertThat(template.mappings(), nullValue());
    }

    @Test
    public void shouldCreateIndexWithExtendedLimitForDynamicMappings() {
        Template template = Signals.createWathLogTemplate("i_have_custom_settings", 2000);

        assertThat(template.settings().get("index.hidden"), equalTo("false"));
        assertThat(template.settings().get("mapping.total_fields.limit"), equalTo("2000"));
        assertThat(template.settings().size(), equalTo(2));
        assertThat(template.mappings(), nullValue());
    }

    @Test
    public void shouldCreateIndexWithHiddenIndexWithExtendedLimitForDynamicMappings() {
        Template template = Signals.createWathLogTemplate(".i_am_hidden_and_have_custom_settings", 4005);

        assertThat(template.settings().get("index.hidden"), equalTo("true"));
        assertThat(template.settings().get("mapping.total_fields.limit"), equalTo("4005"));
        assertThat(template.settings().size(), equalTo(2));
        assertThat(template.mappings(), nullValue());
    }

    @Test
    public void shouldDisableDynamicMappingForRuntimeDataForHiddenIndex() throws DocumentParseException {
        Template template = Signals.createWathLogTemplate(".i_am_hidden_and_have_disabled_mappings", -1);

        assertThat(template.settings().get("index.hidden"), equalTo("true"));
        assertThat(template.settings().size(), equalTo(1));
        String mappings = template.mappings().string();
        DocNode parsedMappings = DocNode.parse(Format.JSON).from(mappings);
        assertThat(parsedMappings, containsValue("$.properties.data.type", "object"));
        assertThat(parsedMappings, containsValue("$.properties.data.dynamic", "false"));
    }

    @Test
    public void shouldDisableDynamicMappingForRuntimeDataForNonHiddenIndex() throws DocumentParseException {
        Template template = Signals.createWathLogTemplate("i_have_disabled_mappings", -1);

        assertThat(template.settings().get("index.hidden"), equalTo("false"));
        assertThat(template.settings().size(), equalTo(1));
        String mappings = template.mappings().string();
        DocNode parsedMappings = DocNode.parse(Format.JSON).from(mappings);
        assertThat(parsedMappings, containsValue("$.properties.data.type", "object"));
        assertThat(parsedMappings, containsValue("$.properties.data.dynamic", "false"));
    }
}