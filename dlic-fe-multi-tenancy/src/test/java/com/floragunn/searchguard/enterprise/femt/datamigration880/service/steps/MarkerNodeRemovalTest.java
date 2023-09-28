package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import org.junit.Test;


import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsFieldPointedByJsonPath;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

public class MarkerNodeRemovalTest {

    @Test
    public void shouldRemoveOnlyMigrationMarker_1() throws DocumentParseException {
        DocNode properties = DocNode.of("prop_one", 1, "prop_two", 2, "sg_data_migrated_to_8_8_0", DocNode.of("type","boolean"));
        DocNode node = DocNode.of("root_one", 1, "root_two", 2, "properties", properties);

        DocNode processed = MarkerNodeRemoval.withoutMigrationMarker(node);

        assertThat(processed, containsValue("$.root_one", 1));
        assertThat(processed, containsValue("$.root_two", 2));
        assertThat(processed, docNodeSizeEqualTo("$", 3));
        assertThat(processed, containsValue("$.properties.prop_one", 1));
        assertThat(processed, containsValue("$.properties.prop_two", 2));
        assertThat(processed, not(containsFieldPointedByJsonPath("$.properties", "sg_data_migrated_to_8_8_0")));
        assertThat(processed, docNodeSizeEqualTo("$.properties", 2));

    }

    @Test
    public void shouldRemoveOnlyMigrationMarker_2() throws DocumentParseException {
        DocNode root = DocNode.EMPTY;
        for(int i = 0; i < 100; ++i) {
            root = root.with("main_attribute_" + i, "value_" + i);
        }
        DocNode properties = DocNode.EMPTY;
        for(int i = 0; i < 100; ++i) {
            properties = properties.with("sub_attribute_" + i, "sub_value_" + i);
        }
        properties = properties.with("sg_data_migrated_to_8_8_0", "please remove me!");
        DocNode node = root.with("properties", properties);

        DocNode processed = MarkerNodeRemoval.withoutMigrationMarker(node);

        assertThat(processed, containsValue("main_attribute_5", "value_5"));
        assertThat(processed, docNodeSizeEqualTo("$", 101));
        assertThat(processed, containsValue("$.properties.sub_attribute_44", "sub_value_44"));
        assertThat(processed, docNodeSizeEqualTo("$.properties", 100));
        assertThat(processed, not(containsFieldPointedByJsonPath("$.properties", "sg_data_migrated_to_8_8_0")));
    }

}