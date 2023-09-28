package com.floragunn.signals.actions.summary;

import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ActionSummaryToWatchFieldMapperTest {

    @Test
    public void shouldMapLastTriggeredFieldForMyAction() {
        final String fieldName = "actions.my-action-name.triggered";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_triggered"));
    }

    @Test
    public void shouldMapLastTriggeredFieldOtherAction() {
        final String fieldName = "actions.other-action.triggered";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.other-action.last_triggered"));
    }

    @Test
    public void shouldNotFindIncorrectField() {
        final String fieldName = "actions.other-action.incorrect-field";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(false));
    }

    @Test
    public void shouldMapLastCheckedFieldForMyAction() {
        final String fieldName = "actions.my-action-name.checked";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_check"));
    }

    @Test
    public void shouldMapLastCheckedFieldForOtherAction() {
        final String fieldName = "actions.other-action-name.checked";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.other-action-name.last_check"));
    }

    @Test
    public void shouldMapCheckResultFieldForOtherAction() {
        final String fieldName = "actions.other-action-name.check_result";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.other-action-name.last_check_result"));
    }

    @Test
    public void shouldMapCheckResultFieldForImportantAction() {
        final String fieldName = "actions.important-action-name.check_result";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.important-action-name.last_check_result"));
    }

    @Test
    public void shouldMapLastExecutionFieldForImportantAction() {
        final String fieldName = "actions.important-action-name.execution";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.important-action-name.last_execution"));
    }

    @Test
    public void shouldMapLastExecutionFieldForMyAction() {
        final String fieldName = "actions.my-action-name.execution";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_execution"));
    }

    @Test
    public void shouldMapLastErrorFieldForMyAction() {
        final String fieldName = "actions.my-action-name.error";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_error"));
    }

    @Test
    public void shouldMapLastErrorFieldForOtherAction() {
        final String fieldName = "actions.other-action-name.error";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.other-action-name.last_error"));
    }

    @Test
    public void shouldMapLastStatusCodeFieldForOtherAction() {
        final String fieldName = "actions.other-action-name.status_code";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.other-action-name.last_status.code.keyword"));
    }

    @Test
    public void shouldMapLastStatusCodeFieldForMyAction() {
        final String fieldName = "actions.my-action-name.status_code";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_status.code.keyword"));
    }

    @Test
    public void shouldMapLastStatusDetailsFieldForMyAction() {
        final String fieldName = "actions.my-action-name.status_details";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.my-action-name.last_status.detail.keyword"));
    }

    @Test
    public void shouldMapLastStatusDetailsFieldForFirstAction() {
        final String fieldName = "actions.first-action-name.status_details";

        Optional<SummaryToWatchFieldMapper> foundField = SummaryToWatchFieldMapper.findFieldByName(fieldName);

        assertThat(foundField.isPresent(), equalTo(true));
        SummaryToWatchFieldMapper summaryField = foundField.get();
        String documentFieldName = summaryField.getDocumentFieldName(fieldName);
        assertThat(documentFieldName, equalTo("actions.first-action-name.last_status.detail.keyword"));
    }
}