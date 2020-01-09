package com.floragunn.signals.accounts;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonParser;
import com.floragunn.signals.SignalsSettings;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.pagerduty.PagerDutyAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;

public abstract class Account implements ToXContentObject {

    private String id;

    public boolean isInUse(Client client, SignalsSettings settings) {
        long hits = client.search(new SearchRequest(settings.getStaticSettings().getIndexNames().getWatches()).source(getReferencingWatchesQuery()))
                .actionGet().getHits().getTotalHits().value;

        return hits > 0;
    }

    public void isInUse(Client client, SignalsSettings settings, ActionListener<Boolean> actionListener) {
        client.search(new SearchRequest(settings.getStaticSettings().getIndexNames().getWatches()).source(getReferencingWatchesQuery())
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN), new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        if (response.getHits().getTotalHits().value > 0) {
                            actionListener.onResponse(Boolean.TRUE);
                        } else {
                            actionListener.onResponse(Boolean.FALSE);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        actionListener.onFailure(e);
                    }
                });
    }

    public abstract SearchSourceBuilder getReferencingWatchesQuery();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getScopedId() {
        return getType().getPrefix() + "/" + id;
    }

    public abstract AccountType getType();

    public final String toJson() throws JsonProcessingException {
        return Strings.toString(this);
    }

    public static Account parse(AccountType accountType, String id, String string) throws ConfigValidationException {
        return create(accountType, id, ValidatingJsonParser.readTree(string));
    }

    public static Account create(AccountType accountType, String id, JsonNode jsonNode) throws ConfigValidationException {

        if (accountType == AccountType.EMAIL) {
            return EmailAccount.create(id, jsonNode);
        } else if (accountType == AccountType.SLACK) {
            return SlackAccount.create(id, jsonNode);
        } else if (accountType == AccountType.PAGERDUTY) {
            return PagerDutyAccount.create(id, jsonNode);
        } else {
            throw new RuntimeException("Unsupported accountType " + accountType);
        }
    }

}
