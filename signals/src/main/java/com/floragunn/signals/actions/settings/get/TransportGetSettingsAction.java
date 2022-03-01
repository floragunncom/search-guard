package com.floragunn.signals.actions.settings.get;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContent.Params;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.signals.Signals;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.settings.SignalsSettings.ParsedSettingsKey;
import com.google.common.collect.ImmutableMap;

public class TransportGetSettingsAction extends HandledTransportAction<GetSettingsRequest, GetSettingsResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetSettingsAction.class);

    private final Signals signals;

    @Inject
    public TransportGetSettingsAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(GetSettingsAction.NAME, transportService, actionFilters, GetSettingsRequest::new);

        this.signals = signals;
    }

    @Override
    protected final void doExecute(Task task, GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
        try {

            Settings settings = this.signals.getSignalsSettings().getDynamicSettings().getSettings();

            if (request.getKey() == null) {
                listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, asJson(settings), "application/json"));
            } else {

                ParsedSettingsKey parsedKey = SignalsSettings.DynamicSettings.matchSetting(request.getKey());

                Object value = getValue(parsedKey, settings);

                if (log.isDebugEnabled()) {
                    log.debug("parsedKey: " + parsedKey + "; value: " + value + "; r: " + settings.get(request.getKey()));
                }

                if (!request.isJsonRequested() && (value instanceof String || value instanceof Number || value instanceof Boolean)) {
                    listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, String.valueOf(value), "text/plain"));
                } else {
                    String json = DefaultObjectMapper.objectMapper.writeValueAsString(value);
                    listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, json, "application/json"));
                }

            }
        } catch (ConfigValidationException e) {
            listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.NOT_FOUND, e.getMessage(), null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String asJson(Settings settings) {
        try {
            Params params = new ToXContent.MapParams(ImmutableMap.of(SettingsFilter.SETTINGS_FILTER_PARAM, "internal_auth.*"));
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            settings.toXContent(builder, params);
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Object getValue(ParsedSettingsKey parsedKey, Settings settings) {

        if (parsedKey.isGroup()) {
            Settings group = settings.getByPrefix(parsedKey.setting.getKey() + parsedKey.groupName);
            return parsedKey.getSubSetting().get(group);
        } else {
            return parsedKey.getSetting().get(settings);
        }
    }

}