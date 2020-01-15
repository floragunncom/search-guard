package com.floragunn.signals.actions.settings.get;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsSettings;
import com.google.common.collect.ImmutableMap;

public class TransportGetSettingsAction extends HandledTransportAction<GetSettingsRequest, GetSettingsResponse> {

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
                Setting<?> setting = SignalsSettings.DynamicSettings.getSetting(request.getKey());

                if (setting == null) {
                    listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.NOT_FOUND, null, null));
                    return;
                }

                boolean group = !setting.getKey().equals(request.getKey());

                if (group) {
                    String value = settings.get(request.getKey());

                    listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, value, "text/plain"));

                } else {
                    Object value = setting.get(settings);

                    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                        listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, String.valueOf(value), "text/plain"));
                    } else {
                        String json = DefaultObjectMapper.objectMapper.writeValueAsString(value);
                        listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, json, "application/json"));
                    }
                }

            }

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
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}