package com.floragunn.signals.actions.settings.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsSettings;

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
                listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, Strings.toString(settings), "application/json"));
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
                    // XXX
                    String value = String.valueOf(setting.get(settings));

                    listener.onResponse(new GetSettingsResponse(GetSettingsResponse.Status.OK, value, "text/plain"));
                }

            }

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}