package com.floragunn.signals.actions;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import com.floragunn.signals.actions.account.config_update.DestinationConfigUpdateAction;
import com.floragunn.signals.actions.account.config_update.TransportDestinationConfigUpdateAction;
import com.floragunn.signals.actions.account.delete.DeleteAccountAction;
import com.floragunn.signals.actions.account.delete.TransportDeleteAccountAction;
import com.floragunn.signals.actions.account.get.GetAccountAction;
import com.floragunn.signals.actions.account.get.TransportGetAccountAction;
import com.floragunn.signals.actions.account.put.PutAccountAction;
import com.floragunn.signals.actions.account.put.TransportPutAccountAction;
import com.floragunn.signals.actions.account.search.SearchAccountAction;
import com.floragunn.signals.actions.account.search.TransportSearchAccountAction;
import com.floragunn.signals.actions.admin.start_stop.StartStopAction;
import com.floragunn.signals.actions.admin.start_stop.TransportStartStopAction;
import com.floragunn.signals.actions.settings.get.GetSettingsAction;
import com.floragunn.signals.actions.settings.get.TransportGetSettingsAction;
import com.floragunn.signals.actions.settings.put.PutSettingsAction;
import com.floragunn.signals.actions.settings.put.TransportPutSettingsAction;
import com.floragunn.signals.actions.settings.update.SettingsUpdateAction;
import com.floragunn.signals.actions.settings.update.TransportSettingsUpdateAction;
import com.floragunn.signals.actions.tenant.start_stop.StartStopTenantAction;
import com.floragunn.signals.actions.tenant.start_stop.TransportStartStopTenantAction;
import com.floragunn.signals.actions.watch.ack.AckWatchAction;
import com.floragunn.signals.actions.watch.ack.TransportAckWatchAction;
import com.floragunn.signals.actions.watch.activate_deactivate.DeActivateWatchAction;
import com.floragunn.signals.actions.watch.activate_deactivate.TransportDeActivateWatchAction;
import com.floragunn.signals.actions.watch.delete.DeleteWatchAction;
import com.floragunn.signals.actions.watch.delete.TransportDeleteWatchAction;
import com.floragunn.signals.actions.watch.execute.ExecuteWatchAction;
import com.floragunn.signals.actions.watch.execute.TransportExecuteWatchAction;
import com.floragunn.signals.actions.watch.get.GetWatchAction;
import com.floragunn.signals.actions.watch.get.TransportGetWatchAction;
import com.floragunn.signals.actions.watch.put.PutWatchAction;
import com.floragunn.signals.actions.watch.put.TransportPutWatchAction;
import com.floragunn.signals.actions.watch.search.SearchWatchAction;
import com.floragunn.signals.actions.watch.search.TransportSearchWatchAction;
import com.floragunn.signals.actions.watch.state.get.GetWatchStateAction;
import com.floragunn.signals.actions.watch.state.get.TransportGetWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.SearchWatchStateAction;
import com.floragunn.signals.actions.watch.state.search.TransportSearchWatchStateAction;

public class SignalsActions {
    public static List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(AckWatchAction.INSTANCE, TransportAckWatchAction.class),
                new ActionHandler<>(GetWatchAction.INSTANCE, TransportGetWatchAction.class),
                new ActionHandler<>(PutWatchAction.INSTANCE, TransportPutWatchAction.class),
                new ActionHandler<>(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class),
                new ActionHandler<>(SearchWatchAction.INSTANCE, TransportSearchWatchAction.class),
                new ActionHandler<>(DeActivateWatchAction.INSTANCE, TransportDeActivateWatchAction.class),
                new ActionHandler<>(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class),
                new ActionHandler<>(DestinationConfigUpdateAction.INSTANCE, TransportDestinationConfigUpdateAction.class),
                new ActionHandler<>(PutAccountAction.INSTANCE, TransportPutAccountAction.class),
                new ActionHandler<>(GetAccountAction.INSTANCE, TransportGetAccountAction.class),
                new ActionHandler<>(DeleteAccountAction.INSTANCE, TransportDeleteAccountAction.class),
                new ActionHandler<>(SearchAccountAction.INSTANCE, TransportSearchAccountAction.class),
                new ActionHandler<>(GetWatchStateAction.INSTANCE, TransportGetWatchStateAction.class),
                new ActionHandler<>(SettingsUpdateAction.INSTANCE, TransportSettingsUpdateAction.class),
                new ActionHandler<>(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class),
                new ActionHandler<>(PutSettingsAction.INSTANCE, TransportPutSettingsAction.class),
                new ActionHandler<>(StartStopTenantAction.INSTANCE, TransportStartStopTenantAction.class),
                new ActionHandler<>(StartStopAction.INSTANCE, TransportStartStopAction.class),
                new ActionHandler<>(SearchWatchStateAction.INSTANCE, TransportSearchWatchStateAction.class)

        );
    }
}
