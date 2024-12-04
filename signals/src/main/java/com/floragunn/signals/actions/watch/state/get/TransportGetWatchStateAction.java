
package com.floragunn.signals.actions.watch.state.get;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.watch.state.WatchState;

public class TransportGetWatchStateAction extends HandledTransportAction<GetWatchStateRequest, GetWatchStateResponse> {

    private final static Logger log = LogManager.getLogger(TransportGetWatchStateAction.class);

    private final Signals signals;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetWatchStateAction(Signals signals, TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            Client client) {
        super(GetWatchStateAction.NAME, transportService, actionFilters, GetWatchStateRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));

        this.signals = signals;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, GetWatchStateRequest request, ActionListener<GetWatchStateResponse> listener) {

        try {
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                throw new RuntimeException("Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            threadPool.generic().submit(() -> {
                Map<String, WatchState> watchStates = signalsTenant.getWatchStateReader().get(request.getWatchIds());

                listener.onResponse(new GetWatchStateResponse(RestStatus.OK, toBytesReferenceMap(watchStates)));
            });
        } catch (SignalsUnavailableException e) {
            listener.onFailure(e.toElasticsearchException());         
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Map<String, BytesReference> toBytesReferenceMap(Map<String, WatchState> map) {
        Map<String, BytesReference> result = new HashMap<>(map.size());

        for (Map.Entry<String, WatchState> entry : map.entrySet()) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.value(entry.getValue());
                result.put(entry.getKey(), BytesReference.bytes(builder));
            } catch (Exception e) {
                log.error("Error while writing " + entry.getKey(), e);
            }
        }

        return result;
    }

}
