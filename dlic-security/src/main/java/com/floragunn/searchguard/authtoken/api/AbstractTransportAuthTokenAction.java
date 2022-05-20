/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.authtoken.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable.Reader;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.user.User;

abstract class AbstractTransportAuthTokenAction<Request extends ActionRequest, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {
    private static final Logger log = LogManager.getLogger(AbstractTransportAuthTokenAction.class);

    private final PrivilegesEvaluator privilegesEvaluator;
    private final String allActionName;

    public AbstractTransportAuthTokenAction(String actionName, TransportService transportService, ActionFilters actionFilters,
            Reader<Request> requestReader, PrivilegesEvaluator privilegesEvaluator) {
        super(actionName, transportService, actionFilters, requestReader);
        this.privilegesEvaluator = privilegesEvaluator;
        this.allActionName = getAllActionName(actionName);
    }

    protected boolean isAllowedToAccessAll(User user, TransportAddress callerTransportAddress) {
        try {
            return this.privilegesEvaluator.hasClusterPermission(user, this.allActionName, callerTransportAddress);
        } catch (PrivilegesEvaluationException e) {
            log.error("Error in allowedToAccessAll(" + user + ")", e);
            return false;
        }
    }

    protected static String getAllActionName(String actionName) {
        return actionName.replace("/_own/", "/_all/");
    }

}
