/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import java.nio.file.Path;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.dlic.auth.http.jwt.AbstractHTTPJwtAuthenticator;
import com.floragunn.dlic.util.SettingsBasedSSLConfigurator;

public class HTTPJwtKeyByOpenIdConnectAuthenticator extends AbstractHTTPJwtAuthenticator {

	//private final static Logger log = LogManager.getLogger(HTTPJwtKeyByOpenIdConnectAuthenticator.class);

	public HTTPJwtKeyByOpenIdConnectAuthenticator(Settings settings, Path configPath) {
		super(settings, configPath);
	}

	protected KeyProvider initKeyProvider(Settings settings, Path configPath) throws Exception {
		int idpRequestTimeoutMs = settings.getAsInt("idp_request_timeout_ms", 5000);
		int idpQueuedThreadTimeoutMs = settings.getAsInt("idp_queued_thread_timeout_ms", 2500);

		int refreshRateLimitTimeWindowMs = settings.getAsInt("refresh_rate_limit_time_window_ms", 10000);
		int refreshRateLimitCount = settings.getAsInt("refresh_rate_limit_count", 10);

		KeySetRetriever keySetRetriever = new KeySetRetriever(settings.get("openid_connect_url"),
				getSSLConfig(settings, configPath), settings.getAsBoolean("cache_jwks_endpoint", false));

		keySetRetriever.setRequestTimeoutMs(idpRequestTimeoutMs);

		SelfRefreshingKeySet selfRefreshingKeySet = new SelfRefreshingKeySet(keySetRetriever);

		selfRefreshingKeySet.setRequestTimeoutMs(idpRequestTimeoutMs);
		selfRefreshingKeySet.setQueuedThreadTimeoutMs(idpQueuedThreadTimeoutMs);
		selfRefreshingKeySet.setRefreshRateLimitTimeWindowMs(refreshRateLimitTimeWindowMs);
		selfRefreshingKeySet.setRefreshRateLimitCount(refreshRateLimitCount);

		return selfRefreshingKeySet;
	}

	private static SettingsBasedSSLConfigurator.SSLConfig getSSLConfig(Settings settings, Path configPath)
			throws Exception {
		return new SettingsBasedSSLConfigurator(settings, configPath, "openid_connect_idp").buildSSLConfig();
	}

	@Override
	public String getType() {
		return "jwt-key-by-oidc";
	}

}
