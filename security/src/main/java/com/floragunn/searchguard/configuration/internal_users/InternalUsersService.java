/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.configuration.internal_users;

import static com.floragunn.searchguard.sgconf.impl.CType.INTERNALUSERS;

import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

class InternalUsersService {

	private final ConfigurationRepository configurationRepository;

	InternalUsersService(ConfigurationRepository configurationRepository) {
		this.configurationRepository = configurationRepository;
	}

	public InternalUser getUser(String userName) throws InternalUserNotFoundException, ConfigUnavailableException {
		SgDynamicConfiguration<InternalUser> users = getAllUsers();
		if (!users.exists(userName)) {
			throw new InternalUserNotFoundException(userName);
		}

		return users.getCEntry(userName);
	}

	public void addOrUpdateUser(String userName, InternalUser internalUser) throws ConfigUpdateException {
		try {
			SgDynamicConfiguration<InternalUser> users = getAllUsers();
			users.putCEntry(userName, internalUser);
			configurationRepository.update(INTERNALUSERS, users);
		} catch (ConfigUnavailableException e) {
			throw new ConfigUpdateException(e);
		}
	}

	public void deleteUser(String userName) throws ConfigUpdateException, InternalUserNotFoundException {
		try {
			SgDynamicConfiguration<InternalUser> users = getAllUsers();
			if (!users.exists(userName)) {
				throw new InternalUserNotFoundException(userName);
			}
			users.remove(userName);
			configurationRepository.update(INTERNALUSERS, users);
		} catch (ConfigUnavailableException e) {
			throw new ConfigUpdateException(e);
		}
	}

	private SgDynamicConfiguration<InternalUser> getAllUsers() throws ConfigUnavailableException {
		return configurationRepository.getConfigurationFromIndex(INTERNALUSERS, "API Request");
	}

}
