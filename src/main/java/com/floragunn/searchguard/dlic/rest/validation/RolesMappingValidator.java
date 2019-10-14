/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlic.rest.validation;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;

public class RolesMappingValidator extends AbstractConfigurationValidator {

	public RolesMappingValidator(final RestRequest request, final BytesReference ref, final Settings esSettings, Object... param) {
		super(request, ref, esSettings, param);
		this.payloadMandatory = true;
		allowedKeys.put("backend_roles", DataType.ARRAY);
		allowedKeys.put("and_backend_roles", DataType.ARRAY);
		allowedKeys.put("hosts", DataType.ARRAY);
		allowedKeys.put("users", DataType.ARRAY);
		allowedKeys.put("description", DataType.STRING);

		mandatoryOrKeys.add("backend_roles");
		mandatoryOrKeys.add("and_backend_roles");
		mandatoryOrKeys.add("hosts");
		mandatoryOrKeys.add("users");
	}
}
