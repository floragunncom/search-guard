/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.dlsfls;

public class DlsFlsLicenseInfo {
    private final boolean licenseForFieldMaskingAvailable;

    DlsFlsLicenseInfo(boolean licenseForFieldMaskingAvailable) {
        this.licenseForFieldMaskingAvailable = licenseForFieldMaskingAvailable;
    }

    public boolean isLicenseForFieldMaskingAvailable() {
        return licenseForFieldMaskingAvailable;
    }
}
