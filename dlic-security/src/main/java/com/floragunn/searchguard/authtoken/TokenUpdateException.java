/*
  * Copyright 2020 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.authtoken;

public class TokenUpdateException extends Exception {

    private static final long serialVersionUID = -8909316128729638749L;

    public TokenUpdateException() {
        super();
    }

    public TokenUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public TokenUpdateException(String message) {
        super(message);
    }

    public TokenUpdateException(Throwable cause) {
        super(cause);
    }

}
