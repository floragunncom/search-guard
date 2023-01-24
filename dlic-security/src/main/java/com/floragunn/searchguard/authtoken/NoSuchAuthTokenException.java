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

public class NoSuchAuthTokenException extends Exception {

    private static final long serialVersionUID = -343178809366694796L;

    public NoSuchAuthTokenException(String id) {
        super("No such auth token: " + id);
    }

    public NoSuchAuthTokenException(String id, Throwable cause) {
        super("No such auth token: " + id, cause);
    }

}
