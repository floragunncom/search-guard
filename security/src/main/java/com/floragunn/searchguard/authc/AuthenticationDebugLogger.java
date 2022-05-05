/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc;

import java.util.ArrayList;
import java.util.List;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.base.AuthcResult.DebugInfo;

public abstract class AuthenticationDebugLogger {
    public static AuthenticationDebugLogger create(boolean enabled) {
        if (enabled) {
            return new AuthenticationDebugLogger.Active();
        } else {
            return DISABLED;
        }
    }

    public abstract ImmutableList<AuthcResult.DebugInfo> get();

    abstract void add(String authcMethod, boolean success, String message);

    abstract void add(String authcMethod, boolean success, String message, ImmutableMap<String, Object> details);

    public abstract void add(AuthcResult.DebugInfo debugInfo);

    public abstract boolean isEnabled();

    public void success(String authcMethod, String message) {
        add(authcMethod, true, message);
    }

    public void success(String authcMethod, String message, ImmutableMap<String, Object> details) {
        add(authcMethod, true, message, details);
    }

    public void failure(String authcMethod, String message) {
        add(authcMethod, false, message);
    }

    public void failure(String authcMethod, String message, ImmutableMap<String, Object> details) {
        add(authcMethod, false, message, details);
    }

    public void success(String authcMethod, String message, String detail1key, Object detail1value) {
        if (isEnabled()) {
            success(authcMethod, message, ImmutableMap.of(detail1key, detail1value));
        }
    }

    public void success(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value) {
        if (isEnabled()) {
            success(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value));
        }
    }

    public void success(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value) {
        if (isEnabled()) {
            success(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value));
        }
    }

    public void success(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value, String detail4key, Object detail4value) {
        if (isEnabled()) {
            success(authcMethod, message,
                    ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value, detail4key, detail4value));
        }
    }

    public void success(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value, String detail4key, Object detail4value, String detail5key, Object detail5value) {
        if (isEnabled()) {
            success(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value, detail4key,
                    detail4value, detail5key, detail5value));
        }
    }

    public void failure(String authcMethod, String message, String detail1key, Object detail1value) {
        if (isEnabled()) {
            failure(authcMethod, message, ImmutableMap.of(detail1key, detail1value));
        }
    }

    public void failure(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value) {
        if (isEnabled()) {
            failure(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value));
        }
    }

    public void failure(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value) {
        if (isEnabled()) {
            failure(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value));
        }
    }

    public void failure(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value, String detail4key, Object detail4value) {
        if (isEnabled()) {
            failure(authcMethod, message,
                    ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value, detail4key, detail4value));
        }
    }

    public void failure(String authcMethod, String message, String detail1key, Object detail1value, String detail2key, Object detail2value,
            String detail3key, Object detail3value, String detail4key, Object detail4value, String detail5key, Object detail5value) {
        if (isEnabled()) {
            failure(authcMethod, message, ImmutableMap.of(detail1key, detail1value, detail2key, detail2value, detail3key, detail3value, detail4key,
                    detail4value, detail5key, detail5value));
        }
    }

    public static class Active extends AuthenticationDebugLogger {

        private final List<AuthcResult.DebugInfo> list = new ArrayList<>();

        @Override
        void add(String authcMethod, boolean success, String message) {
            list.add(new AuthcResult.DebugInfo(authcMethod, success, message));
        }

        @Override
        void add(String authcMethod, boolean success, String message, ImmutableMap<String, Object> details) {
            list.add(new AuthcResult.DebugInfo(authcMethod, success, message, details));
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public ImmutableList<DebugInfo> get() {
            return ImmutableList.of(list);
        }

        @Override
        public void add(DebugInfo debugInfo) {
            list.add(debugInfo);
        }

    }

    public static final AuthenticationDebugLogger DISABLED = new AuthenticationDebugLogger() {

        @Override
        void add(String authcMethod, boolean success, String message) {

        }

        @Override
        void add(String authcMethod, boolean success, String message, ImmutableMap<String, Object> details) {

        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public ImmutableList<DebugInfo> get() {
            return ImmutableList.empty();
        }

        @Override
        public void add(DebugInfo debugInfo) {

        }

    };
}
