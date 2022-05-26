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

package com.floragunn.searchsupport;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.function.Supplier;

import org.elasticsearch.SpecialPermission;

public class PrivilegedCode {

    public static <R> R execute(Supplier<R> supplier) {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(new PrivilegedAction<R>() {
            @Override
            public R run() {
                return supplier.get();
            }
        });
    }
    
    public static void execute(Runnable runnable) {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                runnable.run();
                return null;
            }
        });
    }

    public static <R, E1 extends Exception> R execute(PrivilegedSupplierThrowing1<R, E1> supplier, Class<E1> throws1) throws E1 {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<R>() {
                @Override
                public R run() throws E1 {
                    return supplier.get();
                }
            });
        } catch (PrivilegedActionException e) {
            if (throws1.isAssignableFrom(e.getCause().getClass())) {
                throw throws1.cast(e.getCause());
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static <R, E1 extends Exception, E2 extends Exception> R execute(PrivilegedSupplierThrowing2<R, E1, E2> supplier, Class<E1> throws1,
            Class<E2> throws2) throws E1, E2 {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<R>() {
                @Override
                public R run() throws E1, E2 {
                    return supplier.get();
                }
            });
        } catch (PrivilegedActionException e) {
            if (throws1.isAssignableFrom(e.getCause().getClass())) {
                throw throws1.cast(e.getCause());
            } else if (throws2.isAssignableFrom(e.getCause().getClass())) {
                throw throws2.cast(e.getCause());
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static <R, E1 extends Exception, E2 extends Exception, E3 extends Exception> R execute(PrivilegedSupplierThrowing3<R, E1, E2, E3> supplier,
            Class<E1> throws1, Class<E2> throws2, Class<E3> throws3) throws E1, E2, E3 {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<R>() {
                @Override
                public R run() throws E1, E2, E3 {
                    return supplier.get();
                }
            });
        } catch (PrivilegedActionException e) {
            if (throws1.isAssignableFrom(e.getCause().getClass())) {
                throw throws1.cast(e.getCause());
            } else if (throws2.isAssignableFrom(e.getCause().getClass())) {
                throw throws2.cast(e.getCause());
            } else if (throws3.isAssignableFrom(e.getCause().getClass())) {
                throw throws3.cast(e.getCause());
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static <E1 extends Exception> void execute(PrivilegedProcedureThrowing1<E1> procedure, Class<E1> throws1) throws E1 {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws E1 {
                    procedure.run();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            if (throws1.isAssignableFrom(e.getCause().getClass())) {
                throw throws1.cast(e.getCause());
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @FunctionalInterface
    public interface PrivilegedSupplierThrowing1<R, E1 extends Exception> {
        R get() throws E1;
    }

    @FunctionalInterface
    public interface PrivilegedSupplierThrowing2<R, E1 extends Exception, E2 extends Exception> {
        R get() throws E1, E2;
    }

    @FunctionalInterface
    public interface PrivilegedSupplierThrowing3<R, E1 extends Exception, E2 extends Exception, E3 extends Exception> {
        R get() throws E1, E2, E3;
    }

    @FunctionalInterface
    public interface PrivilegedProcedureThrowing1<E1 extends Exception> {
        void run() throws E1;
    }
}
