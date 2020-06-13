/*
 * Copyright 2014 Groupon.com
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
 */
package com.arpnetworking.utility.test;

import java.lang.reflect.Method;
import java.util.Locale;

/**
 * Common filter utility helper methods.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TestHelper {

    /**
     * Determine whether a particular method is a setter method.
     *
     * @param method The method to evaluate.
     * @return True if and only if the method is a setter method.
     */
    public static boolean isSetterMethod(final Method method) {
        return method.getName().startsWith(SETTER_METHOD_PREFIX)
                &&
                !method.isVarArgs()
                &&
                method.getParameterTypes().length == 1;
    }

    /**
     * Determine whether a particular method is a getter method.
     *
     * @param method The method to evaluate.
     * @return True if and only if the method is a getter method.
     */
    public static boolean isGetterMethod(final Method method) {
        return (method.getName().startsWith(GETTER_GET_METHOD_PREFIX)
                        ||
                        method.getName().startsWith(GETTER_IS_METHOD_PREFIX))
                &&
                !Void.class.equals(method.getReturnType())
                &&
                !method.isVarArgs()
                &&
                method.getParameterTypes().length == 0;
    }

    /**
     * Return the corresponding getter method for a setter method.
     *
     * @param setterMethod The setter method.
     * @param clazz The {@link Class} to inspect.
     * @return The name of the corresponding getter method.
     */
    public static Method getterMethodForSetter(final Method setterMethod, final Class<?> clazz) {
        final String baseName = setterMethod.getName().substring(SETTER_METHOD_PREFIX.length());
        try {
            final String getterName = GETTER_GET_METHOD_PREFIX + baseName;
            return clazz.getDeclaredMethod(getterName);
        } catch (final NoSuchMethodException e1) {
            try {
                final String getterName = GETTER_IS_METHOD_PREFIX + baseName;
                return clazz.getDeclaredMethod(getterName);
            } catch (final NoSuchMethodException e2) {
                try {
                    final String getterName = baseName.substring(0, 1).toLowerCase(Locale.getDefault()) + baseName.substring(1);
                    return clazz.getDeclaredMethod(getterName);
                } catch (final NoSuchMethodException e3) {
                    throw new IllegalArgumentException("No matching getter method found for setter: " + setterMethod);
                }
            }
        }
    }

    private TestHelper() {}

    private static final String GETTER_GET_METHOD_PREFIX = "get";
    private static final String GETTER_IS_METHOD_PREFIX = "is";
    private static final String SETTER_METHOD_PREFIX = "set";
}
