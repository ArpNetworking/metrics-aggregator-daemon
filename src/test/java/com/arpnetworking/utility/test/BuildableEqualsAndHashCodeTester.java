/**
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

import com.arpnetworking.commons.builder.Builder;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Test utility class asserts that the <code>equals</code> and <code>hashCode</code>
 * methods work as expected on a buildable class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class BuildableEqualsAndHashCodeTester {

    /**
     * Test the <code>equals</code> and <code>hashCode</code> methods on a
     * buildable class <code>T</code> given two <code>Builder</code> instances
     * with entirely separate values.
     *
     * @param <T> The class being built.
     * @param builderA Prepopulated <code>Builder</code> instance for <code>T</code>
     * with all fields set to values different from <code>builderB</code>.
     * @param builderB Prepopulated <code>Builder</code> instance for <code>T</code>
     * with all fields set to values different from <code>builderA</code>.
     */
    public static <T> void assertEqualsAndHashCode(final Builder<T> builderA, final Builder<T> builderB) {
        // Create two instances of the buildable
        final T instanceA1 = builderA.build();
        final T instanceA2 = builderA.build();

        // Test self-equality
        Assert.assertTrue(instanceA1.equals(instanceA1));
        Assert.assertEquals(instanceA1.hashCode(), instanceA1.hashCode());

        // Test null equality
        Assert.assertFalse(instanceA1.equals(null));

        // Test other type equality
        Assert.assertFalse(instanceA1.equals("This is a string"));

        // Test equality
        Assert.assertTrue(instanceA1.equals(instanceA2));
        Assert.assertTrue(instanceA1.equals(instanceA2));
        Assert.assertEquals(instanceA1.hashCode(), instanceA2.hashCode());

        // Check all member permutations for equality
        final T instanceB1 = builderB.build();
        for (final Method setterMethod : builderA.getClass().getMethods()) {
            if (TestHelper.isSetterMethod(setterMethod)) {
                try {
                    final Method getterMethod = TestHelper.getterMethodForSetter(setterMethod, instanceA1.getClass());

                    // Change the field on the builder
                    setterMethod.invoke(builderB, getterMethod.invoke(instanceA1));

                    // Create an instance with the changed field
                    final T instanceB2 = builderB.build();

                    // Assert that it is not equal
                    Assert.assertFalse("Setter failed to affect equality: " + setterMethod.getName(), instanceB1.equals(instanceB2));
                    Assert.assertFalse("Setter failed to affect equality: " + setterMethod.getName(), instanceB2.equals(instanceB1));

                    // Change the field back
                    setterMethod.invoke(builderB, getterMethod.invoke(instanceB1));

                } catch (final SecurityException | IllegalAccessException
                        | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private BuildableEqualsAndHashCodeTester() {}
}
