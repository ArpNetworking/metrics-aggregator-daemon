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
package com.arpnetworking.tsdcore.model;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the Quantity class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class QuantityTest {

    @Test
    public void testConstructor() {
        final double expectedValue = 1.23f;
        final Unit expectedUnit = Unit.GIGABYTE;
        final Quantity sample = new Quantity.Builder()
                .setValue(expectedValue)
                .setUnit(expectedUnit)
                .build();
        Assert.assertEquals(expectedValue, sample.getValue(), 0.001);
        Assert.assertTrue(sample.getUnit().isPresent());
        Assert.assertEquals(expectedUnit, sample.getUnit().get());
    }

    @Test
    public void testCompare() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample3 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABIT)
                .build();
        final Quantity sample4 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.TERABYTE)
                .build();

        Assert.assertEquals(0, sample1.compareTo(sample1));
        Assert.assertEquals(0, sample1.compareTo(sample2));
        Assert.assertEquals(1, sample1.compareTo(sample3));
        Assert.assertEquals(-1, sample1.compareTo(sample4));

        final Quantity sample5 = new Quantity.Builder()
                .setValue(1.23d)
                .build();
        final Quantity sample6 = new Quantity.Builder()
                .setValue(1.23d)
                .build();
        final Quantity sample7 = new Quantity.Builder()
                .setValue(2.46d)
                .build();

        Assert.assertEquals(0, sample5.compareTo(sample6));
        Assert.assertEquals(-1, sample5.compareTo(sample7));
        Assert.assertEquals(1, sample7.compareTo(sample5));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareFailureAbsent() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .build();
        sample1.compareTo(sample2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareFailureAbsentReverse() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .build();
        sample2.compareTo(sample1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareFailureDifferentDomains() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.SECOND)
                .build();
        sample1.compareTo(sample2);
    }

    @Test
    public void testHash() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();

        Assert.assertEquals(sample1.hashCode(), sample2.hashCode());
    }

    @Test
    public void testEquality() {
        final Quantity sample1 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample2 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample3 = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABIT)
                .build();
        final Quantity sample4 = new Quantity.Builder()
                .setValue(2.46d)
                .setUnit(Unit.GIGABYTE)
                .build();
        final Quantity sample5 = new Quantity.Builder()
                .setValue(1.23d)
                .build();

        Assert.assertTrue(sample1.equals(sample1));
        Assert.assertFalse(sample1.equals("Not a sample"));
        Assert.assertFalse(sample1.equals(null));
        Assert.assertTrue(sample1.equals(sample2));
        Assert.assertFalse(sample1.equals(sample3));
        Assert.assertFalse(sample1.equals(sample4));
        Assert.assertFalse(sample1.equals(sample5));
        Assert.assertFalse(sample3.equals(sample4));
    }

    @Test
    public void testToString() {
        final Quantity sample = new Quantity.Builder()
                .setValue(1.23d)
                .setUnit(Unit.GIGABYTE)
                .build();
        Assert.assertNotNull(sample.toString());
        Assert.assertFalse(sample.toString().isEmpty());
    }

    @Test
    public void testUnify() {
        final List<Quantity> unified = Quantity.unify(
                ImmutableList.of(
                        new Quantity.Builder()
                                .setValue(120.0)
                                .setUnit(Unit.MINUTE)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1.0)
                                .setUnit(Unit.HOUR)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1800.0)
                                .setUnit(Unit.SECOND)
                                .build()));
        Assert.assertEquals(
                ImmutableList.of(
                        new Quantity.Builder()
                                .setValue(7200.0)
                                .setUnit(Unit.SECOND)
                                .build(),
                        new Quantity.Builder()
                                .setValue(3600.0)
                                .setUnit(Unit.SECOND)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1800.0)
                                .setUnit(Unit.SECOND)
                                .build()),
                unified);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnifyMissingAfter() {
        Quantity.unify(
                ImmutableList.of(
                        new Quantity.Builder()
                                .setValue(60.0)
                                .setUnit(Unit.MINUTE)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1.0)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnifyMissingBefore() {
        Quantity.unify(
                ImmutableList.of(
                        new Quantity.Builder()
                                .setValue(60.0)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1.0)
                                .setUnit(Unit.HOUR)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnifyMismatchedTypes() {
        Quantity.unify(
                ImmutableList.of(
                        new Quantity.Builder()
                                .setValue(60.0)
                                .setUnit(Unit.BYTE)
                                .build(),
                        new Quantity.Builder()
                                .setValue(1.0)
                                .setUnit(Unit.SECOND)
                                .build()));
    }

    @Test
    public void testConvertUnits() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity converted = quantity.convertTo(Unit.SECOND);
        Assert.assertEquals(60, converted.getValue(), 0.00001);
        Assert.assertEquals(Unit.SECOND, converted.getUnit().orNull());
    }

    @Test
    public void testConvertUnitIdentity() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity converted = quantity.convertTo(Unit.MILLISECOND);
        Assert.assertSame(quantity, converted);
    }

    @Test(expected = IllegalStateException.class)
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
    public void testConvertUnitMissing() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .build();
        quantity.convertTo(Unit.MILLISECOND);
    }

    @Test
    public void testConvertUnitsOptional() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity converted = quantity.convertTo(Optional.of(Unit.SECOND));
        Assert.assertEquals(60, converted.getValue(), 0.00001);
        Assert.assertEquals(Unit.SECOND, converted.getUnit().orNull());
    }

    @Test
    public void testConvertUnitIdentityOptional() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity converted = quantity.convertTo(Optional.of(Unit.MILLISECOND));
        Assert.assertSame(quantity, converted);
    }

    @Test(expected = IllegalStateException.class)
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
    public void testConvertUnitMissingOptional() {
        final Quantity quantity = new Quantity.Builder()
                .setValue(60000.0)
                .build();
        quantity.convertTo(Optional.of(Unit.MILLISECOND));
    }

    @Test
    public void testAddQuantities() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .build();
        final Quantity result = quantity1.add(quantity2);
        Assert.assertEquals(15.0d, result.getValue(), 0.00001);
    }

    @Test
    public void testAddQuantitiesUnits() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity result = quantity1.add(quantity2);
        Assert.assertEquals(5010.0d, result.getValue(), 0.00001);
        Assert.assertEquals(Unit.MILLISECOND, result.getUnit().orNull());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddQuantitiesUnitsMismatch() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .setUnit(Unit.BYTE)
                .build();
        quantity1.add(quantity2);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddQuantitiesUnitsMismatchExist() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .build();
        quantity1.add(quantity2);
    }

    @Test
    public void testSubQuantities() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(10.0)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(5.0)
                .build();
        final Quantity result = quantity1.subtract(quantity2);
        Assert.assertEquals(5.0d, result.getValue(), 0.00001);
    }

    @Test
    public void testSubQuantitiesUnits() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .setUnit(Unit.MILLISECOND)
                .build();
        final Quantity result = quantity1.subtract(quantity2);
        Assert.assertEquals(4990.0d, result.getValue(), 0.00001);
        Assert.assertEquals(Unit.MILLISECOND, result.getUnit().orNull());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubQuantitiesUnitsMismatch() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .setUnit(Unit.BYTE)
                .build();
        quantity1.subtract(quantity2);
    }

    @Test(expected = IllegalStateException.class)
    public void testSubQuantitiesUnitsMismatchExist() {
        final Quantity quantity1 = new Quantity.Builder()
                .setValue(5.0)
                .setUnit(Unit.SECOND)
                .build();
        final Quantity quantity2 = new Quantity.Builder()
                .setValue(10.0)
                .build();
        quantity1.subtract(quantity2);
    }
}
