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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the Unit class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class UnitTest {

    @Test
    public void testConvert() {
        Assert.assertEquals(1, Unit.BYTE.convert(8.0, Unit.BIT), 0.001);
        Assert.assertEquals(8, Unit.BIT.convert(1.0, Unit.BYTE), 0.001);
    }

    @Test
    public void testGetSmallerUnit() {
        Assert.assertEquals(Unit.BIT, Unit.BYTE.getSmallerUnit(Unit.BIT));
        Assert.assertEquals(Unit.BIT, Unit.BIT.getSmallerUnit(Unit.BYTE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSmallerUnitDifferentDomain() {
        Unit.SECOND.getSmallerUnit(Unit.BIT);
    }

    @Test
    public void testTemperature() {
        Assert.assertEquals(20.0, Unit.CELCIUS.convert(20.0, Unit.CELCIUS), 0.001);
        Assert.assertEquals(20.0, Unit.CELCIUS.convert(68.0, Unit.FAHRENHEIT), 0.001);
        Assert.assertEquals(20.0, Unit.CELCIUS.convert(293.15, Unit.KELVIN), 0.001);
        Assert.assertEquals(293.15, Unit.KELVIN.convert(293.15, Unit.KELVIN), 0.001);
        Assert.assertEquals(293.15, Unit.KELVIN.convert(20.0, Unit.CELCIUS), 0.001);
        Assert.assertEquals(293.15, Unit.KELVIN.convert(68.0, Unit.FAHRENHEIT), 0.001);
        Assert.assertEquals(68.0, Unit.FAHRENHEIT.convert(68.0, Unit.FAHRENHEIT), 0.001);
        Assert.assertEquals(68.0, Unit.FAHRENHEIT.convert(20.0, Unit.CELCIUS), 0.001);
        Assert.assertEquals(68.0, Unit.FAHRENHEIT.convert(293.15, Unit.KELVIN), 0.001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTemperatureCelciusNotSupported() {
        Unit.CELCIUS.convert(0.0, Unit.BIT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTemperatureFahrenheitNotSupported() {
        Unit.FAHRENHEIT.convert(0.0, Unit.BIT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTemperatureKelvinNotSupported() {
        Unit.KELVIN.convert(0.0, Unit.BIT);
    }

    @Test
    public void testUnitValues() {
        for (final Unit unit : Unit.values()) {
            final String unitAsString = unit.name();
            final Unit actualUnit = Unit.valueOf(unitAsString);
            Assert.assertSame(unit, actualUnit);
        }
    }

    @Test
    public void testUnitTypeValues() {
        for (final Unit.Type type : Unit.Type.values()) {
            final String typeAsString = type.name();
            final Unit.Type actualType = Unit.Type.valueOf(typeAsString);
            Assert.assertSame(type, actualType);
        }
    }
}
