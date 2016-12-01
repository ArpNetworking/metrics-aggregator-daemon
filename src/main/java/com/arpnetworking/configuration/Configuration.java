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
package com.arpnetworking.configuration;

import java.lang.reflect.Type;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Interface for classes which provide configuration.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface Configuration {

    /**
     * Retrieve the value of a particular property by its name.
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     */
    Optional<String> getProperty(final String name);

    /**
     * Retrieve the value of a particular property by its name and if it does
     * not exist return the specified default.
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     */
    String getProperty(final String name, final String defaultValue);

    /**
     * Retrieve the value of a particular property by its name and if it does
     * not exist throw a <code>RuntimeException</code>.
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     */
    String getRequiredProperty(final String name) throws NoSuchElementException;

    /**
     * <code>Boolean</code> specific accessor. Anything other than "true",
     * ignoring case, is treated as <code>false</code>.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     */
    Optional<Boolean> getPropertyAsBoolean(final String name);

    /**
     * <code>Boolean</code> specific accessor. Anything other than "true",
     * ignoring case, is treated as <code>false</code>.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     */
    boolean getPropertyAsBoolean(final String name, final boolean defaultValue);

    /**
     * <code>Boolean</code> specific accessor. Anything other than "true",
     * ignoring case, is treated as <code>false</code>.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     */
    boolean getRequiredPropertyAsBoolean(final String name) throws NoSuchElementException;

    /**
     * <code>Integer</code> specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * <code>Integer</code>.
     */
    Optional<Integer> getPropertyAsInteger(final String name) throws NumberFormatException;

    /**
     * <code>Integer</code> specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * <code>Integer</code>.
     */
    int getPropertyAsInteger(final String name, final int defaultValue) throws NumberFormatException;

    /**
     * <code>Integer</code> specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * <code>Integer</code>.
     */
    int getRequiredPropertyAsInteger(final String name) throws NoSuchElementException, NumberFormatException;

    /**
     * <code>Long</code> specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Long</code>.
     */
    Optional<Long> getPropertyAsLong(final String name) throws NumberFormatException;

    /**
     * <code>Long</code> specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Long</code>.
     */
    long getPropertyAsLong(final String name, final long defaultValue) throws NumberFormatException;

    /**
     * <code>Long</code> specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Long</code>.
     */
    long getRequiredPropertyAsLong(final String name) throws NoSuchElementException, NumberFormatException;

    /**
     * <code>Double</code> specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Double</code>.
     */
    Optional<Double> getPropertyAsDouble(final String name) throws NumberFormatException;

    /**
     * <code>Double</code> specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Double</code>.
     */
    double getPropertyAsDouble(final String name, final double defaultValue) throws NumberFormatException;

    /**
     * <code>Double</code> specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Double</code>.
     */
    double getRequiredPropertyAsDouble(final String name) throws NoSuchElementException, NumberFormatException;

    /**
     * <code>Float</code> specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Float</code>.
     */
    Optional<Float> getPropertyAsFloat(final String name) throws NumberFormatException;

    /**
     * <code>Float</code> specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Float</code>.
     */
    float getPropertyAsFloat(final String name, final float defaultValue) throws NumberFormatException;

    /**
     * <code>Float</code> specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Float</code>.
     */
    float getRequiredPropertyAsFloat(final String name) throws NoSuchElementException, NumberFormatException;

    /**
     * <code>Short</code> specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Short</code>.
     */
    Optional<Short> getPropertyAsShort(final String name) throws NumberFormatException;

    /**
     * <code>Short</code> specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>defaultValue</code> if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Short</code>.
     */
    short getPropertyAsShort(final String name, final short defaultValue) throws NumberFormatException;

    /**
     * <code>Short</code> specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * <code>Short</code>.
     */
    short getRequiredPropertyAsShort(final String name) throws NoSuchElementException, NumberFormatException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param clazz The type of the object to instantiate.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getPropertyAs(final String name, final Class<? extends T> clazz) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param clazz The type of the object to instantiate.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getPropertyAs(final String name, final Class<? extends T> clazz, final T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param clazz The type of the object to instantiate.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getRequiredPropertyAs(final String name, final Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getAs(final Class<? extends T> clazz) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getAs(final Class<? extends T> clazz, final T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the configuration is not available.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getRequiredAs(final Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param type The type of the object to instantiate.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getPropertyAs(final String name, final Type type) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param type The type of the object to instantiate.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getPropertyAs(final String name, final Type type, final T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param type The type of the object to instantiate.
     * @return Returns the property value or <code>Optional.absent</code> if
     * the property name has not been defined.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getRequiredPropertyAs(final String name, final Type type) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getAs(final Type type) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getAs(final Type type, final T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the configuration is not available.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> T getRequiredAs(final Type type) throws NoSuchElementException, IllegalArgumentException;
}

