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
    Optional<String> getProperty(String name);

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
    String getProperty(String name, String defaultValue);

    /**
     * Retrieve the value of a particular property by its name and if it does
     * not exist throw a <code>RuntimeException</code>.
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the <code>RuntimeException</code>
     * if the specified property name is not defined.
     */
    String getRequiredProperty(String name) throws NoSuchElementException;

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
    Optional<Boolean> getPropertyAsBoolean(String name);

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
    boolean getPropertyAsBoolean(String name, boolean defaultValue);

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
    boolean getRequiredPropertyAsBoolean(String name) throws NoSuchElementException;

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
    Optional<Integer> getPropertyAsInteger(String name) throws NumberFormatException;

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
    int getPropertyAsInteger(String name, int defaultValue) throws NumberFormatException;

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
    int getRequiredPropertyAsInteger(String name) throws NoSuchElementException, NumberFormatException;

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
    Optional<Long> getPropertyAsLong(String name) throws NumberFormatException;

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
    long getPropertyAsLong(String name, long defaultValue) throws NumberFormatException;

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
    long getRequiredPropertyAsLong(String name) throws NoSuchElementException, NumberFormatException;

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
    Optional<Double> getPropertyAsDouble(String name) throws NumberFormatException;

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
    double getPropertyAsDouble(String name, double defaultValue) throws NumberFormatException;

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
    double getRequiredPropertyAsDouble(String name) throws NoSuchElementException, NumberFormatException;

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
    Optional<Float> getPropertyAsFloat(String name) throws NumberFormatException;

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
    float getPropertyAsFloat(String name, float defaultValue) throws NumberFormatException;

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
    float getRequiredPropertyAsFloat(String name) throws NoSuchElementException, NumberFormatException;

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
    Optional<Short> getPropertyAsShort(String name) throws NumberFormatException;

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
    short getPropertyAsShort(String name, short defaultValue) throws NumberFormatException;

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
    short getRequiredPropertyAsShort(String name) throws NoSuchElementException, NumberFormatException;

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
    <T> Optional<T> getPropertyAs(String name, Class<? extends T> clazz) throws IllegalArgumentException;

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
    <T> T getPropertyAs(String name, Class<? extends T> clazz, T defaultValue) throws IllegalArgumentException;

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
    <T> T getRequiredPropertyAs(String name, Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getAs(Class<? extends T> clazz) throws IllegalArgumentException;

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
    <T> T getAs(Class<? extends T> clazz, T defaultValue) throws IllegalArgumentException;

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
    <T> T getRequiredAs(Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

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
    <T> Optional<T> getPropertyAs(String name, Type type) throws IllegalArgumentException;

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
    <T> T getPropertyAs(String name, Type type, T defaultValue) throws IllegalArgumentException;

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
    <T> T getRequiredPropertyAs(String name, Type type) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of <code>T</code>.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of <code>T</code>.
     */
    <T> Optional<T> getAs(Type type) throws IllegalArgumentException;

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
    <T> T getAs(Type type, T defaultValue) throws IllegalArgumentException;

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
    <T> T getRequiredAs(Type type) throws NoSuchElementException, IllegalArgumentException;
}

