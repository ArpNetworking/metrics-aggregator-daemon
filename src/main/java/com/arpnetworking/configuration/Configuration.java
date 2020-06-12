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
package com.arpnetworking.configuration;

import java.lang.reflect.Type;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Interface for classes which provide configuration.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public interface Configuration {

    /**
     * Retrieve the value of a particular property by its name.
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
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
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     */
    String getProperty(String name, String defaultValue);

    /**
     * Retrieve the value of a particular property by its name and if it does
     * not exist throw a {@link RuntimeException}.
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     */
    String getRequiredProperty(String name) throws NoSuchElementException;

    /**
     * {@link Boolean} specific accessor. Anything other than "true",
     * ignoring case, is treated as {@code false}.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     */
    Optional<Boolean> getPropertyAsBoolean(String name);

    /**
     * {@link Boolean} specific accessor. Anything other than {@code "true"},
     * ignoring case, is treated as {@code false}.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     */
    boolean getPropertyAsBoolean(String name, boolean defaultValue);

    /**
     * {@link Boolean} specific accessor. Anything other than {@code "true"},
     * ignoring case, is treated as {@code false}.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     */
    boolean getRequiredPropertyAsBoolean(String name) throws NoSuchElementException;

    /**
     * {@link Integer} specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * {@link Integer}.
     */
    Optional<Integer> getPropertyAsInteger(String name) throws NumberFormatException;

    /**
     * {@link Integer} specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * {@link Integer}.
     */
    int getPropertyAsInteger(String name, int defaultValue) throws NumberFormatException;

    /**
     * {@link Integer} specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to an
     * {@link Integer}.
     */
    int getRequiredPropertyAsInteger(String name) throws NoSuchElementException, NumberFormatException;

    /**
     * {@link Long} specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Long}.
     */
    Optional<Long> getPropertyAsLong(String name) throws NumberFormatException;

    /**
     * {@link Long} specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Long}.
     */
    long getPropertyAsLong(String name, long defaultValue) throws NumberFormatException;

    /**
     * {@link Long} specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Long}.
     */
    long getRequiredPropertyAsLong(String name) throws NoSuchElementException, NumberFormatException;

    /**
     * {@link Double} specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Double}.
     */
    Optional<Double> getPropertyAsDouble(String name) throws NumberFormatException;

    /**
     * {@link Double} specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Double}.
     */
    double getPropertyAsDouble(String name, double defaultValue) throws NumberFormatException;

    /**
     * {@link Double} specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Double}.
     */
    double getRequiredPropertyAsDouble(String name) throws NoSuchElementException, NumberFormatException;

    /**
     * {@link Float} specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Float}.
     */
    Optional<Float> getPropertyAsFloat(String name) throws NumberFormatException;

    /**
     * {@link Float} specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Float}.
     */
    float getPropertyAsFloat(String name, float defaultValue) throws NumberFormatException;

    /**
     * {@link Float} specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Float}.
     */
    float getRequiredPropertyAsFloat(String name) throws NoSuchElementException, NumberFormatException;

    /**
     * {@link Short} specific accessor.
     *
     * @see Configuration#getProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Short}.
     */
    Optional<Short> getPropertyAsShort(String name) throws NumberFormatException;

    /**
     * {@link Short} specific accessor.
     *
     * @see Configuration#getProperty(String, String)
     *
     * @param name The name of the property value to retrieve.
     * @param defaultValue The value to return if the specified property name
     * does not exist in the configuration.
     * @return Returns the property value or {@code defaultValue} if
     * the property name has not been defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Short}.
     */
    short getPropertyAsShort(String name, short defaultValue) throws NumberFormatException;

    /**
     * {@link Short} specific accessor.
     *
     * @see Configuration#getRequiredProperty(String)
     *
     * @param name The name of the property value to retrieve.
     * @return Returns the property value.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws NumberFormatException if the value cannot be converted to a
     * {@link Short}.
     */
    short getRequiredPropertyAsShort(String name) throws NoSuchElementException, NumberFormatException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param clazz The type of the object to instantiate.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
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
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getPropertyAs(String name, Class<? extends T> clazz, T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param clazz The type of the object to instantiate.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@link T}.
     */
    <T> T getRequiredPropertyAs(String name, Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
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
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getAs(Class<? extends T> clazz, T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param clazz The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the configuration is not available.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getRequiredAs(Class<? extends T> clazz) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param type The type of the object to instantiate.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
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
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getPropertyAs(String name, Type type, T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param name The name of the property value to retrieve.
     * @param type The type of the object to instantiate.
     * @return Returns the property value or {@link Optional#empty()} if
     * the property name has not been defined.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the specified property name is not defined.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getRequiredPropertyAs(String name, Type type) throws NoSuchElementException, IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
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
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getAs(Type type, T defaultValue) throws IllegalArgumentException;

    /**
     * Generic object accessor.
     *
     * @param <T> The type to return.
     * @param type The type of the object to instantiate.
     * @return Returns the entire configuration as an instance of {@code T}.
     * @throws NoSuchElementException throws the {@link RuntimeException}
     * if the configuration is not available.
     * @throws IllegalArgumentException if the value cannot be converted to an
     * instance of {@code T}.
     */
    <T> T getRequiredAs(Type type) throws NoSuchElementException, IllegalArgumentException;
}
