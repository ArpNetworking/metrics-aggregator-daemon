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
package com.arpnetworking.jackson;

import com.arpnetworking.commons.builder.Builder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * Deserialize JSON into an instance of a specified type <code>T</code> given a
 * builder that creates instances of that type (or a subtype). In order to use
 * this deserializer with an <code>ObjectMapper</code> be sure to register the
 * transitive closure of builder deserializers (e.g. type-builder pairs) that
 * are required to deserialize an instance of the root type.
 *
 * TODO(vkoskela): This is _duplicated_ in metrics-portal and should find its way to a common utility package.
 *
 * @param <T> The type this deserializer supports.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public final class BuilderDeserializer<T> extends JsonDeserializer<T> {

    /**
     * Static factory method.
     *
     * @param <S> The type this deserializer supports.
     * @param builderClass The builder class for the supported type.
     * @return Instance of <code>BuilderDeserializer</code>.
     */
    public static <S> BuilderDeserializer<? extends S> of(final Class<? extends Builder<? extends S>> builderClass) {
        // TODO(vkoskela): Add caching to avoid unnecessary instances [MAI-114]
        return new BuilderDeserializer<S>(builderClass);
    }

    /**
     * Register builder deserializers for the the transitive closure of builders
     * anchored by the target class in the provided <code>SimpleModule</code>.
     *
     * @param targetModule The <code>SimpleModule</code> to register in.
     * @param targetClass The root of the type tree to traverse.
     */
    public static void addTo(final SimpleModule targetModule, final Class<?> targetClass) {
        final Map<Class<?>, JsonDeserializer<?>> typeToDeserializer = Maps.newHashMap();
        addTo(Sets.<Type>newHashSet(), typeToDeserializer, targetClass);
        for (final Map.Entry<Class<?>, JsonDeserializer<?>> entry : typeToDeserializer.entrySet()) {
            @SuppressWarnings("unchecked")
            final Class<Object> clazz = (Class<Object>) entry.getKey();
            targetModule.addDeserializer(clazz, entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        final Builder<? extends T> builder = parser.readValueAs(_builderClass);
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        final BuilderDeserializer<?> other = (BuilderDeserializer<?>) object;

        return Objects.equal(_builderClass, other._builderClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(_builderClass);
    }

    private static void addTo(final Set<Type> visited, final Map<Class<?>, JsonDeserializer<?>> deserializerMap, final Type targetType) {
        if (visited.contains(targetType)) {
            return;
        }
        visited.add(targetType);

        if (targetType instanceof Class<?>) {
            @SuppressWarnings("unchecked")
            final Class<Object> targetClass = (Class<Object>) targetType;
            try {
                // Look-up and register the builder for this class
                final Class<? extends Builder<? extends Object>> builderClass = getBuilderForClass(targetClass);
                deserializerMap.put(targetClass, BuilderDeserializer.of(builderClass));

                LOGGER.trace()
                        .setMessage("Registered builder for class")
                        .addData("targetClass", targetClass)
                        .addData("targetClassBuilder", builderClass)
                        .log();

                // Process all setters on the builder
                for (final Method method : builderClass.getMethods()) {
                    if (isSetterMethod(builderClass, method)) {
                        final Type setterArgumentType = method.getGenericParameterTypes()[0];
                        // Recursively register builders for each setter's type
                        addTo(visited, deserializerMap, setterArgumentType);
                    }
                }
            } catch (final ClassNotFoundException e) {
                // Log that the class is not build-able
                LOGGER.trace()
                        .setMessage("Ignoring class without builder")
                        .addData("targetClass", targetClass)
                        .log();
            }

            // Support for JsonSubTypes annotation
            if (targetClass.isAnnotationPresent(JsonSubTypes.class)) {
                final JsonSubTypes.Type[] subTypes = targetClass.getAnnotation(JsonSubTypes.class).value();
                for (final JsonSubTypes.Type subType : subTypes) {
                    addTo(visited, deserializerMap, subType.value());
                }
            }

            // Support for JsonTypeInfo annotation
            // TODO(vkoskela): Support JsonTypeInfo classpath scan [MAI-116]
        }

        if (targetType instanceof ParameterizedType) {
            // Recursively register builders for each parameterized type
            final ParameterizedType targetParameterizedType = (ParameterizedType) targetType;
            final Type rawType = targetParameterizedType.getRawType();
            addTo(visited, deserializerMap, rawType);
            for (final Type argumentActualType : targetParameterizedType.getActualTypeArguments()) {
                addTo(visited, deserializerMap, argumentActualType);
            }
        }
    }

    // NOTE: Package private for testing.
    @SuppressWarnings("unchecked")
    static <T> Class<? extends Builder<? extends T>> getBuilderForClass(final Class<? extends T> clazz)
            throws ClassNotFoundException {
        return (Class<? extends Builder<? extends T>>) (Class.forName(
                clazz.getName() + "$Builder",
                true, // initialize
                clazz.getClassLoader()));
    }

    // NOTE: Package private for testing.
    static boolean isSetterMethod(final Class<?> builderClass, final Method method) {
        return method.getName().startsWith(SETTER_PREFIX)
                &&
                builderClass.equals(method.getReturnType())
                &&
                !method.isVarArgs()
                &&
                method.getParameterTypes().length == 1;
    }

    private BuilderDeserializer(final Class<? extends Builder<? extends T>> builderClass) {
        _builderClass = builderClass;
    }

    private final Class<? extends Builder<? extends T>> _builderClass;

    private static final String SETTER_PREFIX = "set";
    private static final Logger LOGGER = LoggerFactory.getLogger(BuilderDeserializer.class);
}
