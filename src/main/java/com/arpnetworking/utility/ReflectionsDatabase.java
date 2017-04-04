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
package com.arpnetworking.utility;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Default implementation of <code>InterfaceDatabase</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class ReflectionsDatabase implements InterfaceDatabase, AnnotationDatabase {

    /**
     * Create a new instance of <code>InterfaceDatabase</code>.
     *
     * @return New instance of <code>InterfaceDatabase</code>.
     */
    public static ReflectionsDatabase newInstance() {
        return new ReflectionsDatabase(REFLECTIONS);
    }

    @Override
    public Set<Class<?>> findClassesWithAnnotation(final Class<? extends Annotation> annotationClass) {
        if (!annotationClass.isAnnotation()) {
            throw new IllegalArgumentException(String.format("Class must be an annotation; class=%s", annotationClass));
        }
        return _reflections.getTypesAnnotatedWith(annotationClass);
    }

    @Override
    public <T> Set<Class<? extends T>> findClassesWithInterface(final Class<T> interfaceClass) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException(String.format("Class must be an interface; class=%s", interfaceClass));
        }
        return _reflections.getSubTypesOf(interfaceClass);
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("reflections", _reflections)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    // NOTE: Package private for testing
    /* package private */ReflectionsDatabase(final Reflections reflections) {
        _reflections = reflections;
    }

    private final Reflections _reflections;

    private static final Reflections REFLECTIONS = new Reflections("com.arpnetworking");
}
