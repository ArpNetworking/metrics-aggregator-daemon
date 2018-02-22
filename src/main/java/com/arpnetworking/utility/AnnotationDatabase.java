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

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * This class provides searchable access to runtime annotations.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface AnnotationDatabase {

    /**
     * Retrieve the <code>Set</code> of classes with the declared annotation.
     *
     * @param annotationClass The annotation class to search for.
     * @return The <code>Set</code> of classes that are declared with the
     * specified annotation.
     */
    Set<Class<?>> findClassesWithAnnotation(Class<? extends Annotation> annotationClass);
}
