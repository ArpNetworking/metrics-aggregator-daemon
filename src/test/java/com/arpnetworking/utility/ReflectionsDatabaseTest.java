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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;

/**
 * Tests for the ReflectionsDatabase class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class ReflectionsDatabaseTest {

    @Test
    public void testFindClassesWithAnnotation() {
        final AnnotationDatabase db = ReflectionsDatabase.newInstance();
        final Set<Class<?>> classes = db.findClassesWithAnnotation(TestUsedAnnotation.class);
        Assert.assertNotNull(classes);
        Assert.assertTrue(classes.contains(TestClassWithAnnotation.class));
    }

    @Test
    public void testFindClassesWithAnnotationNoMatches() {
        final AnnotationDatabase db = ReflectionsDatabase.newInstance();
        final Set<Class<?>> classes = db.findClassesWithAnnotation(TestUnusedAnnotation.class);
        Assert.assertNotNull(classes);
        Assert.assertTrue(classes.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressFBWarnings(value = "DM_NEW_FOR_GETCLASS", justification = "Need a new class")
    public void testFindClassesWithAnnotationNonAnnotation() {
        final Class<? extends Annotation> notAnAnnotation = ((Annotation) () -> null).getClass();
        final AnnotationDatabase db = ReflectionsDatabase.newInstance();
        db.findClassesWithAnnotation(notAnAnnotation);
    }

    @Test
    public void testFindClassesWithInterface() {
        final InterfaceDatabase db = ReflectionsDatabase.newInstance();
        final Set<Class<? extends TestInterface>> classes = db.findClassesWithInterface(TestInterface.class);
        Assert.assertNotNull(classes);
        Assert.assertEquals(2, classes.size());
        Assert.assertTrue(classes.contains(TestClassImplementsInterface.class));
        Assert.assertTrue(classes.contains(TestSubClassClass.class));
    }

    @Test
    public void testFindClassesWithInterfaceNoMatches() {
        final InterfaceDatabase db = ReflectionsDatabase.newInstance();
        final Set<Class<? extends TestUnusedInterface>> classes = db.findClassesWithInterface(TestUnusedInterface.class);
        Assert.assertNotNull(classes);
        Assert.assertTrue(classes.isEmpty());
    }

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface TestUsedAnnotation {}

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface TestUnusedAnnotation {}

    @TestUsedAnnotation
    private static final class TestClassWithAnnotation {}

    private interface TestInterface {}

    private interface TestUnusedInterface {}

    private static class TestClassImplementsInterface implements TestInterface {}

    private static class TestSubClassClass extends TestClassImplementsInterface {}

}
