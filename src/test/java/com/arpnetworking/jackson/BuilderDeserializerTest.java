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
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Objects;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Tests for the <code>BuilderDeserializer</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class BuilderDeserializerTest {

    @Test
    public void testWithNull() throws IOException {
        final TestFooBeanInterface expectedBean = TestFooBeanImpl.Builder.newInstance()
                .setFieldString(UUID.randomUUID().toString())
                .setFieldBoolean(Boolean.TRUE)
                .setFieldPrimitive(RANDOM_GENERATOR.nextInt())
                .setFieldBooleanPrimitive(false)
                .setFieldNull(null)
                .build();

        final String jsonString =
                "{\"fieldString\":\"" + expectedBean.getFieldString() + "\""
                        + ", \"fieldBoolean\":" + expectedBean.isFieldBoolean()
                        + ", \"fieldPrimitive\":" + expectedBean.getFieldPrimitive()
                        + ", \"fieldBooleanPrimitive\":" + expectedBean.isFieldBooleanPrimitive()
                        + ", \"fieldNull\":" + expectedBean.getFieldNull()
                        + "}";

        final JsonFactory jsonFactory = new JsonFactory(ObjectMapperFactory.createInstance());
        try (final JsonParser jsonParser = jsonFactory.createParser(jsonString)) {
            final JsonDeserializer<? extends TestFooBeanInterface> deserializer = BuilderDeserializer.of(TestFooBeanImpl.Builder.class);
            final TestFooBeanInterface actualBean = deserializer.deserialize(jsonParser, null);
            Assert.assertEquals(expectedBean, actualBean);
        }
    }

    @Test
    public void testWithoutNull() throws IOException {
        final TestFooBeanInterface expectedBean = TestFooBeanImpl.Builder.newInstance()
                .setFieldString(UUID.randomUUID().toString())
                .setFieldBoolean(Boolean.TRUE)
                .setFieldPrimitive(RANDOM_GENERATOR.nextInt())
                .setFieldBooleanPrimitive(false)
                .build();

        final String jsonString =
                "{\"fieldString\":\"" + expectedBean.getFieldString() + "\""
                        + ", \"fieldBoolean\":" + expectedBean.isFieldBoolean()
                        + ", \"fieldPrimitive\":" + expectedBean.getFieldPrimitive()
                        + ", \"fieldBooleanPrimitive\":" + expectedBean.isFieldBooleanPrimitive()
                        + "}";

        final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());
        try (final JsonParser jsonParser = jsonFactory.createParser(jsonString)) {
            final JsonDeserializer<? extends TestFooBeanInterface> deserializer = BuilderDeserializer.of(TestFooBeanImpl.Builder.class);
            final TestFooBeanInterface actualBean = deserializer.deserialize(jsonParser, null);
            Assert.assertEquals(expectedBean, actualBean);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAddTo() {
        final SimpleModule module = Mockito.mock(SimpleModule.class);
        BuilderDeserializer.addTo(module, TestFooBeanImpl.class);
        Mockito.verify(module).addDeserializer(
                TestFooBeanImpl.class,
                (JsonDeserializer<TestFooBeanImpl>) BuilderDeserializer.of(TestFooBeanImpl.Builder.class));
        Mockito.verify(module).addDeserializer(
                TestBarBeanImpl.class,
                BuilderDeserializer.of(TestBarBeanImpl.Builder.class));
    }

    @Test
    public void testAddToJsonSubTypes() {
        final SimpleModule module = Mockito.mock(SimpleModule.class);
        BuilderDeserializer.addTo(module, TestJsonSubTypesInterface.class);
        Mockito.verify(module).addDeserializer(
                TestJsonSubTypesClassA.class,
                BuilderDeserializer.of(TestJsonSubTypesClassA.Builder.class));
        Mockito.verify(module).addDeserializer(
                TestJsonSubTypesClassB.class,
                BuilderDeserializer.of(TestJsonSubTypesClassB.Builder.class));
    }

    @Test
    public void testEquals() {
        final Builder<?> builderA = Mockito.mock(Builder.class, "BuilderA");

        @SuppressWarnings("unchecked")
        final BuilderDeserializer<? extends Object> builderDeserializerA = BuilderDeserializer.of(
                (Class<? extends Builder<Object>>) builderA.getClass());

        final BuilderDeserializer<? extends Object> builderDeserializerB1 = BuilderDeserializer.of(TestFooBeanImpl.Builder.class);
        final BuilderDeserializer<? extends Object> builderDeserializerB2 = BuilderDeserializer.of(TestFooBeanImpl.Builder.class);

        Assert.assertTrue(builderDeserializerA.equals(builderDeserializerA));
        Assert.assertTrue(builderDeserializerB1.equals(builderDeserializerB2));
        Assert.assertFalse(builderDeserializerA.equals(null));
        Assert.assertFalse(builderDeserializerA.equals("This is a string"));
        Assert.assertFalse(builderDeserializerA.equals(builderDeserializerB1));
    }

    @Test
    public void testHashCode() {
        final BuilderDeserializer<? extends TestFooBeanInterface> builderDeserializerA =
                BuilderDeserializer.of(TestFooBeanImpl.Builder.class);
        final BuilderDeserializer<? extends TestBarBeanImpl> builderDeserializerB =
                BuilderDeserializer.of(TestBarBeanImpl.Builder.class);

        Assert.assertTrue(builderDeserializerA.hashCode() != builderDeserializerB.hashCode());
    }

    @Test
    public void testBuilderForClass() throws ClassNotFoundException {
        Assert.assertEquals(TestFooBeanImpl.Builder.class, BuilderDeserializer.getBuilderForClass(TestFooBeanImpl.class));
    }

    @Test(expected = ClassNotFoundException.class)
    public void testBuilderForClassDoesNotExist() throws ClassNotFoundException {
        BuilderDeserializer.getBuilderForClass(String.class);
    }

    @Test
    public void testIsSetterMethod() throws NoSuchMethodException, SecurityException {
        // Happy case
        Assert.assertTrue(BuilderDeserializer.isSetterMethod(
                TestFooBeanImpl.Builder.class,
                TestFooBeanImpl.Builder.class.getMethod("setFieldNull", Object.class)));

        // Incorrect prefix
        Assert.assertFalse(BuilderDeserializer.isSetterMethod(
                TestFooBeanImpl.Builder.class,
                TestFooBeanImpl.Builder.class.getMethod("fooNotSetter1", Object.class)));

        // Incorrect return type
        Assert.assertFalse(BuilderDeserializer.isSetterMethod(
                TestFooBeanImpl.Builder.class,
                TestFooBeanImpl.Builder.class.getMethod("setNotSetter2", Object.class)));

        // Incorrect var-args
        Assert.assertFalse(BuilderDeserializer.isSetterMethod(
                TestFooBeanImpl.Builder.class,
                TestFooBeanImpl.Builder.class.getMethod("setNotSetter3", Object[].class)));

        // Incorrect no args
        Assert.assertFalse(BuilderDeserializer.isSetterMethod(
                TestFooBeanImpl.Builder.class,
                TestFooBeanImpl.Builder.class.getMethod("setNotSetter4")));
    }

    private static final Random RANDOM_GENERATOR = new Random();

    private interface TestFooBeanInterface {
        Object getFieldNull();

        String getFieldString();

        Boolean isFieldBoolean();

        int getFieldPrimitive();

        boolean isFieldBooleanPrimitive();

        TestBarBeanImpl getFieldBarBean();
    }

    private static final class TestFooBeanImpl implements TestFooBeanInterface {

        @Override
        public Object getFieldNull() {
            return _fieldNull;
        }

        @Override
        public String getFieldString() {
            return _fieldString;
        }

        @Override
        public Boolean isFieldBoolean() {
            return _fieldBoolean;
        }

        @Override
        public int getFieldPrimitive() {
            return _fieldPrimitive;
        }

        @Override
        public boolean isFieldBooleanPrimitive() {
            return _fieldBooleanPrimitive;
        }

        @Override
        public TestBarBeanImpl getFieldBarBean() {
            return _fieldBarBean;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final TestFooBeanImpl other = (TestFooBeanImpl) object;

            return Objects.equal(_fieldNull, other._fieldNull)
                    && Objects.equal(_fieldString, other._fieldString)
                    && Objects.equal(_fieldBoolean, other._fieldBoolean)
                    && _fieldPrimitive == other._fieldPrimitive
                    && _fieldBooleanPrimitive == other._fieldBooleanPrimitive
                    && Objects.equal(_fieldBarBean, other._fieldBarBean);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(
                    _fieldNull,
                    _fieldString,
                    _fieldBoolean,
                    _fieldPrimitive,
                    _fieldBooleanPrimitive,
                    _fieldBarBean);
        }

        private TestFooBeanImpl(final Builder builder) {
            _fieldNull = builder._fieldNull;
            _fieldString = builder._fieldString;
            _fieldBoolean = builder._fieldBoolean;
            _fieldPrimitive = builder._fieldPrimitive;
            _fieldBooleanPrimitive = builder._fieldBooleanPrimitive;
            _fieldBarBean = builder._fieldBarBean;
        }

        private final Object _fieldNull;
        private final String _fieldString;
        private final Boolean _fieldBoolean;
        private final int _fieldPrimitive;
        private final boolean _fieldBooleanPrimitive;
        private final TestBarBeanImpl _fieldBarBean;

        public static class Builder implements com.arpnetworking.commons.builder.Builder<TestFooBeanInterface> {

            public static Builder newInstance() {
                return new Builder();
            }

            public Builder setFieldNull(final Object value) {
                _fieldNull = value;
                return this;
            }

            public Builder setFieldString(final String value) {
                _fieldString = value;
                return this;
            }

            public Builder setFieldBoolean(final Boolean value) {
                _fieldBoolean = value;
                return this;
            }

            public Builder setFieldPrimitive(final int value) {
                _fieldPrimitive = value;
                return this;
            }

            public Builder setFieldBooleanPrimitive(final boolean value) {
                _fieldBooleanPrimitive = value;
                return this;
            }

            @SuppressWarnings("unused")
            public Builder setFieldBar(final TestBarBeanImpl value) {
                _fieldBarBean = value;
                return this;
            }

            @Override
            public TestFooBeanInterface build() {
                return new TestFooBeanImpl(this);
            }

            @SuppressWarnings("unused")
            public Builder fooNotSetter1(final Object value) {
                return this;
            }

            @SuppressWarnings("unused")
            public void setNotSetter2(final Object value) {}

            @SuppressWarnings("unused")
            public Builder setNotSetter3(final Object... values) {
                return this;
            }

            @SuppressWarnings("unused")
            public Builder setNotSetter4() {
                return this;
            }

            private Object _fieldNull;
            private String _fieldString;
            private Boolean _fieldBoolean;
            private int _fieldPrimitive;
            private boolean _fieldBooleanPrimitive;
            private TestBarBeanImpl _fieldBarBean;
        }
    }

    private static final class TestBarBeanImpl {

        @SuppressWarnings("unused")
        public TestBarBeanImpl getFieldBarBean() {
            return _fieldBarBean;
        }

        @SuppressWarnings("unused")
        public List<String> getFieldListString() {
            return _fieldListString;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final TestBarBeanImpl other = (TestBarBeanImpl) object;

            return Objects.equal(_fieldBarBean, other._fieldBarBean)
                    && Objects.equal(_fieldListString, other._fieldListString);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(
                    _fieldBarBean,
                    _fieldListString);
        }

        private TestBarBeanImpl(final Builder builder) {
            _fieldBarBean = builder._fieldBarBean;
            _fieldListString = builder._fieldListString;
        }

        private final TestBarBeanImpl _fieldBarBean;
        private final List<String> _fieldListString;

        private static final class Builder implements com.arpnetworking.commons.builder.Builder<TestBarBeanImpl> {

            @SuppressWarnings("unused")
            private Builder() {}

            @SuppressWarnings("unused")
            public Builder setFieldBar(final TestBarBeanImpl value) {
                _fieldBarBean = value;
                return this;
            }

            @SuppressWarnings("unused")
            public Builder setFieldListStrings(final List<String> value) {
                _fieldListString = value;
                return this;
            }

            @Override
            public TestBarBeanImpl build() {
                return new TestBarBeanImpl(this);
            }

            private TestBarBeanImpl _fieldBarBean;
            private List<String> _fieldListString;
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
        @Type(value = TestJsonSubTypesClassA.class, name = "foo"),
        @Type(value = TestJsonSubTypesClassB.class, name = "bar")
    })
    private interface TestJsonSubTypesInterface {

    }

    private static final class TestJsonSubTypesClassA implements TestJsonSubTypesInterface {
        private static class Builder implements com.arpnetworking.commons.builder.Builder<TestJsonSubTypesClassA> {
            @Override
            public TestJsonSubTypesClassA build() {
                return null;
            }
        }
    }

    private static final class TestJsonSubTypesClassB implements TestJsonSubTypesInterface {
        private static class Builder implements com.arpnetworking.commons.builder.Builder<TestJsonSubTypesClassB> {
            @Override
            public TestJsonSubTypesClassB build() {
                return null;
            }
        }
    }

}
