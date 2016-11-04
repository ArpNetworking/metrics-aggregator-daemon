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
package com.arpnetworking.configuration.jackson;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.configuration.triggers.DirectoryTrigger;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of <code>DynamicConfigurationFactory</code> which maps keys
 * to file names in a directory.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class DirectoryDynamicConfigurationFactory implements DynamicConfigurationFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public DynamicConfiguration create(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys) {
        update(builder, keys);
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(
            final DynamicConfiguration.Builder builder,
            final Collection<Key> keys) {
        // TODO(vkoskela): The source and trigger need to be paired [AINT-553]
        // -> At the moment any trigger will cause all sources to reload which is
        // not as bad as it sounds since all the sources need to be reloaded in
        // order to merge them properly. Of course this could be changed by keeping
        // the unmerged data around.
        final Collection<String> fileNames = keys.stream().map(this::keyToFileName).collect(Collectors.toList());
        for (final File directory : _directories) {
            builder
                    .addSourceBuilder(
                            new JsonNodeDirectorySource.Builder()
                                    .setDirectory(directory)
                                    .setFileNames(fileNames))
                    .addTrigger(
                            new DirectoryTrigger.Builder()
                                    .setDirectory(directory)
                                    .setFileNames(fileNames)
                                    .build());
        }
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("directories", _directories)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Convert the <code>Key</code> to a file name.
     *
     * @param key The <code>Key</code> to convert.
     * @return The corresponding file name.
     */
    protected String keyToFileName(final Key key) {
        return key.getParts().stream().collect(Collectors.joining("."));
    }

    private DirectoryDynamicConfigurationFactory(final Builder builder) {
        _directories = Lists.newArrayList(builder._directories);
    }

    private final List<File> _directories;

    /**
     * <code>Builder</code> implementation for <code>DirectoryDynamicConfigurationFactory</code>.
     */
    public static final class Builder extends OvalBuilder<DirectoryDynamicConfigurationFactory> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DirectoryDynamicConfigurationFactory::new);
        }

        /**
         * Set the directories.
         *
         * @param value The directories.
         * @return This <code>Builder</code> instance.
         */
        public Builder setDirectories(final List<File> value) {
            _directories = value;
            return this;
        }

        private List<File> _directories;
    }
}
