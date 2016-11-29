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
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Json based configuration sourced by merging zero or more <code>JsonSource</code>
 * instances together.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class JsonNodeMergingSource implements JsonNodeSource {

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getValue(final String... keys) {
        return BaseJsonNodeSource.getValue(getJsonNode(), keys);
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("mergedNode", _mergedNode)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private static JsonNode merge(final JsonNode target, final JsonNode source) {
        if (target instanceof ArrayNode && source instanceof ArrayNode) {
            // Both the target and source are array nodes, then append the
            // contents of the source array to the target array without
            // performing a deep copy. This is safe because either:
            // 1) Successive merges are also arrays and are appended.
            // 2) Successive merge(s) are not arrays and replace the array.
            // Note: this assumes that the target node is modifiable.
            ((ArrayNode) target).addAll((ArrayNode) source);
            return target;
        } else if (target instanceof ObjectNode && source instanceof ObjectNode) {
            // Both the target and source are object nodes, then recursively
            // merge the fields of the source node over the same fields in
            // the target node. Any unmatched fields from the source node are
            // simply added to the target node; this requires a deep copy
            // since subsequent merges may modify it.
            final Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
            while (iterator.hasNext()) {
                final Map.Entry<String, JsonNode> sourceFieldEntry = iterator.next();
                final JsonNode targetFieldValue = target.get(sourceFieldEntry.getKey());
                if (targetFieldValue != null) {
                    // Recursively merge the source field value into the target
                    // field value and replace the target value with the result.
                    final JsonNode newTargetFieldValue = merge(targetFieldValue, sourceFieldEntry.getValue());
                    ((ObjectNode) target).set(sourceFieldEntry.getKey(), newTargetFieldValue);
                } else {
                    // Add a deep copy of the source field to the target.
                    ((ObjectNode) target).set(sourceFieldEntry.getKey(), sourceFieldEntry.getValue().deepCopy());
                }
            }
            return target;
        } else {
            // The target and source nodes are of different types. Replace the
            // target node with the source node. This requires a deep copy of
            // the source node since subsequent merges may modify it.
            return source.deepCopy();
        }
    }

    /* package private */ Optional<JsonNode> getJsonNode() {
        return _mergedNode;
    }

    private JsonNodeMergingSource(final Builder builder) {
        Optional<JsonNode> mergedNode = Optional.empty();
        for (final JsonNodeSource source : builder._sources) {
            final Optional<JsonNode> sourceNode = source.getValue();
            if (sourceNode.isPresent()) {
                if (mergedNode.isPresent()) {
                    mergedNode = Optional.of(merge(mergedNode.get(), sourceNode.get()));
                } else {
                    mergedNode = Optional.of(sourceNode.get().deepCopy());
                }
            }
        }
        _mergedNode = mergedNode;
    }

    private final Optional<JsonNode> _mergedNode;

    /**
     * Builder for <code>JsonNodeMergingSource</code>.
     */
    public static final class Builder extends OvalBuilder<JsonNodeMergingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(JsonNodeMergingSource::new);
        }

        /**
         * Set the <code>List</code> of <code>JsonSource</code> instances in
         * order of importance (least significant first). Cannot be null.
         *
         * @param value The <code>List</code> of <code>JsonSource</code>
         * instances in order of importance (least significant first).
         * @return This <code>Builder</code> instance.
         */
        public Builder setSources(final List<JsonNodeSource> value) {
            _sources = Lists.newArrayList(value);
            return this;
        }

        /**
         * Add a <code>JsonSource</code> instance.
         *
         * @param value The <code>JsonSource</code> instance.
         * @return This <code>Builder</code> instance.
         */
        public Builder addSource(final JsonNodeSource value) {
            if (_sources == null) {
                _sources = Lists.newArrayList(value);
            } else {
                _sources.add(value);
            }
            return this;
        }

        @NotNull
        private List<JsonNodeSource> _sources = Lists.newArrayList();
    }
}
