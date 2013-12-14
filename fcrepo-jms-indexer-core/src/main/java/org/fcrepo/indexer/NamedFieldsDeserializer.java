/**
 * Copyright 2013 DuraSpace, Inc.
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

package org.fcrepo.indexer;

import static com.google.common.collect.ImmutableList.builder;
import static com.google.common.collect.Maps.transformValues;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Deserializes JSON maps
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class NamedFieldsDeserializer extends TypeAdapter<NamedFields> {

    private static final Type type = new TypeToken<Collection<Map<String, JsonElement>>>() {}
            .getType();

    private Gson gson;

    //TODO make index-time boost somehow adjustable, or something
    public static final Long INDEX_TIME_BOOST = 1L;

    private static final Logger LOGGER = getLogger(NamedFieldsDeserializer.class);

    private static Function<JsonElement, Collection<String>> jsonElement2list =
        new Function<JsonElement, Collection<String>>() {

            @Override
            public List<String> apply(final JsonElement input) {
                final ImmutableList.Builder<String> b = builder();
                for (final JsonElement value : input.getAsJsonArray()) {
                    b.add(value.getAsString());
                }
                return b.build();
            }
        };

    @Override
    public void write(final JsonWriter out, final NamedFields value)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NamedFields read(final JsonReader in)
        throws IOException {
        try {
            final Collection<Map<String, JsonElement>> fields =
                gson.fromJson(in, type);
            // note: we assume that only one element will exist in
            // fields, because that is the nature of the LDPath machinery
            return new NamedFields(transformValues(fields.iterator().next(), jsonElement2list));
        } catch (final Exception e) {
            LOGGER.error("Failed to parse JSON to Map<String, Collection<String>>!", e);
            throw e;
        }

    }


    /**
     * @param gson the Gson engine to set
     */
    public void setGson(final Gson gson) {
        this.gson = gson;
    }

}
