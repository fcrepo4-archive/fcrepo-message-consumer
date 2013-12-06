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

package org.fcrepo.indexer.solr;

import static com.google.common.collect.Maps.transformEntries;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;

import com.google.common.collect.Maps.EntryTransformer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Translates JSON maps to {@link SolrInputDocument}s
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class SolrInputDocumentDeserializer extends
        TypeAdapter<SolrInputDocument> {

    private static final Type type = new TypeToken<Collection<Map<String, JsonElement>>>() {}
            .getType();

    private Gson gson;

    //TODO make index-time boost somehow adjustable, or something
    public static final Long INDEX_TIME_BOOST = 1L;

    private static final Logger LOGGER = getLogger(SolrInputDocumentDeserializer.class);

    private static EntryTransformer<String, JsonElement, SolrInputField> jsonElement2solrInputField =
        new EntryTransformer<String, JsonElement, SolrInputField>() {

            @Override
            public SolrInputField transformEntry(final String key,
                final JsonElement input) {
                final SolrInputField field = new SolrInputField(key);
                for (final JsonElement value : input.getAsJsonArray()) {
                    field.addValue(value.getAsString(), INDEX_TIME_BOOST);
                }
                return field;
            }
        };

    @Override
    public void write(final JsonWriter out, final SolrInputDocument value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SolrInputDocument read(final JsonReader in) throws IOException {
        try {
            final Collection<Map<String, JsonElement>> fields =
                gson.fromJson(in, type);
            return new SolrInputDocument(transformEntries(fields.iterator()
                    .next(), jsonElement2solrInputField));
        } catch (final Exception e) {
            LOGGER.error("Failed to parse JSON to Solr update document!", e);
            throw e;
        }

    }


    /**
     * @param gson the Gson engine to use
     */
    public void setGson(final Gson gson) {
        this.gson = gson;
    }

}
