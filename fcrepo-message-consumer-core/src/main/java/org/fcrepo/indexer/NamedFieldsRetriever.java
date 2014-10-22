/**
 * Copyright 2014 DuraSpace, Inc.
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

import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.http.HttpStatus.SC_OK;
import static org.fcrepo.indexer.IndexerGroup.INDEXING_TRANSFORM_PREDICATE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * Retrieves resources transformed into sets of named fields via LDPath.
 * For use with indexers like Solr.
 *
 * @author ajs6f
 * @since Dec 6, 2013
 */
public class NamedFieldsRetriever implements Supplier<NamedFields> {

    private final String uri;

    private final HttpClient httpClient;

    private final Supplier<Model> rdfr;

    private Gson gson;

    private static final Type typeToken = new TypeToken<NamedFields>() {}
            .getType();

    private static final Logger LOGGER = getLogger(NamedFieldsRetriever.class);

    /**
     * @param uri
     * @param client
     * @param rdfr Used to determine the transform to use with this indexing
     *        step
     */
    public NamedFieldsRetriever(final String uri, final HttpClient client,
        final Supplier<Model> rdfr) {
        this.uri = uri;
        this.httpClient = client;
        this.rdfr = rdfr;
        final NamedFieldsDeserializer deserializer =
            new NamedFieldsDeserializer();
        this.gson =
            new GsonBuilder().registerTypeAdapter(typeToken, deserializer)
                    .create();
        deserializer.setGson(gson);
    }

    @Override
    public NamedFields get() {
        LOGGER.debug("Retrieving RDF representation for: {}", uri);
        try {
            final Model rdf = rdfr.get();
            if (!rdf.contains(createResource(uri), INDEXING_TRANSFORM_PREDICATE)) {
                LOGGER.info(
                        "Found no property locating LDPath transform for: {}, will not retrieve transformed content.",
                        uri);
                throw new AbsentTransformPropertyException(uri);
            }
            final RDFNode indexingTransform =
                rdf.listObjectsOfProperty(createResource(uri),
                        INDEXING_TRANSFORM_PREDICATE).next();
            final String transformKey =
                indexingTransform.asLiteral().getString();

            LOGGER.debug("Discovered transform key: {}", transformKey);
            final HttpGet transformedResourceRequest =
                new HttpGet(uri + "/fcr:transform/" + transformKey);
            LOGGER.debug("Retrieving transformed resource from: {}",
                    transformedResourceRequest.getURI());

            final HttpResponse response =
                httpClient.execute(transformedResourceRequest);
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw new HttpException(response.getStatusLine().toString());
            }
            try (
                Reader r =
                    new InputStreamReader(response.getEntity().getContent(),
                            "UTF8")) {
                return gson.fromJson(r, typeToken);
            }

        } catch (IOException | HttpException e) {
            throw propagate(e);
        }
    }

}
