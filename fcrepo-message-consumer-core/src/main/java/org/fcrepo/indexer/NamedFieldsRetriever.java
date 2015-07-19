/**
 * Copyright 2015 DuraSpace, Inc.
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
import java.net.URI;

import com.hp.hpl.jena.rdf.model.NodeIterator;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

import javax.ws.rs.core.Link;

/**
 * Retrieves resources transformed into sets of named fields via LDPath.
 * For use with indexers like Solr.
 *
 * @author ajs6f
 * @since Dec 6, 2013
 */
public class NamedFieldsRetriever implements Supplier<NamedFields> {

    private final URI uri;

    private final HttpClient httpClient;

    private final Supplier<Model> rdfr;

    private Gson gson;

    private static final Type typeToken = new TypeToken<NamedFields>() {
    }
            .getType();

    private static final Logger LOGGER = getLogger(NamedFieldsRetriever.class);

    /**
     * @param uri    the URI identifier
     * @param client the http client
     * @param rdfr   Used to determine the transform to use with this indexing
     *               step
     */
    public NamedFieldsRetriever(final URI uri, final HttpClient client,
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
        final HttpHead headRequest = new HttpHead(uri);

        try {
            final Model rdf = rdfr.get();

            // If there is no transform-predicate on this resource, look deeper...
            if (!rdf.contains(createResource(uri.toString()), INDEXING_TRANSFORM_PREDICATE)) {
                LOGGER.info("Looking up property locating LDPath transform for: {}", uri);
                // make an initial HEAD request and check Link headers for descriptions located elsewhere
                final HttpResponse headResponse = httpClient.execute(headRequest);
                URI descriptionURI = null;
                final Header[] links = headResponse.getHeaders("Link");
                if (links != null) {
                    for (Header h : headResponse.getHeaders("Link")) {
                        final Link link = Link.valueOf(h.getValue());
                        if (link.getRel().equals("describedby")) {
                            descriptionURI = link.getUri();
                            LOGGER.debug("Using URI from Link header: {}", descriptionURI);
                        }
                    }
                }
                if (descriptionURI == null) {
                    throw new AbsentTransformPropertyException("Property lookup failed for uri: " + uri);
                }

                // Return fields based on transform-predicate defined on the resource-description
                return getNamedFields(rdf, descriptionURI);
            }

            // Return fields based on transform-predicate defined on the original resource
            return getNamedFields(rdf, uri);
        } catch (IOException | HttpException e) {
            throw propagate(e);
        } finally {
            headRequest.releaseConnection();
        }
    }

    private NamedFields getNamedFields(final Model rdf, final URI uri) throws IOException, HttpException {
        HttpGet transformedResourceRequest = null;

        final NodeIterator nodeIterator =
                rdf.listObjectsOfProperty(createResource(uri.toString()),
                        INDEXING_TRANSFORM_PREDICATE);
        if (!nodeIterator.hasNext()) {
            throw new AbsentTransformPropertyException("Property lookup failed for uri: " + uri);
        }

        final RDFNode indexingTransform = nodeIterator.next();
        final String transformKey =
                indexingTransform.asLiteral().getString();
        LOGGER.debug("Discovered transform key: {}", transformKey);
        transformedResourceRequest =
                new HttpGet(uri.toString() + "/fcr:transform/" + transformKey);
        try {
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
        } finally {
            if (transformedResourceRequest != null) {
                transformedResourceRequest.releaseConnection();
            }
        }
    }

}
