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

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static org.fcrepo.indexer.IndexerGroup.INDEXING_TRANSFORM_PREDICATE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * Retrieves resources transformed into sets of named fields via LDPath.
 * For use with indexers like Solr.
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class NamedFieldsRetriever extends CachingRetriever {

    private final String uri;

    private final HttpClient httpClient;

    private final RdfRetriever rdfr;

    private static final Logger LOGGER = getLogger(NamedFieldsRetriever.class);

    /**
     * @param uri
     * @param client
     */
    public NamedFieldsRetriever(final String uri, final HttpClient client,
        final RdfRetriever rdfr) {
        this.uri = uri;
        this.httpClient = client;
        this.rdfr = rdfr;
    }

    @Override
    public HttpResponse retrieveHttpResponse() throws AbsentTransformPropertyException,
        ClientProtocolException, IOException, HttpException {
        LOGGER.debug("Retrieving RDF representation from: {}", uri);
        final Model rdf = createDefaultModel().read(rdfr.call(), null, "N3");
        if (!rdf.contains(createResource(uri), INDEXING_TRANSFORM_PREDICATE)) {
            LOGGER.info(
                    "Found no property locating LDPath transform for: {}, will not retrieve transformed content.",
                    uri);
            throw new AbsentTransformPropertyException(uri);
        }
        final RDFNode indexingTransform =
            rdf.listObjectsOfProperty(createResource(uri),
                    INDEXING_TRANSFORM_PREDICATE).next();
        final String transformKey = indexingTransform.asLiteral().getString();

        LOGGER.debug("Discovered transform key: {}", transformKey);
        final HttpGet transformedResourceRequest =
            new HttpGet(uri + "/fcr:transform/" + transformKey);
        LOGGER.debug("Retrieving transformed resource from: {}",
                transformedResourceRequest.getURI());
        return
            httpClient.execute(transformedResourceRequest);
    }

}
