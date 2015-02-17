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
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.jena.riot.WebContent.contentTypeN3;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;

import javax.ws.rs.core.Link;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Retrieves RDF representations of resources for storage in a triplestore.
 * TODO: Extend functionality to provide for transformation, a la
 * {@link NamedFieldsRetriever}
 *
 * @author ajs6f
 * @since Dec 6, 2013
 */
public class RdfRetriever implements Supplier<Model> {

    private static final String RDF_SERIALIZATION = contentTypeN3;

    private final URI identifier;

    private final HttpClient httpClient;

    private static final Logger LOGGER = getLogger(RdfRetriever.class);

    /**
     * @param identifier the uri identifier
     * @param client the http client
     */
    public RdfRetriever(final URI identifier, final HttpClient client) {
        this.identifier = identifier;
        this.httpClient = client;
    }

    @Override
    public Model get() {

        try {
            // make an initial HEAD request and check Link headers for descriptions located elsewhere
            final HttpHead headRequest = new HttpHead(identifier);
            final HttpResponse headResponse = httpClient.execute(headRequest);
            URI descriptionURI = null;
            final Header[] links = headResponse.getHeaders("Link");
            if ( links != null ) {
                for ( Header h : headResponse.getHeaders("Link") ) {
                    final Link link = Link.valueOf(h.getValue());
                    if ( link.getRel().equals("describedby") ) {
                        descriptionURI = link.getUri();
                        LOGGER.debug("Using URI from Link header: {}", descriptionURI);
                    }
                }
            }
            if ( descriptionURI == null ) {
                descriptionURI = identifier;
            }

            final HttpUriRequest request = new HttpGet(descriptionURI);
            request.addHeader("Accept", RDF_SERIALIZATION);
            LOGGER.debug("Retrieving RDF content from: {}...", request.getURI());
            final HttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() == SC_OK) {
                try (
                    Reader r =
                        new InputStreamReader(
                                response.getEntity().getContent(), "UTF8")) {
                    return createDefaultModel().read(r, "", "N3");
                }
            } else {
                throw new HttpException(response.getStatusLine().toString());
            }
        } catch (IOException | HttpException e) {
            throw propagate(e);
        }
    }

}
