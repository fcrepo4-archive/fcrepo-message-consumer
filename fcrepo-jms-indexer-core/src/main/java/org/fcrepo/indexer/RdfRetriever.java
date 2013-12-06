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

import static org.apache.http.HttpStatus.SC_OK;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.jena.riot.WebContent;
import org.slf4j.Logger;

/**
 * Retrieves RDF representations of resources for storage in a triplestore.
 * TODO: Extend functionality to provide for transformation, a la
 * {@link NamedFieldsRetriever}
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class RdfRetriever extends CachingRetriever {

    private static final String RDF_SERIALIZATION = WebContent.contentTypeN3;

    private final String identifier;

    private final HttpClient httpClient;

    private static final Logger LOGGER = getLogger(RdfRetriever.class);


    /**
     * @param identifier
     * @param client
     */
    public RdfRetriever(final String identifier, final HttpClient client) {
        this.identifier = identifier;
        this.httpClient = client;
    }

    @Override
    public HttpResponse retrieveHttpResponse() throws ClientProtocolException, IOException,
        HttpException {
        final HttpUriRequest request = new HttpGet(identifier);
        request.addHeader("Accept", RDF_SERIALIZATION);
        LOGGER.debug("Retrieving RDF content from: {}...", request.getURI());
        final HttpResponse response = httpClient.execute(request);
        if (response.getStatusLine().getStatusCode() == SC_OK) {
            return response;
        } else {
            throw new HttpException(response.getStatusLine().toString());
        }
    }

}
