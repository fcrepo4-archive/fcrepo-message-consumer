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

import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.jena.riot.WebContent.contentTypeN3;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.codec.binary.Base64;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
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
 * @date Dec 6, 2013
 */
public class RdfRetriever implements Supplier<Model> {

    private static final String RDF_SERIALIZATION = contentTypeN3;

    private final String identifier;

    private String fedoraAuth;
    private String fedoraUsername;
    private String fedoraPassword;

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

    /**
     * @param identifier
     * @param client
     */
    public RdfRetriever(final String identifier, final HttpClient client,
                        final String fedoraAuth, final String fedoraUsername, final String fedoraPassword) {
        this.identifier = identifier;
        this.httpClient = client;
        this.fedoraAuth = fedoraAuth;
        this.fedoraUsername = fedoraUsername;
        this.fedoraPassword = fedoraPassword;
    }

    @Override
    public Model get() {
        final HttpUriRequest request = new HttpGet(identifier);
        if (!"".equals(this.fedoraAuth)) {
            final String creds = this.fedoraUsername + ":" + this.fedoraPassword;
            LOGGER.debug("Adding BASIC authentication: {}...", creds);
            final String encoding =
                new String(Base64.encodeBase64(creds.getBytes()));
            request.setHeader("Authorization", this.fedoraAuth + " " + encoding);
        }
        request.addHeader("Accept", RDF_SERIALIZATION);
        LOGGER.debug("Retrieving RDF content from: {}...", request.getURI());
        try {
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

    /**
     * Set Fedora auth type.
     **/
    public void setFedoraAuth(final String fedoraAuth) {
        this.fedoraAuth = fedoraAuth;
    }

    /**
     * Get Fedora auth type.
     **/
    public String getFedoraAuth() {
        return fedoraAuth;
    }

    /**
     * Set Fedora username.
     **/
    public void setFedoraUsername(final String fedoraUsername) {
        this.fedoraUsername = fedoraUsername;
    }

    /**
     * Get Fedora username.
     **/
    public String getFedoraUsername() {
        return fedoraUsername;
    }

    /**
     * Set Fedora password.
     **/
    public void setFedoraPassword(final String fedoraPassword) {
        this.fedoraPassword = fedoraPassword;
    }

    /**
     * Get Fedora password.
     **/
    public String getFedoraPassword() {
        return fedoraPassword;
    }
}
