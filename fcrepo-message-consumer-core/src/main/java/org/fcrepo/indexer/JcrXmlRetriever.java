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
import static org.apache.http.HttpStatus.SC_OK;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;

import com.google.common.base.Supplier;

/**
 * Retrieves Modeshape jcr/xml for file system persistence
 * @author lsitu
 */
public class JcrXmlRetriever implements Supplier<InputStream> {

    private final URI identifier;

    private final HttpClient httpClient;

    private static final Logger LOGGER = getLogger(JcrXmlRetriever.class);

    /**
     * Constructor
     * @param identifier
     * @param client
     */
    public JcrXmlRetriever(final URI identifier, final HttpClient client) {
        this.identifier = identifier;
        this.httpClient = client;
    }

    @Override
    /**
     * Retrieve jcr/xml with no binary contents from the repository
     */
    public InputStream get() {
        final HttpUriRequest request = new HttpGet(identifier.toString() + "/fcr:export?skipBinary=true");
        LOGGER.debug("Retrieving jcr/xml content from: {}...", request.getURI());
        try {
            final HttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() == SC_OK) {
               return response.getEntity().getContent();
            } else {
                throw new HttpException(response.getStatusLine().toString());
            }
        } catch (IOException | HttpException e) {
            throw propagate(e);
        }
    }

}
