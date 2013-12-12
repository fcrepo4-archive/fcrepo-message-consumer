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
package org.fcrepo.indexer.system;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.junit.Before;

import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.fcrepo.indexer.IndexerGroup.INDEXER_NAMESPACE;
import static org.junit.Assert.assertEquals;
import org.slf4j.Logger;

import com.google.common.io.Files;


public abstract class IndexingIT {

    private static final Logger LOGGER = getLogger(IndexingIT.class);

    protected static final int SERVER_PORT = parseInt(getProperty("test.port",
            "8080"));

    protected static final String serverAddress = "http://localhost:"
            + SERVER_PORT + "/";

    protected static HttpClient client;

    @Before
    public void setUp() throws ClientProtocolException, IOException {
        final PoolingClientConnectionManager connMann =
            new PoolingClientConnectionManager();
        connMann.setMaxTotal(MAX_VALUE);
        connMann.setDefaultMaxPerRoute(MAX_VALUE);
        client = new DefaultHttpClient(connMann);
        LOGGER.debug("Installing indexing namespace...");
        final String nsSparqlUpdate =
            "INSERT { <"
                    + INDEXER_NAMESPACE
                    + "> <http://purl.org/vocab/vann/preferredNamespacePrefix> \"indexing\" } WHERE { }";
        HttpPost update = new HttpPost(serverAddress + "fcr:namespaces");
        update.setEntity(new StringEntity(nsSparqlUpdate));
        update.setHeader("Content-Type", "application/sparql-update");
        HttpResponse response = client.execute(update);
        assertEquals("Failed to install indexing namespace!",
                SC_NO_CONTENT, response.getStatusLine().getStatusCode());

        LOGGER.debug("Installing indexing type information...");
        update = new HttpPost(serverAddress + "fcr:nodetypes");
        update.setHeader("Content-Type", "text/cnd");
        final HttpEntity cnd =
            new StringEntity(Files.toString(new File(
                    "target/classes/indexing.cnd"), defaultCharset()));
        update.setEntity(cnd);
        response = client.execute(update);
        assertEquals("Failed to install indexing type information!",
                SC_NO_CONTENT, response.getStatusLine().getStatusCode());
        /*HttpGet nsRequest = new HttpGet(serverAddress + "fcr:namespaces");
        nsRequest.setHeader("Content-Type", WebContent.contentTypeN3Alt1);
        LOGGER.debug("Now registered namespaces include:\n{}", IOUtils
                .toString(client.execute(nsRequest).getEntity().getContent()));
        nsRequest = new HttpGet(serverAddress + "fcr:nodetypes");
        nsRequest.setHeader("Content-Type", WebContent.contentTypeN3Alt1);
        LOGGER.debug("and registered node types:\n{}", IOUtils.toString(client
                .execute(nsRequest).getEntity().getContent()));*/

    }



}
