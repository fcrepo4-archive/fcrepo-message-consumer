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
package org.fcrepo.indexer.integration.webapp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

/**
 * @author Esmé Cowles
 * @since 2014-04-08
 */
public class FedoraIndexerIT {

    /**
     * The server port of the application, set as system property by
     * maven-failsafe-plugin.
     */
    private static final String SERVER_PORT = System.getProperty("fcrepo.test.port");

    /**
     * The context path of the application (including the leading "/"), set as
     * system property by maven-failsafe-plugin.
     */
    private static final String CONTEXT_PATH = System
            .getProperty("fcrepo.test.context.path");

    private static final String HOSTNAME = "localhost";

    private static final String serverAddress = "http://" + HOSTNAME + ":" +
            SERVER_PORT + CONTEXT_PATH;

    private static final String repoAddress = "http://" + HOSTNAME + ":" +
            SERVER_PORT + "/f4/rest/";

    private static HttpClient client = HttpClients.custom()
            .setConnectionManager(new PoolingHttpClientConnectionManager()).build();

    @Test
    public void testReindex() throws IOException {
        final HttpPost reindex = new HttpPost(serverAddress + "/reindex/");
        final List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("baseURI", repoAddress));
        reindex.setEntity(new UrlEncodedFormEntity(params));
        final HttpResponse response = client.execute(reindex);
        assertEquals(200, response.getStatusLine().getStatusCode());
        //substring required for OS specific differences
        assertEquals("Reindexing started".substring(0,18), EntityUtils.toString(response.getEntity()).substring(0,18));
    }

    @Test
    public void testReindexWithoutBaseURI() throws IOException {
        final HttpPost reindex = new HttpPost(serverAddress + "/reindex/");
        final HttpResponse response = client.execute(reindex);
        assertEquals(400, response.getStatusLine().getStatusCode());
    }
}
