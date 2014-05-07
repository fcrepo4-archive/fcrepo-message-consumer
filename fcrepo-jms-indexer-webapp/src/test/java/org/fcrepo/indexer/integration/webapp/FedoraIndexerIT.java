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
package org.fcrepo.indexer.integration.webapp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

/**
 * @author Esmé Cowles
 *         Date: April 08, 2014
 */
public class FedoraIndexerIT {

    /**
     * The server port of the application, set as system property by
     * maven-failsafe-plugin.
     */
    private static final String SERVER_PORT = System.getProperty("test.port");

    /**
     * The context path of the application (including the leading "/"), set as
     * system property by maven-failsafe-plugin.
     */
    private static final String CONTEXT_PATH = System
            .getProperty("test.context.path");

    private static final String HOSTNAME = "localhost";

    private static final String serverAddress = "http://" + HOSTNAME + ":" +
            SERVER_PORT + CONTEXT_PATH;

    private static HttpClient client = new DefaultHttpClient();

    @Test
    public void testReindex() throws IOException {
        HttpPost reindex = new HttpPost(serverAddress + "/reindex/");
        HttpResponse response = client.execute(reindex);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("Reindexing started\n", EntityUtils.toString(response.getEntity()) );
    }
}
