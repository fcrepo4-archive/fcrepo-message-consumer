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
package org.fcrepo.indexer.system;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.junit.Before;

import org.slf4j.Logger;

/**
 * @author ajs6f
 */
public abstract class IndexingIT {

    private static final Logger LOGGER = getLogger(IndexingIT.class);

    protected static final int SERVER_PORT = parseInt(getProperty("fcrepo.dynamic.test.port",
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
    }
}
