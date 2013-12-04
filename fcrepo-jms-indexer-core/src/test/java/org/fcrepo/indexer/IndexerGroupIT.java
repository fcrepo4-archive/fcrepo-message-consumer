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

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;

/**
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class IndexerGroupIT {

    private static final Logger LOGGER = getLogger(IndexerGroupIT.class);

    private static final int SERVER_PORT = parseInt(getProperty("test.port", "8080"));

    private static final String serverAddress = "http://localhost:"
            + SERVER_PORT + "/";

    private static final long TIMEOUT = 5000;

    private final PoolingClientConnectionManager connectionManager =
            new PoolingClientConnectionManager();

    private static HttpClient client;

    @Inject
    private IndexerGroup indexerGroup;

    @Inject
    private TestIndexer testIndexer;

    @Before
    public void setup() {
        client = new DefaultHttpClient(connectionManager);
    }

    @Test
    public void testIndexerGroupUpdate() throws Exception {
        doIndexerGroupUpdateTest("/updateTestPid");
    }

    private void doIndexerGroupUpdateTest(final String pid) throws Exception {
        // create dummy object
        final String uri = serverAddress + pid;
        final HttpPost method = new HttpPost(uri);
        final HttpResponse response = client.execute(method);
        assertEquals(201, response.getStatusLine().getStatusCode());
        LOGGER.debug("Created object at: {}", uri);

        final Long start = currentTimeMillis();
        synchronized (testIndexer) {
            while (!testIndexer.receivedUpdate(pid)
                    && (currentTimeMillis() - start < TIMEOUT)) {
                LOGGER.debug("Waiting for next notification from TestIndexer...");
                testIndexer.wait(1000);
            }
        }
        assertTrue("Test indexer should have received an update message for " + pid + "!", testIndexer
                .receivedUpdate(pid));
        LOGGER.debug("Received update at test indexer for pid: {}", pid);

    }

    @Test
    public void testIndexerGroupDelete() throws Exception {

        // create and verify dummy object
        final String pid = "/removeTestPid";
        final String uri = serverAddress + pid;
        doIndexerGroupUpdateTest(pid);

        // delete dummy object
        final HttpDelete method = new HttpDelete(uri);
        final HttpResponse response = client.execute(method);
        assertEquals(204, response.getStatusLine().getStatusCode());
        LOGGER.debug("Deleted object at: {}", uri);

        final Long start = currentTimeMillis();
        synchronized (testIndexer) {
            while (!testIndexer.receivedRemove(pid)
                    && (currentTimeMillis() - start < TIMEOUT)) {
                LOGGER.debug("Waiting for next notification from TestIndexer...");
                testIndexer.wait(1000);
            }
        }
        assertTrue("Test indexer should have received remove message for " + pid + "!", testIndexer
                .receivedRemove(pid));
        LOGGER.debug("Received remove at test indexer for pid: {}", pid);



    }
}
