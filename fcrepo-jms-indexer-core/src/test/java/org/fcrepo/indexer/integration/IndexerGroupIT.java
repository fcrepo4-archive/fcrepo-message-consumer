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

package org.fcrepo.indexer.integration;

import static java.lang.System.currentTimeMillis;
import static org.apache.jena.riot.WebContent.contentTypeN3Alt1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import javax.inject.Inject;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.fcrepo.indexer.IndexerGroup;
import org.fcrepo.indexer.TestIndexer;
import org.fcrepo.indexer.system.IndexingIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class IndexerGroupIT extends IndexingIT {

    private static final long TIMEOUT = 15000;

    @Inject
    private IndexerGroup indexerGroup;

    @Inject
    private TestIndexer testIndexer;

    private static final Logger LOGGER = getLogger(IndexerGroupIT.class);

    @Test
    public void testIndexerGroupUpdate() throws Exception {
        doIndexerGroupUpdateTest(serverAddress + "updateTestPid");
    }

    private void doIndexerGroupUpdateTest(final String uri) throws Exception {
        final HttpPost createRequest = new HttpPost(uri);
        final String objectRdf =
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."
                    + "@prefix indexing:<http://fedora.info/definitions/v4/indexing#>."
                    + "<" + uri + ">  rdf:type  <http://fedora.info/definitions/v4/indexing#indexable> ;"
                    + "indexing:hasIndexingTransformation \"default\".";

        createRequest.setEntity(new StringEntity(objectRdf));
        createRequest.addHeader("Content-Type", contentTypeN3Alt1);

        final HttpResponse response = client.execute(createRequest);
        assertEquals(201, response.getStatusLine().getStatusCode());
        LOGGER.debug("Created object at: {}", uri);

        final Long start = currentTimeMillis();
        synchronized (testIndexer) {
            while (!testIndexer.receivedUpdate(uri)
                    && (currentTimeMillis() - start < TIMEOUT)) {
                LOGGER.debug("Waiting for next notification from TestIndexer...");
                testIndexer.wait(1000);
            }
        }
        assertTrue("Test indexer should have received an update message for " + uri + "!", testIndexer
                .receivedUpdate(uri));
        LOGGER.debug("Received update at test indexer for identifier: {}", uri);

    }

    @Test
    public void testIndexerGroupDelete() throws Exception {

        final String uri = serverAddress + "removeTestPid";

        doIndexerGroupUpdateTest(uri);
        // delete dummy object
        final HttpDelete method = new HttpDelete(uri);
        final HttpResponse response = client.execute(method);
        assertEquals(204, response.getStatusLine().getStatusCode());
        LOGGER.debug("Deleted object at: {}", uri);

        final Long start = currentTimeMillis();
        synchronized (testIndexer) {
            while (!testIndexer.receivedRemove(uri)
                    && (currentTimeMillis() - start < TIMEOUT)) {
                LOGGER.debug("Waiting for next notification from TestIndexer...");
                testIndexer.wait(1000);
            }
        }
        assertTrue("Test indexer should have received remove message for " + uri + "!", testIndexer
                .receivedRemove(uri));
        LOGGER.debug("Received remove at test indexer for identifier: {}", uri);



    }

}
