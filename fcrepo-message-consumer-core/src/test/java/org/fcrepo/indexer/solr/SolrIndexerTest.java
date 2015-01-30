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
package org.fcrepo.indexer.solr;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.apache.solr.core.CoreContainer.createAndLoad;
import static org.fcrepo.indexer.Indexer.IndexerType.NAMEDFIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import static org.mockito.Mockito.any;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.fcrepo.indexer.NamedFields;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;


/**
 * @author yecao
 * @author ajs6f
 * @since Nov 2013
 */
public class SolrIndexerTest {

    private final String solrHome = "target/test-classes/solr";

    private SolrIndexer testIndexer;

    final CoreContainer container =
            createAndLoad(solrHome, new File(solrHome, "solr.xml"));

    private SolrServer server = new EmbeddedSolrServer(container, "testCore");

    @Mock
    private SolrServer mockServer;

    @Mock
    private UpdateResponse mockUpdateResponse;

    private static final Logger LOGGER = getLogger(SolrIndexerTest.class);

    private static final long TIMEOUT = 15000;

    private static final long TIME_TO_WAIT_STEP = 1000;

    private static Boolean successfulExecution = false;


    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Using Solr home: {}", new File(container.getSolrHome())
                .getAbsolutePath());
        testIndexer = new SolrIndexer(server);
        initMocks(this);
    }

    @Test
    public void testMutatesWithBadResults() throws Exception {
        final String id = "testBadUpdate:" + randomUUID();
        final SolrIndexer hold = testIndexer;
        when(mockServer.add(any(SolrInputDocument.class))).thenReturn(
                mockUpdateResponse);
        when(mockServer.deleteById(any(String.class))).thenReturn(
                mockUpdateResponse);
        // update failure
        when(mockUpdateResponse.getStatus()).thenReturn(1);
        testIndexer = new SolrIndexer(mockServer);
        final Collection<String> values = asList(id);
        final NamedFields testContent = new NamedFields(of("id", values));

        UpdateResponse result = testIndexer.update(new URI(id), testContent).get();
        assertEquals("Got wrong update response code!", 1, result.getStatus());
        result = testIndexer.remove(new URI(id)).get();
        assertEquals("Got wrong update response code!", 1, result.getStatus());


        testIndexer = hold;
    }

    @Test(expected = ExecutionException.class)
    public void testUpdateThatExplodes() throws Exception {
        final String id = "testExplodingUpdate:" + randomUUID();
        final SolrIndexer hold = testIndexer;
        // update failure
        when(mockServer.add(any(SolrInputDocument.class))).thenThrow(
                new SolrServerException("Expected."));

        testIndexer = new SolrIndexer(mockServer);
        final Collection<String> values = asList(id);
        final NamedFields testContent = new NamedFields(of("id", values));

        testIndexer.update(new URI(id), testContent).get();
        testIndexer = hold;
    }

    @Test(expected = ExecutionException.class)
    public void testUpdateWithAlternateExplosion() throws Exception {
        final String id = "testExplodingUpdate2:" + randomUUID();
        final SolrIndexer hold = testIndexer;
        // update failure
        when(mockServer.add(any(SolrInputDocument.class))).thenThrow(
                new IOException("Expected."));

        testIndexer = new SolrIndexer(mockServer);
        final Collection<String> values = asList(id);
        final NamedFields testContent = new NamedFields(of("id", values));

        testIndexer.update(new URI(id), testContent).get();
        testIndexer = hold;
    }

    @Test
    public void testUpdate() throws Exception {
        doUpdate("456");

    }

    private void doUpdate(final String pid) throws Exception {
        final Collection<String> values = asList(pid);
        final NamedFields testContent = new NamedFields(of("id", values));
        LOGGER.debug(
                "Trying update operation with identifier: {} and content: \"{}\".",
                pid, testContent);
        testIndexer.update(new URI(pid), testContent);

        final SolrParams query = new SolrQuery("id:" + pid);
        List<SolrDocument> results = server.query(query).getResults();
        Boolean success = results.size() == 1;
        final Long start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to appear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for presence of appropriate index record...");
            results = server.query(query).getResults();
            success = results.size() == 1;
        }
        assertTrue("Didn't find our expected record!", success);
    }

    @Test
    public void testRemove() throws Exception {
        final String pid = "123";
        doUpdate(pid);
        testIndexer.remove(new URI(pid));
        final SolrParams query = new SolrQuery("id:" + pid);
        List<SolrDocument> results = server.query(query).getResults();
        Boolean success = results.size() == 0;
        final Long start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to appear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for presence of appropriate index record...");
            results = server.query(query).getResults();
            success = results.size() == 0;
        }
        assertTrue("Found our record when we shouldn't have!", success);
    }

    @Test
    public void testGetIndexerType() {
        assertEquals("Got wrong testIndexer type!", NAMEDFIELDS, testIndexer
                .getIndexerType());
    }

    @Test
    public void testExecutorService() throws InterruptedException {
        testIndexer.executorService().execute(new Runnable() {

            @Override
            public void run() {
                successfulExecution = true;

            }});
        final Long start = currentTimeMillis();
        while (!successfulExecution && (currentTimeMillis() - start ) < TIMEOUT) {
            sleep(TIME_TO_WAIT_STEP);
        }
        assertTrue(
                "Failed to execute a task in this indexer's executor service before "
                        + TIMEOUT + "ms!", successfulExecution);
    }



}
