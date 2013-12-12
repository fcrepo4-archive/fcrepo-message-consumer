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
package org.fcrepo.indexer.solr;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.apache.solr.core.CoreContainer.createAndLoad;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.fcrepo.indexer.solr.SolrIndexer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;


/**
 * @author yecao
 * @author ajs6f
 * @date Nov 2013
 */
public class SolrIndexerTest {

    private final String solrHome = "target/test-classes/solr";

    private SolrIndexer indexer;

    private SolrServer server;

    private static final Logger LOGGER = getLogger(SolrIndexerTest.class);

    private static final long TIMEOUT = 15000;

    private static final long TIME_TO_WAIT_STEP = 1000;


    @Before
    public void setUp() throws Exception {
        final CoreContainer container =
            createAndLoad(solrHome, new File(solrHome, "solr.xml"));
        LOGGER.debug("Using Solr home: {}", new File(container.getSolrHome())
                .getAbsolutePath());
        server = new EmbeddedSolrServer(container, "testCore");
        indexer = new SolrIndexer(server);
    }

    @Test
    public void testUpdate() throws SolrServerException, IOException, InterruptedException {
        doUpdate("456");
    }

    private void doUpdate(final String pid) throws SolrServerException, IOException, InterruptedException {
        final String content = "[{\"id\" : [\"" +pid+ "\"]}]";
        LOGGER.debug(
                "Trying update operation with identifier: {} and content: \"{}\".",
                pid, content);
        indexer.update(pid, new StringReader(content));

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
    public void testRemove() throws IOException, SolrServerException, InterruptedException {
        final String pid = "123";
        doUpdate(pid);
        indexer.remove(pid);
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

}
