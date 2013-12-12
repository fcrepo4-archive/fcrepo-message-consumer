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
package org.fcrepo.indexer.integration.solr;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.fcrepo.indexer.solr.SolrIndexer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author yecao
 * @author ajs6f
 * @date Nov 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class SolrIndexerIT {

    @Autowired
    private SolrIndexer solrIndexer;
    @Autowired
    private SolrServer server;

    private static final Logger LOGGER = getLogger(SolrIndexerIT.class);

    private static final long TIMEOUT = 15000;

    private static final long TIME_TO_WAIT_STEP = 1000;

    @Test
    public void testUpdate() throws SolrServerException, IOException, InterruptedException {
        doUpdate("123");
    }

    private void doUpdate(final String pid) throws SolrServerException, IOException, InterruptedException {
        final String json =
            "[{\"id\" : [\"" + pid + "\"]}]";
        solrIndexer.update(pid, new StringReader(json));
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
        assertEquals("Didn't find the record that should have been created!",
                pid, results.get(0).get("id"));
    }

    @Test
    public void testRemove() throws IOException, SolrServerException, InterruptedException {
        doUpdate("345");
        solrIndexer.remove("345");
        final SolrParams query = new SolrQuery("id:345");
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
        assertEquals("Found a record that should have been deleted!", 0,
                results.size());
    }

}
