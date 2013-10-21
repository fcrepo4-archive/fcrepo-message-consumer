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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * @author yecao
 *
 */
public class SolrIndexerTest {

    private SolrIndexer indexer;

    private SolrServer server;

    // get hold of CoreConatiner in-order to shut down the server
    private CoreContainer coreContainer;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        System.setProperty("solr.solr.home", "./target/test-classes/");
        final CoreContainer.Initializer initializer =
                new CoreContainer.Initializer();
        coreContainer = initializer.initialize();
        server = new EmbeddedSolrServer(coreContainer, "");
        indexer = new SolrIndexer(server);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        coreContainer.shutdown();
    }

    /**
     * Test method for
     * {@link org.fcrepo.indexer.SolrIndexer#update(java.lang.String, java.lang.String)}
     * .
     * 
     * @throws SolrServerException
     */
    @Test
    public void testUpdate() throws SolrServerException {
        indexer.update("123", "some content");
        final SolrParams params = new SolrQuery("content");
        final QueryResponse response = server.query(params);
        assertEquals("123", response.getResults().get(0).get("id"));
    }

    /**
     * Test method for
     * {@link org.fcrepo.indexer.SolrIndexer#remove(java.lang.String)}.
     * 
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void testRemove() throws IOException, SolrServerException {
        indexer.update("123", "some content");
        indexer.remove("123");
        final SolrParams params = new SolrQuery("content");
        final QueryResponse response = server.query(params);
        assertEquals(0, response.getResults().getNumFound());
    }

}
