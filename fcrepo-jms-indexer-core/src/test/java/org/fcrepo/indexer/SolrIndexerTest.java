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

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Before;
import org.junit.Test;


/**
 * @author yecao
 *
 */
public class SolrIndexerTest extends AbstractSolrTestCase {

    private final String solrHome = "./target/test-classes/solr";

    private SolrIndexer indexer;

    private SolrServer server;
    /**
     * @throws java.lang.Exception
     */
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        server =
                new EmbeddedSolrServer(h.getCoreContainer(), h.getCore()
                        .getName());
        indexer = new SolrIndexer(server);
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

    @Override
    public String getSchemaFile() {
        return solrHome + "/conf/schema.xml";
    }

    @Override
    public String getSolrConfigFile() {
        return solrHome + "/conf/solrconfig.xml";
    }

}
