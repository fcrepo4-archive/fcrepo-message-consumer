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
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexerTest {
	private SolrIndexer indexer;
	private SolrServerFactory solrServerFactory;
	private SolrServer server;
	private boolean embedded = true;
    private String solrServerUrlOrSolrTestHome="./target/test-classes/";
	final private Logger logger = LoggerFactory.getLogger(SolrIndexerTest.class);
	@Before
	public void setUp() throws Exception {
		solrServerFactory=new SolrServerFactory(embedded,solrServerUrlOrSolrTestHome);
		indexer=new SolrIndexer(solrServerFactory);
		indexer.instantiateSolrServer();
		server=indexer.getSolrServer();
	}

	@Test
	public void testUpdate()  throws IOException, SolrServerException {
		indexer.update("123", "some content");
		SolrParams params = new SolrQuery("content");
        QueryResponse response = server.query(params);
        assertEquals("123", response.getResults().get(0).get("id"));
	}

	@Test
	public void testRemove() throws IOException, SolrServerException {
		indexer.update("123", "some content");
		indexer.remove("123");
		SolrParams params = new SolrQuery("content");
        QueryResponse response = server.query(params);
        assertEquals(0, response.getResults().getNumFound());
	}

	
}
