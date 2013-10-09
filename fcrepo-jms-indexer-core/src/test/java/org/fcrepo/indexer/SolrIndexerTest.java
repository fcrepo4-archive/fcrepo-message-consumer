package org.fcrepo.indexer;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexerTest {
	private SolrIndexer indexer;
	private SolrServer server;
	final private Logger logger = LoggerFactory.getLogger(SolrServerFactoryTest.class);
	@Before
	public void setUp() throws Exception {
		indexer=new SolrIndexer();
		//hack to get server instance
		System.setProperty("solr.solr.home", "./target/test-classes/");
		Initializer initializer = new CoreContainer.Initializer();
        CoreContainer cc = initializer.initialize();
        server = new EmbeddedSolrServer(cc, "");
        indexer.solrServer=server;
	}

	@Test
	public void testUpdate() throws IOException, SolrServerException {
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
