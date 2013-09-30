package org.fcrepo.indexer;

import java.io.IOException;
<<<<<<< HEAD
import javax.annotation.PostConstruct;
=======

import javax.annotation.PostConstruct;
import javax.inject.Inject;
>>>>>>> upstream/master

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexer implements Indexer {

	
	private SolrServerFactory solrServerFactory;
	
	private SolrServer solrServer;
	
	@PostConstruct
	public void instantiateSolrServer()
	{
		this.solrServer = getSolrServerFactory().getSolrServer();
	}
	
	@Override
	public void update(String pid, String doc) throws IOException {
		try {
			SolrInputDocument  inputDoc = new SolrInputDocument();
			inputDoc.addField("id", pid);
			inputDoc.addField("content", doc);
			UpdateResponse resp = solrServer.add(inputDoc);
			solrServer.commit();
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void remove(String pid) throws IOException {
		try {
			solrServer.deleteById(pid);
		} catch (SolrServerException e) {
			throw new IOException(e);
		}

	}

	public SolrServerFactory getSolrServerFactory() {
		return solrServerFactory;
	}

	public void setSolrServerFactory(SolrServerFactory solrServerFactory) {
		this.solrServerFactory = solrServerFactory;
	}


	

}
