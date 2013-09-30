package org.fcrepo.indexer;

<<<<<<< HEAD
=======
import java.io.File;

>>>>>>> upstream/master
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;

public class SolrServerFactory {

	private boolean embedded = false;
	private String solrServerUrl;
	private String solrTestHome;
	private String solrTestConfig;
	
	
	public SolrServer getSolrServer() {
		if(!isEmbedded())
		{
			return new HttpSolrServer(solrServerUrl);
		
		}
		else
		{
			//new Corecon
			CoreContainer cc = new CoreContainer(getSolrTestHome());
			EmbeddedSolrServer embeddedSolrServer = new EmbeddedSolrServer(cc, "");
			return embeddedSolrServer;
		}
		
	}


	public boolean isEmbedded() {
		return embedded;
	}


	public void setEmbedded(boolean embedded) {
		this.embedded = embedded;
	}


	public String getSolrTestHome() {
		return solrTestHome;
	}


	public void setSolrTestHome(String solrTestHome) {
		this.solrTestHome = solrTestHome;
	}


	public String getSolrTestConfig() {
		return solrTestConfig;
	}


	public void setSolrTestConfig(String solrTestConfig) {
		this.solrTestConfig = solrTestConfig;
	}
	
}
