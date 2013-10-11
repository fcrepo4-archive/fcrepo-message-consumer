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

import javax.annotation.PostConstruct;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A Solr Indexer (stub) implementation that adds some basic information to
 * a Solr index server.
 * @author walter
 * 
**/
public class SolrIndexer implements Indexer {
    private SolrServerFactory solrServerFactory;
	private SolrServer solrServer;
    private Logger logger = 
    		LoggerFactory.getLogger
    		(SolrIndexer.class);
	
    /**
	 * @param solrServerFactory
	 */
	public SolrIndexer(SolrServerFactory solrServerFactory) {
		this.solrServerFactory = solrServerFactory;
	}
    /**
     * Initially instancing a Solr server instance
     */
    @PostConstruct
    public void instantiateSolrServer() {
        this.solrServer = solrServerFactory.getSolrServer();
    }
    
    public final SolrServer getSolrServer() {
		return solrServer;
	}

    /**
     * Implementation of the update method overriding org.fcrepo.indexer.Indexer
     * for the Solr indexer implementation
     * @see org.fcrepo.indexer.Indexer#update(java.lang.String, java.lang.String)
     */
    public void update(String pid, String doc) throws IOException {
        try {
            SolrInputDocument inputDoc = new SolrInputDocument();
            inputDoc.addField("id", pid);
            inputDoc.addField("content", doc);
            UpdateResponse resp = solrServer.add(inputDoc);
            if (resp.getStatus() == 0) {
               logger.debug("update request was successful for pid: {}", pid);
               solrServer.commit();
            }
            else{logger.debug("update request has error, code: {} for pid: {}", resp.getStatus(),pid);}
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /**
     * Implementation of the remove method overriding org.fcrepo.indexer.Indexer
     * for the Solr indexer implementation
     * @see org.fcrepo.indexer.Indexer#remove(java.lang.String)
     */
	public void remove(String pid) throws IOException {
        try {
        	UpdateResponse resp = solrServer.deleteById(pid);
            if (resp.getStatus() == 0) {
            	logger.debug("remove request was successful for pid: {}", pid);
            	solrServer.commit();
            }
            else{logger.debug("remove request has error, code: {} for pid: {}", resp.getStatus(),pid);}
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

    }

}
