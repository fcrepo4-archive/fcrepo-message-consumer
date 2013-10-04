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

/**
 * @author walter
 *
 */
public class SolrIndexer implements Indexer {

    private SolrServerFactory solrServerFactory;

    private SolrServer solrServer;

    /**
     * 
     */
    @PostConstruct
    public void instantiateSolrServer() {
        this.solrServer = getSolrServerFactory().getSolrServer();
    }

    /* (non-Javadoc)
     * @see org.fcrepo.indexer.Indexer#update(java.lang.String, java.lang.String)
     */
    @Override
    public void update(String pid, String doc) throws IOException {
        try {
            SolrInputDocument inputDoc = new SolrInputDocument();
            inputDoc.addField("id", pid);
            inputDoc.addField("content", doc);
            UpdateResponse resp = solrServer.add(inputDoc);
            solrServer.commit();
        } catch (SolrServerException e) {
            throw new IOException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.fcrepo.indexer.Indexer#remove(java.lang.String)
     */
    @Override
    public void remove(String pid) throws IOException {
        try {
            solrServer.deleteById(pid);
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

    }

    /**
     * @return SolrServerFactory instance. Needed for generating
     */
    public SolrServerFactory getSolrServerFactory() {
        return solrServerFactory;
    }

    /**
     * @param solrServerFactory (SolrServerFactory) a factory for generating
     * SOLR instances
     */
    public void setSolrServerFactory(SolrServerFactory solrServerFactory) {
        this.solrServerFactory = solrServerFactory;
    }

}
