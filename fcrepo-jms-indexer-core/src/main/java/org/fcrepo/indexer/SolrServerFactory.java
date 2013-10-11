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

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

/**
 * An Implementation for a Solr server factory used to create instances of Solr
 * servers.
 * 
 * @author walter
 */
public class SolrServerFactory {

	private static final Logger LOGGER = getLogger(SolrServerFactory.class);
    private boolean embedded;
    private String solrServerUrlOrSolrTestHome; 
    
    /**
	 * @param embedded
	 * @param solrServerUrl
	 * @param solrTestHome
	 */
	public SolrServerFactory(boolean embedded,
			String solrServerUrlOrSolrTestHome) {
		this.embedded = embedded;
		this.solrServerUrlOrSolrTestHome = solrServerUrlOrSolrTestHome;
	}
    /**
     * Returns a SolrServer instance for indexing purpose
     * 
     * @return Solr server (SolrServer) instance or null if no instance could
     * be created
     */
    public SolrServer getSolrServer() {
        if (!this.embedded) {
            return new HttpSolrServer(solrServerUrlOrSolrTestHome);

        } else {
            EmbeddedSolrServer embeddedSolrServer = null;
            try {
                // trying to set up a new embedded instance
                System.setProperty("solr.solr.home", solrServerUrlOrSolrTestHome);
                Initializer initializer = new CoreContainer.Initializer();
                CoreContainer cc = initializer.initialize();
                embeddedSolrServer = new EmbeddedSolrServer(cc, "");
            } catch (IOException | ParserConfigurationException
                    | SAXException e)
            {

                LOGGER.error("Couldn't initialize CoreContainer", e);
            }
            return embeddedSolrServer;
        }

    }
}
