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

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.fcrepo.jms.legacy.LegacyMethod;
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

    private boolean embedded = false;

    private String solrServerUrl;

    private String solrTestHome;

    private String solrTestConfig;

    /**
     * Returns a SolrServer instance for indexing purpose
     * 
     * @return Solr server (SolrServer) instance or null if no instance could
     * be created
     */
    public SolrServer getSolrServer() {
        if (!isEmbedded()) {
            return new HttpSolrServer(solrServerUrl);

        } else {
            EmbeddedSolrServer embeddedSolrServer = null;
            try {
                // trying to set up a new embedded instance
                System.setProperty("solr.solr.home", solrTestHome);
                Initializer initializer = new CoreContainer.Initializer();
                CoreContainer cc = initializer.initialize();
                embeddedSolrServer = new EmbeddedSolrServer(cc, "");
            } catch (IOException | ParserConfigurationException | SAXException e) {

                LOGGER.error("Couldn't initialize CoreContainer", e);
            }
            return embeddedSolrServer;
        }

    }

    /**
     * Describes whether a embedded or a standalone server instance should be
     * created
     * 
     * @return true if embedded version is enabled
     */
    public boolean isEmbedded() {
        return embedded;
    }

    /**
     * Setter method for embedded. Embedded describes whether an embedded or a 
     * standalone server instance should be created
     * 
     * @param embedded if a embedded server instance should be requested
     */
    public void setEmbedded(boolean embedded) {
        this.embedded = embedded;
    }

    /**
     * Getter method for the solrTestHome path
     * 
     * @return solrTestHome path
     */
    public String getSolrTestHome() {
        return solrTestHome;
    }

    /**
     * Setter method for the solrTestHome path
     * 
     * @param solrTestHome
     */
    public void setSolrTestHome(String solrTestHome) {
        this.solrTestHome = solrTestHome;
    }

    /**
     * Getter method for solr test configuration file path
     * 
     * @return solrTestConfig file path
     */
    public String getSolrTestConfig() {
        return solrTestConfig;
    }

    /**
     * Setter method for solr test configuration file path
     * 
     * @param solrTestConfig file path
     */
    public void setSolrTestConfig(String solrTestConfig) {
        this.solrTestConfig = solrTestConfig;
    }

}
