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

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.xml.sax.SAXException;

/**
 * @author walter
 *
 */
public class SolrServerFactory {

    private boolean embedded = false;

    private String solrServerUrl;

    private String solrTestHome;

    private String solrTestConfig;

    /**
     * @return
     */
    public SolrServer getSolrServer() {
        if (!isEmbedded()) {
            return new HttpSolrServer(solrServerUrl);

        } else {
            // new Corecon
            File file = new File(".");
            System.out.println(file.getAbsolutePath());
            System.setProperty("solr.solr.home", solrTestHome);
            Initializer initializer = new CoreContainer.Initializer();
            CoreContainer cc = null;
            try {
                cc = initializer.initialize();
            } catch (IOException | ParserConfigurationException | SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            EmbeddedSolrServer embeddedSolrServer =
                    new EmbeddedSolrServer(cc, "");
            return embeddedSolrServer;
        }

    }

    /**
     * @return
     */
    public boolean isEmbedded() {
        return embedded;
    }

    /**
     * @param embedded
     */
    public void setEmbedded(boolean embedded) {
        this.embedded = embedded;
    }

    /**
     * @return
     */
    public String getSolrTestHome() {
        return solrTestHome;
    }

    /**
     * @param solrTestHome
     */
    public void setSolrTestHome(String solrTestHome) {
        this.solrTestHome = solrTestHome;
    }

    /**
     * @return
     */
    public String getSolrTestConfig() {
        return solrTestConfig;
    }

    /**
     * @param solrTestConfig
     */
    public void setSolrTestConfig(String solrTestConfig) {
        this.solrTestConfig = solrTestConfig;
    }

}
