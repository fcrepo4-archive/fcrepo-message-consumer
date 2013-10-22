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

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author yecao
 *
 */
public class SolrIndexer implements Indexer {

    private final SolrServer server;

    private final Logger logger = LoggerFactory.getLogger(SolrIndexer.class);

    /**
     * @Autowired solrServer instance is auto-@Autowired in indexer-core.xml
     */
    @Autowired
    public SolrIndexer(final SolrServer solrServer) {
        this.server = solrServer;
    }

    @Override
    public void update(final String pid, final String doc) {
        try {
            final SolrInputDocument inputDoc = new SolrInputDocument();
            inputDoc.addField("id", pid);
            inputDoc.addField("content", doc);
            final UpdateResponse resp = server.add(inputDoc);
            if (resp.getStatus() == 0) {
                logger.debug("update request was successful for pid: {}", pid);
                server.commit();
            } else {
                logger.warn(
                        "update request has error, code: {} for pid: {}",
                        resp.getStatus(), pid);
            }
        } catch (final SolrServerException | IOException e) {
            logger.warn("Update Exception: {}", e);
        }

    }

    /* (non-Javadoc)
     * @see org.fcrepo.indexer.Indexer#remove(java.lang.String)
     */
    @Override
    public void remove(final String pid) throws IOException {
        try {
            final UpdateResponse resp = server.deleteById(pid);
            if (resp.getStatus() == 0) {
                logger.debug("remove request was successful for pid: {}", pid);
                server.commit();
            } else {
                logger.warn("remove request has error, code: {} for pid: {}",
                        resp.getStatus(), pid);
            }
        } catch (final SolrServerException | IOException e) {
            logger.warn("Delete Exception: {}", e);
        }

    }

}
