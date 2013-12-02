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

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

/**
 * A Solr Indexer (stub) implementation that adds some basic information to
 * a Solr index server.
 *
 * @author ajs6f
 * @author yecao
 * @date Nov 2013
 */
public class SolrIndexer implements Indexer {

    private final SolrServer server;

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexer.class);

    /**
     * @Autowired solrServer instance is auto-@Autowired in indexer-core.xml
     */
    @Autowired
    public SolrIndexer(final SolrServer solrServer) {
        this.server = solrServer;
    }

    @Override
    public ListenableFuture<UpdateResponse> update(final String pid,
            final String doc) {
        LOGGER.debug("Received request for update to: {}", pid);
        return run(ListenableFutureTask.create(new Callable<UpdateResponse>() {

            @Override
            public UpdateResponse call() throws Exception {
                try {
                    final SolrInputDocument inputDoc = new SolrInputDocument();
                    inputDoc.addField("id", pid);
                    inputDoc.addField("content", doc);
                    final UpdateResponse resp = server.add(inputDoc);
                    if (resp.getStatus() == 0) {
                        LOGGER.debug(
                                "Update request was successful for: {}",
                                pid);
                        server.commit();
                    } else {
                        LOGGER.error(
                                "update request has error, code: {} for pid: {}",
                                resp.getStatus(), pid);
                    }
                    return resp;
                } catch (final SolrServerException | IOException e) {
                    LOGGER.error("Update Exception: {}", e);
                    throw propagate(e);
                }
            }
        }));
    }

    /**
     * (non-Javadoc)
     * @return
     * @see org.fcrepo.indexer.Indexer#remove(java.lang.String)
     */
    @Override
    public ListenableFuture<UpdateResponse> remove(final String pid)
        throws IOException {
        LOGGER.debug("Received request for removal of: {}", pid);
        return run(ListenableFutureTask.create(new Callable<UpdateResponse>() {

            @Override
            public UpdateResponse call() throws Exception {
                try {
                    final UpdateResponse resp = server.deleteById(pid);
                    if (resp.getStatus() == 0) {
                        LOGGER.debug(
                                "Remove request was successful for: {}",
                                pid);
                        server.commit();

                    } else {
                        LOGGER.error(
                                "Remove request has error, code: {} for pid: {}",
                                resp.getStatus(), pid);
                    }
                    return resp;
                } catch (final SolrServerException | IOException e) {
                    LOGGER.error("Delete Exception: {}", e);
                    throw propagate(e);
                }
            }
        }));
    }

    private static <T> ListenableFuture<T> run(
        final ListenableFutureTask<T> task) {
        task.run();
        return task;
    }
}
