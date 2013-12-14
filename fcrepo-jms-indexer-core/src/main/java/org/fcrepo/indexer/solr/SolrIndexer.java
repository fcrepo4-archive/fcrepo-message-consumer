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

package org.fcrepo.indexer.solr;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Maps.transformEntries;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.fcrepo.indexer.Indexer.IndexerType.NAMEDFIELDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.fcrepo.indexer.AsynchIndexer;
import org.fcrepo.indexer.IndexerGroup;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * A Solr Indexer (stub) implementation that adds some basic information to a
 * Solr index server.
 *
 * @author ajs6f
 * @author yecao
 * @date Nov 2013
 */
public class SolrIndexer extends
    AsynchIndexer<Map<String, Collection<String>>, UpdateResponse> {

    public static final String CONFIGURATION_FOLDER =
        "/fedora:system/fedora:transform/fedora:ldpath/";

    // TODO make index-time boost somehow adjustable, or something
    public static final Long INDEX_TIME_BOOST = 1L;

    private final SolrServer server;

    /**
     * Number of threads to use for operating against the triplestore.
     */
    private static final Integer THREAD_POOL_SIZE = 5;

    private ListeningExecutorService executorService =
        listeningDecorator(newFixedThreadPool(THREAD_POOL_SIZE));


    private static final Logger LOGGER = getLogger(SolrIndexer.class);

    @Inject
    private IndexerGroup indexerGroup;

    /**
     * @Autowired solrServer instance is auto-@Autowired in indexer-core.xml
     */
    @Autowired
    public SolrIndexer(final SolrServer solrServer) {
        this.server = solrServer;
    }

    @Override
    public ListenableFutureTask<UpdateResponse> updateSynch(final String pid,
            final Map<String, Collection<String>> fields) {
        LOGGER.debug("Received request for update to: {}", pid);
        return ListenableFutureTask.create(new Callable<UpdateResponse>() {

            @Override
            public UpdateResponse call() throws Exception {
                try {
                    // parse the JSON fields to a Solr input doc
                    final SolrInputDocument inputDoc = fromMap(fields);
                    LOGGER.debug("Parsed SolrInputDocument: {}", inputDoc);


                    // add the identifier of the resource as a unique index-key
                    inputDoc.addField("id", pid);

                    final UpdateResponse resp = server.add(inputDoc);
                    if (resp.getStatus() == 0) {
                        LOGGER.debug("Update request was successful for: {}",
                                pid);
                        server.commit();
                    } else {
                        LOGGER.error(
                                "update request has error, code: {} for pid: {}",
                                resp.getStatus(), pid);
                    }
                    return resp;
                } catch (final SolrServerException | IOException e) {
                    LOGGER.error("Update exception: {}!", e);
                    throw propagate(e);
                }
            }
        });
    }

    protected SolrInputDocument fromMap(final Map<String, Collection<String>> fields) {
        return new SolrInputDocument(transformEntries(fields,
                collection2solrInputField));
    }

    private static EntryTransformer<String, Collection<String>, SolrInputField> collection2solrInputField =
            new EntryTransformer<String, Collection<String>, SolrInputField>() {

                @Override
                public SolrInputField transformEntry(final String key,
                    final Collection<String> input) {
                    final SolrInputField field = new SolrInputField(key);
                    for (final String value : input) {
                        field.addValue(value, INDEX_TIME_BOOST);
                    }
                    return field;
                }
            };


    @Override
    public ListenableFutureTask<UpdateResponse> removeSynch(final String pid) {
        LOGGER.debug("Received request for removal of: {}", pid);
        return ListenableFutureTask.create(new Callable<UpdateResponse>() {

            @Override
            public UpdateResponse call() throws Exception {
                try {
                    final UpdateResponse resp = server.deleteById(pid);
                    if (resp.getStatus() == 0) {
                        LOGGER.debug("Remove request was successful for: {}",
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
        });
    }

    /**
     * @return the {@link SolrServer} in use
     */
    public SolrServer getServer() {
        return server;
    }

    @Override
    public IndexerType getIndexerType() {
        return NAMEDFIELDS;
    }

    @Override
    public ListeningExecutorService executorService() {
        return executorService;
    }


}
