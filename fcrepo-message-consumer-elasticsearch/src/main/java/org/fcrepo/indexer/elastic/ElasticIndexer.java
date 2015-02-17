/**
 * Copyright 2015 DuraSpace, Inc.
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
package org.fcrepo.indexer.elastic;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.fcrepo.indexer.Indexer.IndexerType.NAMEDFIELDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.net.URI;
import java.util.concurrent.Callable;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.fcrepo.indexer.AsynchIndexer;
import org.fcrepo.indexer.NamedFields;
import org.slf4j.Logger;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * {@link org.fcrepo.indexer.Indexer} for Elasticsearch.
 *
 * @author ajs6f
 * @since Dec 14, 2013
 */
public class ElasticIndexer extends AsynchIndexer<NamedFields, ActionResponse> {

    @Inject
    private Client client;

    /**
     * The Elasticsearch index type to use
     */
    private String searchIndexType;

    /**
     * The Elasticsearch index name to use
     */
    private String indexName;

    private static final Logger LOGGER = getLogger(ElasticIndexer.class);


    /**
     * Constructs an index with the name supplied.
     * @throws InterruptedException if interrupted exception occurred
     */
    @PostConstruct
    public void initIndex() throws InterruptedException {
        if (!client.admin().indices().prepareExists(getIndexName()).execute()
                .actionGet().isExists()) {
            LOGGER.debug("Creating index {}.", getIndexName());
            client.admin().indices().prepareCreate(getIndexName()).execute()
                    .actionGet();
        }
        client.admin().cluster().prepareHealth().setWaitForGreenStatus()
                .execute().actionGet();
    }

    @Override
    public Callable<ActionResponse> removeSynch(final URI id) {
        return new Callable<ActionResponse>() {

            @Override
            public ActionResponse call() {
                return client.prepareDelete(getIndexName(),
                        getSearchIndexType(), id.toString()).execute().actionGet();
            }
        };
    }

    @Override
    public Callable<ActionResponse> updateSynch(final URI id, final NamedFields content) {
        return new Callable<ActionResponse>() {

            @Override
            public ActionResponse call() {
                return client.prepareIndex(indexName, searchIndexType, id.toString())
                        .execute().actionGet();

            }
        };
    }

    @Override
    public IndexerType getIndexerType() {
        return NAMEDFIELDS;
    }

    /**
     * Number of threads to use for operating against the index.
     */
    private static final Integer THREAD_POOL_SIZE = 5;

    private ListeningExecutorService executorService =
        listeningDecorator(newFixedThreadPool(THREAD_POOL_SIZE));

    @Override
    public ListeningExecutorService executorService() {
        return executorService;
    }

    /**
     * @param searchIndexType the searchIndexType to set
     */
    public void setSearchIndexType(final String searchIndexType) {
        this.searchIndexType = searchIndexType;
    }

    /**
     * @param indexName the indexName to set
     */
    public void setIndexName(final String indexName) {
        this.indexName = indexName;
    }


    /**
     * @return the searchIndexType
     */
    public String getSearchIndexType() {
        return searchIndexType;
    }


    /**
     * @return the indexName
     */
    public String getIndexName() {
        return indexName;
    }

}
