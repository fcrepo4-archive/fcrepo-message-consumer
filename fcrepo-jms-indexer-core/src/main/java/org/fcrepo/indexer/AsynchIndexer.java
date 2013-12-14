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
import org.slf4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * An {@link Indexer} that executes its operation asynchronously.
 *
 * @author ajs6f
 * @date Dec 8, 2013
 *
 * @param <Content> the type of content to index
 * @param <Result> the type of response to expect from an operation
 */
public abstract class AsynchIndexer<Content, Result> implements
    Indexer<Content> {

    private static final Logger LOGGER = getLogger(AsynchIndexer.class);

    /**
     * @return The {@link ListeningExecutorService} to use for operation.
     */
    public abstract ListeningExecutorService executorService();

    @Override
    public ListenableFuture<Result> update(final String identifier,
        final Content content) throws IOException {
        LOGGER.debug("Received update for identifier: {}", identifier);

        final ListenableFutureTask<Result> task = updateSynch(identifier, content);
        task.addListener(new Runnable() {
            @Override
            public void run() {
                synchronized (this) {
                    notifyAll();
                }
            }
        }, executorService());
        executorService().submit(task);
        return task;
    }

    @Override
    public ListenableFuture<Result> remove(final String identifier)
        throws IOException {
        LOGGER.debug("Received remove for identifier: {}", identifier);
        final ListenableFutureTask<Result> task = removeSynch(identifier);
        task.addListener(new Runnable() {
            @Override
            public void run() {
                synchronized (this) {
                    notifyAll();
                }
            }
        }, executorService());
        executorService().submit(task);
        return task;
    }

    /**
     * @param identifier
     * @return
     */
    public abstract ListenableFutureTask<Result> removeSynch(final String identifier);

    /**
     * @param identifier
     * @param content
     * @return
     */
    public abstract ListenableFutureTask<Result> updateSynch(final String identifier,
            final Content content);

}
