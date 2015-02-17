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
package org.fcrepo.indexer;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;

import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * An {@link Indexer} that completes its operations synchronously.
 *
 * @author ajs6f
 * @since Dec 8, 2013
 * @param <Content> the type of content to index
 * @param <Result> the type of response to expect from an operation
 */
public abstract class SynchIndexer<Content, Result> extends
    AsynchIndexer<Content, Result> {

    private final ListeningExecutorService executorService =
        sameThreadExecutor();

    @Override
    public ListeningExecutorService executorService() {
        return executorService;
    }

}
