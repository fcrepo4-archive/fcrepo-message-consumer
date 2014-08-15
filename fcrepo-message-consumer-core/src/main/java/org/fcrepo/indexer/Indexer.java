/**
 * Copyright 2014 DuraSpace, Inc.
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
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Main interface for individual indexers to implement.  Each type of
 * destination (Solr, triplestore, files, etc.) should have its own
 * implementation. Abstract classes {@link AsynchIndexer} and {@link SynchIndexer
 * are provided for convenience.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
 *
 * @param <Content> the type of content to index
 *
**/
public interface Indexer<Content> {

    /**
     * Create or update an index entry for the object.
     *
     * @param id
     * @param content
     * @return the results of addition
     * @throws IOException
     */
    public ListenableFuture<?> update(final String id, final Content content) throws IOException;

    /**
     * Remove the object from the index.
     *
     * @param id
     * @return the results of removal
     * @throws IOException
     */
    public ListenableFuture<?> remove(final String id) throws IOException;

    /**
     * @return What kind of indexer this is.
     */
    public IndexerType getIndexerType();

    /**
     * Types of content processed by {@link Indexer}s.
     *
     * @author ajs6f
     * @date Dec 14, 2013
     */
    public static enum IndexerType {
        NAMEDFIELDS, RDF, NO_CONTENT, JCRXML_PERSISTENCE
    }

    /**
     * Class for indexers that do not actually accept content.
     *
     * @author ajs6f
     * @date Dec 14, 2013
     */
    public static interface NoContent {

    }
}
