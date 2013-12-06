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
import java.io.Reader;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Main interface for individual indexers to implement.  Each type of
 * destination (Solr, triplestore, files, etc.) should have its own
 * implementation.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
**/
public interface Indexer {

    /**
     * Create or update an index entry for the object.
    **/
    public ListenableFuture<?> update(final String pid, final Reader doc) throws IOException;

    /**
     * Remove the object from the index.
    **/
    public ListenableFuture<?> remove(final String pid) throws IOException;

    /**
     * @return What kind of indexer this is.
     */
    public IndexerType getIndexerType();

    public static enum IndexerType {
        NAMEDFIELDS, RDF, NO_CONTENT
    }
}
