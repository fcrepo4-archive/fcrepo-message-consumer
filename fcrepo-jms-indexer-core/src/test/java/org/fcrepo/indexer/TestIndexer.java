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

import static org.fcrepo.indexer.Indexer.IndexerType.NO_CONTENT;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Reader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.ListenableFutureTask;


/**
 * Indexer implementation that tracks which PIDs it has received messages for.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Nov 25, 2013
**/
public class TestIndexer extends SynchIndexer<Boolean> {

    private static final Logger LOGGER = getLogger(TestIndexer.class);

    private final Set<String> updates = new HashSet<>();
    private final Set<String> removes = new HashSet<>();

    @Override
    public ListenableFutureTask<Boolean> updateSynch(final String identifier,
        final Reader content) {
        LOGGER.debug("Received update for identifier: {}", identifier);
        return ListenableFutureTask.create(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                final Boolean success = updates.add(identifier);
                LOGGER.debug("Current recorded updates include: {}", updates);
                return success;
            }
        });
    }

    @Override
    public ListenableFutureTask<Boolean>
    removeSynch(final String identifier) {
        LOGGER.debug("Received remove for identifier: {}", identifier);
        return ListenableFutureTask.create(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    final Boolean success = removes.add(identifier);
                    LOGGER.debug("Current recorded removes include: {}",
                            removes);
                    return success;
                }
            });

    }

    /**
     * Test whether an update message has been received for a PID.
    **/
    public boolean receivedUpdate(final String id) {
        LOGGER.debug("Checked whether we received an update for: {}, {}", id,
                updates.contains(id));
        return updates.contains(id);
    }

    /**
     * Test whether a remove message has been received for a PID.
    **/
    public boolean receivedRemove(final String id) {
        LOGGER.debug("Checked whether we received a remove for: {}, {}", id,
                removes.contains(id));
        return removes.contains(id);
    }

    @Override
    public IndexerType getIndexerType() {
        return NO_CONTENT;
    }

}
