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
import static org.fcrepo.indexer.Indexer.NoContent;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;


/**
 * Indexer implementation that tracks which PIDs it has received messages for,
 * but does not actually process content.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Nov 25, 2013
 **/
public class TestIndexer extends SynchIndexer<NoContent, Boolean> {

    private static final Logger LOGGER = getLogger(TestIndexer.class);

    private final Set<String> updates = new HashSet<>();
    private final Set<String> removes = new HashSet<>();

    public void clear() {
        LOGGER.debug("Clearing updates and removes");
        updates.clear();
        removes.clear();
    }

    @Override
    public Callable<Boolean> updateSynch(final String identifier,
        final NoContent content) {
        LOGGER.debug("Received update for identifier: {}", identifier);
        return new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                final Boolean success = updates.add(identifier);
                LOGGER.debug("Current recorded updates include: {}", updates);
                return success;
            }
        };
    }

    @Override
    public Callable<Boolean> removeSynch(final String identifier) {
        LOGGER.debug("Received remove for identifier: {}", identifier);
        return new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    final Boolean success = removes.add(identifier);
                    LOGGER.debug("Current recorded removes include: {}",
                            removes);
                    return success;
                }
            };

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
