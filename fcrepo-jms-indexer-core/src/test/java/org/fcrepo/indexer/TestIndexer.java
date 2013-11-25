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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;


/**
 * Indexer implementation that tracks which PIDs it has received messages for.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Nov 25, 2013
**/
public class TestIndexer implements Indexer {

    private static final Logger LOGGER = getLogger(TestIndexer.class);

    private final Set<String> updates = new HashSet<>();
    private final Set<String> removes = new HashSet<>();


    /**
     * Create or update an index entry for the object.
     *
     * @return
     **/
    @Override
    public ListenableFuture<Boolean> update(final String pid,
            final String content) throws IOException {
        LOGGER.debug("Received update for: {}", pid);
        final ListenableFutureTask<Boolean> result = ListenableFutureTask.create(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return updates.add(pid);
            }
        });
        result.run();
        synchronized(this) {
            notifyAll();
            return result;
        }
    }

    /**
     * Remove the object from the index.
     * @return
    **/
    @Override
    public ListenableFuture<Boolean> remove(final String pid)
        throws IOException {
        LOGGER.debug("Received remove for: {}", pid);
        final ListenableFutureTask<Boolean> result = ListenableFutureTask.create(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return removes.add(pid);
            }
        });
        result.run();
        synchronized (this) {
            notifyAll();
            return result;
        }

    }

    /**
     * Test whether an update message has been received for a PID.
    **/
    public boolean receivedUpdate(final String pid) {
        LOGGER.debug("Checked whether we received an update for: {}, {}", pid,
                updates.contains(pid));
        return updates.contains(pid);
    }

    /**
     * Test whether a remove message has been received for a PID.
    **/
    public boolean receivedRemove(final String pid) {
        LOGGER.debug("Checked whether we received a remove for: {}, {}", pid,
                removes.contains(pid));
        return removes.contains(pid);
    }
}
