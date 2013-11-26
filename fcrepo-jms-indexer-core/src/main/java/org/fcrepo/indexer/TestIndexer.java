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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Indexer implementation that tracks which PIDs it has received messages for.
 *
 * @author Esmé Cowles
 *         Date: Nov. 25, 2013
**/
public class TestIndexer implements Indexer {
    private final Logger logger = LoggerFactory.getLogger(TestIndexer.class);

    private Set<String> updates;
    private Set<String> removes;

    /**
     * Default constructor.
    **/
    public TestIndexer() {
        updates = new HashSet<String>();
        removes = new HashSet<String>();
    }

    /**
     * Create or update an index entry for the object.
    **/
    public void update(String pid, String content) throws IOException {
        logger.warn( "update: {}", pid);
        updates.add(pid);
    }

    /**
     * Remove the object from the index.
    **/
    public void remove(String pid) throws IOException {
        logger.warn( "remove: {}", pid);
        removes.add(pid);
    }

    /**
     * Test whether an update message has been received for a PID.
    **/
    public boolean receivedUpdate(String pid) {
        logger.warn( "receivedUpdate: {}, {}", pid, updates.contains(pid));
        return updates.contains(pid);
    }

    /**
     * Test whether a remove message has been received for a PID.
    **/
    public boolean receivedRemove(String pid) {
        logger.warn( "receivedRemove: {}, {}", pid, removes.contains(pid));
        return removes.contains(pid);
    }
}
