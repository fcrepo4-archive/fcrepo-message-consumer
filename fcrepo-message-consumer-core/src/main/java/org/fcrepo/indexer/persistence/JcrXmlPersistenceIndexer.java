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
package org.fcrepo.indexer.persistence;

import static org.fcrepo.indexer.Indexer.IndexerType.JCRXML_PERSISTENCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

/**
 * Basic Indexer implementation to write contents from an InputStream files on disk.
 * @author ajs6f
 * @author Esmé Cowles
 * @since Aug 19, 2013
 * @author lsitu
**/
public class JcrXmlPersistenceIndexer extends BasePersistenceIndexer<InputStream, File> {

    private static final Logger LOGGER = getLogger(JcrXmlPersistenceIndexer.class);

    /**
     * Constructor
     * @param pathName of directory in which jcr/xml exports will be stored
     */
    public JcrXmlPersistenceIndexer(final String pathName) {
        super(pathName, ".jcr.xml");
    }

    /**
     * Create/update an index entry for the object.
     * @param id The object's URI
     * @param content InputStream containing JCR/XML content
     * @return The file where the content was written.
    **/
    @Override
    public Callable<File> updateSynch(final URI id, final InputStream content) {
        if (id.toString().endsWith("/")) {
            throw new IllegalArgumentException("Identifiers for use with this indexer may not end in '/'!");
        }

        return new Callable<File>() {
            @Override
            public File call() throws IOException {
                final Path p = pathFor(id);
                LOGGER.debug("Updating {} to file: {}", id, p.toAbsolutePath().toString());
                Files.copy(content, p, new CopyOption[]{});
                return p.toFile();
            }
        };
    }

    /**
     * Remove the object from the file system.
     * @param id The object's URI
    **/
    @Override
    public Callable<File> removeSynch(final URI id) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", id);
        return updateSynch(id, new ByteArrayInputStream("".getBytes()));
    }

    @Override
    public IndexerType getIndexerType() {
        return JCRXML_PERSISTENCE;
    }
}
