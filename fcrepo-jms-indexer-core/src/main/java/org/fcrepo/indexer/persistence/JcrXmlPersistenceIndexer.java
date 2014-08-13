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
package org.fcrepo.indexer.persistence;

import static org.fcrepo.indexer.Indexer.IndexerType.JCRXML_PERSISTENCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.fcrepo.indexer.SynchIndexer;
import org.slf4j.Logger;

/**
 * Basic Indexer implementation to write contents from an InputStream files on disk.
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
 * @author lsitu
**/
public class JcrXmlPersistenceIndexer extends SynchIndexer<InputStream, File> {

    private static final Logger LOGGER = getLogger(JcrXmlPersistenceIndexer.class);

    private File path = null;

    /**
     * Constructor
     * @param pathName of directory in which jcr/xml exports will be stored
     */
    public JcrXmlPersistenceIndexer(final String pathName) {
        this.path = new File(pathName);
        if (!this.path.exists()) {
            this.path.mkdirs();
        }
    }

    /**
     * Create/update an index entry for the object.
     * @param id
     * @param content
     * @return
    **/
    @Override
    public Callable<File> updateSynch(final String idPath, final InputStream content) {

        if (idPath.endsWith("/")) {
            throw new IllegalArgumentException(
                    "Identifiers for use with this indexer may not end in '/'!");
        }

        return new Callable<File>() {

            @Override
            public File call() throws IOException {
                final Path dir = Paths.get(getPath(), idPath);
                if (Files.notExists(dir, LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(Paths.get(getPath(), idPath));
                }

                // file name with object identifier
                final String fileName = dir.getFileName() + "-jcr.xml";

                // write content to disk
                final Path p = Paths.get(dir.toString(), fileName);

                LOGGER.debug("Updating {} to file: {}", idPath, p.toAbsolutePath().toString());

                Files.copy(content, p, new CopyOption[]{});
                return p.toFile();
            }
        };
    }

    private String getPath() {
        return path.getAbsolutePath();
    }

    /**
     * Remove the object from the file system.
    **/
    @Override
    public Callable<File> removeSynch(final String idPath) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", idPath);
        return updateSynch(idPath, new ByteArrayInputStream("".getBytes()));
    }

    @Override
    public IndexerType getIndexerType() {
        return JCRXML_PERSISTENCE;
    }
}
