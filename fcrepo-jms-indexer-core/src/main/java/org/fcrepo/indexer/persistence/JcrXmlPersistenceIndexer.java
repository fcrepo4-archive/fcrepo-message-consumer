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

import static org.apache.commons.lang.StringUtils.substringAfterLast;
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

import org.apache.commons.lang3.StringUtils;
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
    public Callable<File> updateSynch(final String id, final InputStream content) {

        if (id.endsWith("/")) {
            throw new IllegalArgumentException(
                    "Identifiers for use with this indexer may not end in '/'!");
        }

        return new Callable<File>() {

            @Override
            public File call() throws IOException {
                // strip http protocol and replace column(:)
                String fullPath = id.substring(id.indexOf("//") + 2).replace(":", "/");
                final String idPath = substringAfterLast(fullPath, "/");
                fullPath = StringUtils.substringBeforeLast(fullPath, "/");

                final Path dir = Paths.get(getPath(), fullPath);
                if (Files.notExists(dir, LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(Paths.get(getPath(), fullPath));
                }

                // file name with object identifier
                final String fileName = idPath + ".jcr.xml";

                // write content to disk
                final Path p = Paths.get(dir.toString(), fileName);

                LOGGER.debug("Updating {} to file: {}", fullPath, p.toAbsolutePath().toString());

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
    public Callable<File> removeSynch(final String id) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", id);
        return updateSynch(id, new ByteArrayInputStream("".getBytes()));
    }

    @Override
    public IndexerType getIndexerType() {
        return JCRXML_PERSISTENCE;
    }
}