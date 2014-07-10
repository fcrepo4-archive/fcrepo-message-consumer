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

import static com.google.common.base.Throwables.propagate;
import static org.fcrepo.indexer.Indexer.IndexerType.JCRXML_PERSISTENCE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
     * Set path to write files.
     *
     * @param pathName
     */
    public void setPath( final String pathName ) {
        this.path = new File(pathName);
        if (!this.path.exists()) {
            this.path.mkdirs();
        }
    }
    /**
     * Return path where files are written.
     *
     * @return
     */
    public String getPath() {
        return path.getAbsolutePath();
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

        // file name with object identifier
        final String fileName = id.substring(id.lastIndexOf("/") + 1) + "-jcr.xml";

        final File file = new File(path, fileName);
        LOGGER.debug("Updating {} to file: {}", id, file);
        return new Callable<File>() {

            @Override
            public File call() {
                // write content to disk
                FileOutputStream out = null;
                final byte[] buf = new byte[4096];
                int len = 0;
                try {
                    out = new FileOutputStream(file);
                    while ((len = content.read(buf)) > 0 ) {
                        out.write(buf, 0, len);
                    }
                } catch (final IOException e) {
                    LOGGER.error("Failed to write to file: {}", file);
                    propagate(e);
                } finally {
                    if (out != null) {
                        try {
                            out.close();
                        } catch (final IOException e) {
                        }
                    }
                }
                return file;
            }
        };
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
