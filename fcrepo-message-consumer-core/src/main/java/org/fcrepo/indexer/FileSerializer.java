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

import static com.google.common.base.Throwables.propagate;
import static java.util.Locale.US;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.fcrepo.indexer.Indexer.IndexerType.NAMEDFIELDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

/**
 * Basic Indexer implementation that writes object content to timestamped files
 * on disk.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @since Aug 19, 2013
**/
public class FileSerializer extends SynchIndexer<NamedFields, File> {

    private static final Logger LOGGER = getLogger(FileSerializer.class);

    private static SimpleDateFormat fmt =
        new SimpleDateFormat("yyyyMMddHHmmss", US);

    private File path;

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
     * Create or update an index entry for the object.
     * @return
    **/
    @Override
    public Callable<File> updateSynch(final URI id, final NamedFields content) {

        if (id.toString().endsWith("/")) {
            throw new IllegalArgumentException(
                    "Identifiers for use with this indexer may not end in '/'!");
        }

        // timestamped filename
        String fn = id.toString() + "@" + fmt.format(new Date());
        if (fn.indexOf('/') != -1) {
            fn = substringAfterLast(fn, "/");
        }
        final File file = new File(path, fn);
        LOGGER.debug("Updating to file: {}", file);
        return new Callable<File>() {

            @Override
            public File call() {
                // write content to disk
                try (
                    Writer w =
                        new OutputStreamWriter(new FileOutputStream(file),
                                "UTF8")) {
                    if (content.isEmpty()) {
                        w.write("");
                    } else {
                        w.write(content.toString());
                    }
                } catch (final IOException e) {
                    LOGGER.error("Failed to write to file: {}", file);
                    propagate(e);
                }
                return file;
            }
        };

    }


    /**
     * Remove the object from the index.
    **/
    @Override
    public Callable<File> removeSynch(final URI id) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", id);
        return updateSynch(id, new NamedFields());
    }

    @Override
    public IndexerType getIndexerType() {
        return NAMEDFIELDS;
    }
}
