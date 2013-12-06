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

import static com.google.common.base.Throwables.propagate;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.fcrepo.indexer.Indexer.IndexerType.NAMEDFIELDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import com.google.common.util.concurrent.ListenableFutureTask;

/**
 * Basic Indexer implementation that writes object content to timestamped files
 * on disk.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
**/
public class FileSerializer extends SynchIndexer<File> {

    private static final Logger LOGGER = getLogger(FileSerializer.class);

    private static SimpleDateFormat fmt =
        new SimpleDateFormat("yyyyMMddHHmmss");

    private File path;

    /**
     * Set path to write files.
    **/
    public void setPath( final String pathName ) {
        this.path = new File(pathName);
        if (!this.path.exists()) {
            this.path.mkdirs();
        }
    }
    /**
     * Return path where files are written.
    **/
    public String getPath() {
        return path.getAbsolutePath();
    }

    /**
     * Create or update an index entry for the object.
     * @return
    **/
    @Override
    public ListenableFutureTask<File> updateSynch(final String pid, final Reader content) {
        // timestamped filename
        String fn = pid + "." + fmt.format(new Date());
        if (fn.indexOf('/') != -1) {
            fn = substringAfterLast(fn, "/");
        }
        final File file = new File(path, fn);
        LOGGER.debug("Updating to file: {}", file);
        return ListenableFutureTask.create(new Callable<File>() {

            @Override
            public File call() {
                // write content to disk
                try (Writer w = new FileWriter(file)) {
                    IOUtils.copy(content, w);
                } catch (final IOException e) {
                    propagate(e);
                }
                return file;
            }
        });

    }


    /**
     * Remove the object from the index.
    **/
    @Override
    public ListenableFutureTask<File> removeSynch(final String id) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", id);
        return updateSynch(id, new StringReader(""));
    }

    @Override
    public IndexerType getIndexerType() {
        return NAMEDFIELDS;
    }
}
