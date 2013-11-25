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

import static org.apache.commons.io.IOUtils.write;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

/**
 * Basic Indexer implementation that writes object content to timestamped files
 * on disk.
 *
 * @author ajs6f
 * @author Esmé Cowles
 * @date Aug 19, 2013
**/
public class FileSerializer implements Indexer {

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
    public ListenableFuture<File> update(final String pid, final String content) throws IOException {
        // timestamped filename
        String fn = pid + "." + fmt.format(new Date());
        if (fn.indexOf('/') != -1) {
            fn = substringAfterLast(fn, "/");
        }
        final File file = new File(path, fn);
        return run(ListenableFutureTask.create(new Callable<File>() {

            @Override
            public File call() {
                // write content to disk
                try (Writer fw = new FileWriter(file)) {
                    write(content, fw);
                } catch (final IOException ex) {
                    LOGGER.error("Error writing file", ex);
                }
                return file;
            }
        }));

    }


    /**
     * Remove the object from the index.
    **/
    @Override
    public ListenableFuture<File> remove(final String pid) throws IOException {
        // empty update
        return update(pid,"");
    }

    private static <T> ListenableFuture<T> run(
        final ListenableFutureTask<T> task) {
        task.run();
        return task;
    }
}
