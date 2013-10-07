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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Basic Indexer implementation that writes object content to timestamped files
 * on disk.
 *
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
**/
public class FileSerializer implements Indexer {

    private static SimpleDateFormat fmt
        = new SimpleDateFormat("yyyyMMddHHmmss");
    private File path;

    /**
     * Set path to write files.
    **/
    public void setPath( String pathName ) {
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
    **/
    public void update(String pid, String content) throws IOException {
        // timestamped filename
        String fn = pid + "." + fmt.format( new Date() );
        if ( fn.indexOf("/") != -1 ) {
            fn = StringUtils.substringAfterLast(fn, "/");
        }

        // write content to disk
        FileWriter fw = null;
        try {
            fw = new FileWriter( new File(path,fn) );
            IOUtils.write( content, fw );
        } finally {
            fw.close();
        }
    }

    /**
     * Remove the object from the index.
    **/
    public void remove(String pid) throws IOException {
        update(pid,""); // empty update
    }
}
