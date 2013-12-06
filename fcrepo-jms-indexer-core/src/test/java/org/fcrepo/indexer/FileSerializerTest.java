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
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.text.SimpleDateFormat;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19, 2013
 */
public class FileSerializerTest {
    private static SimpleDateFormat fmt = new SimpleDateFormat("HHmmssSSS");
    private FileSerializer serializer;
    private File path;

    @Before
    public void setup() {
        path = new File("./target/fileSerializer." + fmt.format(new Date()));
        serializer = new FileSerializer();
        serializer.setPath(path.getAbsolutePath());
    }

    @Test
    public void pathTest() throws IOException {
        // should automatically create path
        assertTrue("Path not found: " + path.getAbsolutePath(), path.exists());
        assertTrue("Not a dir: " + path.getAbsolutePath(), path.isDirectory());
    }

    @Test
    public void updateTest() throws IOException, InterruptedException, ExecutionException {
        serializer.update("abc123", new StringReader("test content"));

        // file should exist
        final File f = path.listFiles()[0];
        assertTrue("Filename doesn't match", f.getName().startsWith("abc123"));

        // content should be 'test content'
        final String content = new String( Files.readAllBytes(f.toPath()) );
        assertEquals("Content doesn't match", content, "test content");
    }

    @Test
    public void removeTest() throws IOException {
        // should write empty file to disk
        serializer.remove("def456");

        // file should exist
        final File f = path.listFiles()[0];
        assertTrue("Filename doesn't match", f.getName().startsWith("def456"));

        // content should be ''
        final String content = new String( Files.readAllBytes(f.toPath()) );
        assertEquals("Content doesn't match", content, "");
    }
}
