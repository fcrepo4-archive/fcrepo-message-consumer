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

import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static org.fcrepo.indexer.Indexer.IndexerType.JCRXML_PERSISTENCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Test the FilePesistenceIndexer
 * @author lsitu
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19, 2013
 */
public class JcrXmlPersistenceIndexerTest {

    private static final Logger LOGGER = getLogger(JcrXmlPersistenceIndexerTest.class);

    private JcrXmlPersistenceIndexer indexer;

    private File path;

    private final String testContent =
            "<sv:node xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" sv:name=\"testContent\"></sv:node>";

    @Before
    public void setup() {
        path = new File("./target/persistence");
        indexer = new JcrXmlPersistenceIndexer(path.getAbsolutePath());
    }

    @Test
    public void pathTest() throws IOException {
        // should automatically create directory
        assertTrue("Path not found: " + path.getAbsolutePath() + "!", path.exists());
        assertTrue("Path was not a dirextory: " + path.getAbsolutePath() + "!", path.isDirectory());
    }

    @Test
    public void updateTest() throws IOException, InterruptedException, ExecutionException {
        final String testId = "updateTest" + randomUUID();
        final InputStream input = new ByteArrayInputStream (testContent.getBytes());

        final File f = indexer.update("http://localhost:8080/" + testId, input).get();

        // file should exist
        LOGGER.debug("Got filename: {}", f.getName());
        assertTrue("Filename doesn't match", f.getName().startsWith(testId));

        // content should be 'test content'
        final String content = new String(readAllBytes(f.toPath()));
        assertTrue("Content doesn't contain our property!", content.equals(testContent));
    }

    @Test
    public void updateWithHierarchyPathTest()
            throws IOException, InterruptedException, ExecutionException {
        final String path1 = "updateHier" +  randomUUID();
        final String path2 = "" + randomUUID();
        final String testId = "http://localhost:8080/" + path1 + "/" + path2;
        final InputStream input = new ByteArrayInputStream (testContent.getBytes());

        final File f = indexer.update(testId, input).get();

        // file should exist
        LOGGER.debug("Got filename: {}", f.getName());
        assertTrue("Filename doesn't match", f.getName().startsWith(path2));
        assertTrue("Path Path1 doesn't match", f.getParentFile().getName().equals(path1));
        assertTrue("Path port number doesn't match",
                f.getParentFile().getParentFile().getName().equals("8080"));
        assertTrue("Path hostname doesn't match",
                f.getParentFile().getParentFile().getParentFile().getName().equals("localhost"));

        // content should be 'test content'
        final String content = new String(readAllBytes(f.toPath()));
        assertTrue("Content doesn't contain our property!", content.equals(testContent));
    }

    @Test
    public void updateWithSpecialCharacterPathTest()
            throws IOException, InterruptedException, ExecutionException {
        final String path = "updateHier : \\'\" < >" +  randomUUID();
        final String testId = "http://localhost:8080/" + path;
        final InputStream input = new ByteArrayInputStream (testContent.getBytes());

        final File f = indexer.update(testId, input).get();

        // file should exist
        LOGGER.debug("Got filename: {}", f.getName());
        assertTrue("Filename doesn't match", f.getName().startsWith(URLEncoder.encode(path, "UTF-8")));
        assertTrue("Path port number doesn't match",
                f.getParentFile().getName().equals("8080"));
        assertTrue("Path hostname doesn't match",
                f.getParentFile().getParentFile().getName().equals("localhost"));

        // content should be 'test content'
        final String content = new String(readAllBytes(f.toPath()));
        assertTrue("Content doesn't contain our property!", content.equals(testContent));
    }

    @Test
    public void removeTest() throws IOException, InterruptedException, ExecutionException {
        final String path1 = "removeTest" +  randomUUID();
        final String path2 = "" + randomUUID();
        final String testId = path1 + "/" + path2;
        // should write empty file to disk
        final File f = indexer.remove("http://localhost:8080/" + testId).get();

        // file should exist
        LOGGER.debug("Got filename: {}", f.getName());
        assertTrue("Filename doesn't match", f.getName().startsWith(path2));

        // content should be empty
        final String content = new String(readAllBytes(f.toPath()));
        assertTrue("Content doesn't match", content.isEmpty());
    }

    @Test
    public void testGetIndexerType() {
        assertEquals("Got wrong indexer type!", JCRXML_PERSISTENCE, indexer.getIndexerType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadId() throws IOException {
        final String testId = "testBadId/";
        indexer.update(testId, null);
    }

}
