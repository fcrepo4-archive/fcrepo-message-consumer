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

import static org.apache.commons.io.filefilter.FileFilterUtils.prefixFileFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.apache.abdera.model.Text.Type.TEXT;

import java.io.File;
import java.io.FilenameFilter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.iq80.leveldb.util.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.Message;
import javax.jms.TextMessage;
import javax.inject.Inject;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Entry;

/**
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class IndexerGroupIT {

    private static final int SERVER_PORT = Integer.parseInt(System
            .getProperty("test.port", "8080"));

    private static final String serverAddress = "http://localhost:"
            + SERVER_PORT + "/";

    private final PoolingClientConnectionManager connectionManager =
            new PoolingClientConnectionManager();

    private static HttpClient client;

    @Inject
    private IndexerGroup indexerGroup;
    @Inject
    private SparqlIndexer sparqlIndexer;
    @Inject
    private FileSerializer fileSerializer;
    private File fileSerializerPath;

    private static TextMessage getMessage(String operation,
                                          String pid) throws Exception {
        Abdera abdera = new Abdera();

        Entry entry = abdera.newEntry();
        entry.setTitle(operation, TEXT).setBaseUri(serverAddress);
        entry.addCategory("xsd:string", pid, "fedora-types:pid");
        entry.addCategory("xsd:string", "/" + pid, "path");
        entry.setContent("contentds");
        StringWriter writer = new StringWriter();
        entry.writeTo(writer);

        String atomMessage = writer.toString();

        TextMessage msg = mock(TextMessage.class);
        when(msg.getText()).thenReturn(atomMessage);
        return msg;
    }

    @Before
    public void setup() {
        client = new DefaultHttpClient(connectionManager);
        fileSerializerPath = new File(fileSerializer.getPath());
    }

    @Test
    public void indexerGroupUpdateTest() throws Exception {
        doIndexerGroupUpdateTest("test_pid_0");
    }

    private void doIndexerGroupUpdateTest(final String pid) throws Exception {
        // create dummy object
        final HttpPost method = new HttpPost(serverAddress + pid);
        final HttpResponse response = client.execute(method);
//        assertEquals(201, response.getStatusLine().getStatusCode());

        FilenameFilter filter = prefixFileFilter(pid);
        waitForFiles(1, filter); // wait for message to be processed

        // file should exist and contain data
        File[] files = fileSerializerPath.listFiles(filter);
        assertNotNull(files);
        assertTrue("There should be 1 file", files.length == 1);

        File f = files[0];
        assertTrue("Filename doesn't match: " + f.getAbsolutePath(),
                   f.getName().startsWith(pid));
        assertTrue("File size too small: " + f.length(), f.length() > 500);

        final int expectedTriples = 4;
        waitForTriples(expectedTriples, pid);
        
        // triples should exist in the triplestore
        assertTrue("Triples should exist",
                sparqlIndexer.countTriples(serverAddress + pid) == expectedTriples);
    }    

    @Test
    public void indexerGroupDeleteTest() throws Exception {
        // create and verify dummy object
        final String pid = "test_pid_5";
        doIndexerGroupUpdateTest(pid);

        Thread.sleep(1200); // Let the creation event persist

        // delete dummy object
        final HttpDelete method = new HttpDelete(serverAddress + pid);
        final HttpResponse response = client.execute(method);
        assertEquals(204, response.getStatusLine().getStatusCode());
        
        // create update message and send to indexer group
        Message deleteMessage = getMessage("purgeObject", pid);
        indexerGroup.onMessage( deleteMessage );

        FilenameFilter filter = prefixFileFilter(pid);
        waitForFiles(2, filter); // wait for message to be processed

        // two files should exist: one empty and one with data
        File[] files = fileSerializerPath.listFiles(filter);
        
        assertNotNull(files);
        assertEquals(2, files.length);

        Arrays.sort(files); // sort files by filename (i.e., creation time)
        File f1 = files[0];
        File f2 = files[1];
        assertTrue("Filename doesn't match: " + f1.getAbsolutePath(),
                f1.getName().startsWith(pid) );
        assertTrue("File size too small: " + f1.length(), f1.length() > 500);
        assertTrue("Filename doesn't match: " + f2.getAbsolutePath(),
                f2.getName().startsWith(pid) );
        assertTrue("File size should be 0: " + f2.length(), f2.length() == 0);

        final int expectedTriples = 0;
        waitForTriples(expectedTriples, pid);
        
        // triples should not exist in the triplestore
        assertTrue("Triples should not exist",
                sparqlIndexer.countTriples(serverAddress + pid) == expectedTriples);
    }    
    
    @Test
    public void indexerGroupUpdateTestingFullPath() throws Exception {
    	  // create update message and send to indexer group
        final String pid = "test_pid_10";
        final String SUFFIX = "a/b/c/";        
       
        // create dummy object
        final HttpPost method = new HttpPost(serverAddress + SUFFIX +  pid);
        final HttpResponse response = client.execute(method);
//        assertEquals(201, response.getStatusLine().getStatusCode());

        FilenameFilter filter = prefixFileFilter(pid);
        waitForFiles(1, filter); // wait for message to be processed

        // file should exist and contain data
        File[] files = fileSerializerPath.listFiles(filter);
        assertNotNull(files);
        assertTrue("There should be 1 file", files.length == 1);

        File f = files[0];
        assertTrue("Filename doesn't match: " + f.getAbsolutePath(),
                   f.getName().startsWith(pid));
        assertTrue("File size too small: " + f.length(), f.length() > 500);
        
        final int expectedTriples = 4;
        waitForTriples(expectedTriples, SUFFIX + pid);
        
        // triples should exist in the triplestore
        assertTrue("Triples should exist",
                sparqlIndexer.countTriples(serverAddress + SUFFIX + pid) == expectedTriples);
    }
    	
    private void waitForFiles(int expectedFiles, FilenameFilter filter) throws InterruptedException {
        long elapsed = 0;
        long restingWait = 500;
        long maxWait = 15000; // 15 seconds

        List<File> files = FileUtils.listFiles(fileSerializerPath, filter);
        while (expectedFiles != files.size() && (elapsed < maxWait)) {
            Thread.sleep(restingWait);
            files = FileUtils.listFiles(fileSerializerPath, filter);

            elapsed += restingWait;
        }
    }

    private void waitForTriples(int expectTriples, String pid) throws InterruptedException {
        long elapsed = 0;
        long restingWait = 1500;
        long maxWait = 15000; // 15 seconds

        int count = sparqlIndexer.countTriples(pid);
        while ((count != expectTriples) && (elapsed < maxWait)) {
            Thread.sleep(restingWait);
            count = sparqlIndexer.countTriples(pid);

            elapsed += restingWait;
        }
    }

}
