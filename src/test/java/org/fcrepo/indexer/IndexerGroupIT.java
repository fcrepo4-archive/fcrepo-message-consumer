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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.abdera.model.Text.Type.TEXT;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import javax.inject.Inject;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Entry;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class IndexerGroupIT {

    protected static final int SERVER_PORT = Integer.parseInt(System
            .getProperty("test.port", "8080"));

    protected static final String serverAddress = "http://localhost:" +
            SERVER_PORT + "/rest/objects/";

    protected final PoolingClientConnectionManager connectionManager =
            new PoolingClientConnectionManager();

    protected static HttpClient client;

    private static String TEST_PID = "changeme_1001";

    private static SimpleDateFormat fmt = new SimpleDateFormat("HHmmssSSS");

    private Connection connection;

    @Inject
    private IndexerGroup indexerGroup;
    @Inject
    private SparqlIndexer sparqlIndexer;
    @Inject
    private FileSerializer fileSerializer;
    private File fileSerializerPath;

    private static TextMessage getMessage(String operation)
            throws JMSException {
        Abdera abdera = new Abdera();

        Entry entry = abdera.newEntry();
        entry.setTitle(operation, TEXT)
                .setBaseUri("http://localhost:8080/rest");
        entry.addCategory("xsd:string", TEST_PID, "fedora-types:pid");
        entry.setContent("contentds");
        StringWriter writer = new StringWriter();
        try {
            entry.writeTo(writer);
        } catch (IOException e) {
            // hush
        }
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
    public void indexerGroupUpdateTest() throws IOException, JMSException {
        // create dummy object
        final HttpPost method = new HttpPost(serverAddress + TEST_PID);
        final HttpResponse response = client.execute(method);
        assertEquals(201, response.getStatusLine().getStatusCode());

        try {
          Thread.sleep(1500); // wait for message to be processed
        } catch ( Exception ex ) { }

        // file should exist and contain data
        File[] files = fileSerializerPath.listFiles();
        assertNotNull(files);
        assertTrue("There should be 1 file", files.length == 1);

        File f = fileSerializerPath.listFiles()[0];
        assertTrue("Filename doesn't match: " + f.getAbsolutePath(),
                f.getName().startsWith(TEST_PID) );
        assertTrue("File size too small: " + f.length(), f.length() > 500);

        // triples should exist in the triplestore
        assertTrue("Triples should exist",
                sparqlIndexer.countTriples(TEST_PID) > 0 );
    }

    @Test
    public void indexerGroupDeleteTest() throws IOException, JMSException {
        // delete dummy object
        final HttpDelete method = new HttpDelete(serverAddress + TEST_PID);
        final HttpResponse response = client.execute(method);
        assertEquals(204, response.getStatusLine().getStatusCode());

        // create update message and send to indexer group
        Message deleteMessage = getMessage("purgeObject");
        indexerGroup.onMessage( deleteMessage );

        try {
          Thread.sleep(5000); // wait for message to be processed
        } catch ( Exception ex ) { }

        // two files should exist: one empty and one with data
        File[] files = fileSerializerPath.listFiles();
        assertNotNull(files);
        assertTrue("There should be 2 files", files.length == 2);

        Arrays.sort(files); // sort files by filename (i.e., creation time)
        File f1 = files[0];
        File f2 = files[1];
        assertTrue("Filename doesn't match: " + f1.getAbsolutePath(),
                f1.getName().startsWith(TEST_PID) );
        assertTrue("File size too small: " + f1.length(), f1.length() > 500);
        assertTrue("Filename doesn't match: " + f2.getAbsolutePath(),
                f2.getName().startsWith(TEST_PID) );
        assertTrue("File size should be 0: " + f2.length(), f2.length() == 0);

        // triples should not exist in the triplestore
        assertTrue("Triples should not exist",
                sparqlIndexer.countTriples(TEST_PID) == 0 );
    }
}
