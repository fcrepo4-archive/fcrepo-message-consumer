
package org.fcrepo.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/master.xml"})
public class FileSerializerIT {

    protected static final int SERVER_PORT = Integer.parseInt(System
            .getProperty("test.port", "8080"));

    protected static final String serverAddress = "http://localhost:" +
            SERVER_PORT + "/rest/objects/";

    protected final PoolingClientConnectionManager connectionManager =
            new PoolingClientConnectionManager();

    protected static HttpClient client;

    final private Logger logger = LoggerFactory
            .getLogger(FileSerializerIT.class);

    private static String TEST_PID = "changeme_1001";

    private static SimpleDateFormat fmt = new SimpleDateFormat("HHmmssSSS");

    @Inject
    private FileSerializer fileSerializer;
    private File fileSerializerPath;

    public FileSerializerIT() {
        client = new DefaultHttpClient(connectionManager);
    }

    @Before
    public void setup() {
        fileSerializerPath = new File("./target/test-classes/fileSerializer."
                + fmt.format(new Date()) );
        fileSerializer.setPath( fileSerializerPath.getAbsolutePath() );
    }

    @Test
    public void indexerGroupTest() throws IOException {
        final HttpPost method = new HttpPost(serverAddress + TEST_PID);
        final HttpResponse response = client.execute(method);
        assertEquals(201, response.getStatusLine().getStatusCode());

        // file should exist and contain triple starting with URI
        //File fileSerializerPath = new File(
        //        "./target/test-classes/fileSerializer");
        File f = fileSerializerPath.listFiles()[0];
        assertTrue("Filename doesn't match: " + f.getAbsolutePath(),
                f.getName().startsWith(TEST_PID) );
    }
}
