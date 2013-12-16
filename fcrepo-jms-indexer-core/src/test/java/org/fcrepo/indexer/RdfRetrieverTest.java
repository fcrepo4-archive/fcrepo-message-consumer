package org.fcrepo.indexer;

import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static com.hp.hpl.jena.graph.Triple.create;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;


public class RdfRetrieverTest {

    private RdfRetriever testRetriever;

    @Mock
    private HttpClient mockClient;

    @Mock
    private HttpResponse mockResponse;

    @Mock
    private HttpEntity mockEntity;

    @Mock
    private StatusLine mockStatusLine;

    @Before
    public void setUp() throws ClientProtocolException, IOException {
        initMocks(this);
        when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(
                mockResponse);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    }

    @Test
    public void testSimpleRetrieval() throws IOException {
        final String testId = "testSimpleRetrieval";
        final Triple testTriple =
            create(createURI("info:test"), createURI("info:test"),
                    createURI("info:test"));
        final Model input = createDefaultModel();
        input.add(input.asStatement(testTriple));
        when(mockStatusLine.getStatusCode()).thenReturn(SC_OK);
        try (StringWriter w = new StringWriter()) {
            input.write(w, "N3");
            try (
                InputStream rdf =
                    new ByteArrayInputStream(w.toString().getBytes())) {
                when(mockEntity.getContent()).thenReturn(rdf);
            }
        }

        testRetriever = new RdfRetriever(testId, mockClient);
        final Model result = testRetriever.get();
        assertTrue("Didn't find our test triple!", result.contains(result
                .asStatement(testTriple)));

    }

    @Test(expected = RuntimeException.class)
    public void testFailedRetrieval(){
        final String testId = "testFailedRetrieval";
        when(mockStatusLine.getStatusCode()).thenReturn(SC_NOT_FOUND);
        new RdfRetriever(testId, mockClient).get();
    }

    @Test(expected = RuntimeException.class)
    public void testOtherFailedRetrieval() throws IOException {
        final String testId = "testFailedRetrieval";
        when(mockEntity.getContent()).thenThrow(new IOException());
        new RdfRetriever(testId, mockClient).get();
    }

}
