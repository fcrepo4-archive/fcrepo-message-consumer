package org.fcrepo.indexer;

import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static com.hp.hpl.jena.graph.Triple.create;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;

import com.google.common.io.CharStreams;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;

public class NamedFieldsRetrieverTest {

    @Mock
    private RdfRetriever mockRetriever;

    @Mock
    private HttpClient mockClient;

    @Mock
    private HttpResponse mockResponse;

    @Mock
    private HttpEntity mockEntity;

    @Mock
    private StatusLine mockStatusLine;

    private static final Triple testTriple = create(createURI("info:test"),
            createURI("info:test"), createURI("info:test"));

    private static String dc_rdf;

    static {
        try (
            InputStream in =
                NamedFieldsRetrieverTest.class.getResource(
                        "/rdf/dublin_core.n3").openStream();) {
            dc_rdf = CharStreams.toString(new InputStreamReader(in, "UTF-8"));
        } catch (final IOException e) {
            // unlikely
        }

    }


    @Before
    public void setUp() throws IOException {
        initMocks(this);
        when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(
                mockResponse);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    }

    @Test(expected = AbsentTransformPropertyException.class)
    public void testShouldntRetrieve() {
        final String testUri = "http://example.com/testShouldntRetrieve";
        final Model mockRdf =
            createDefaultModel().add(
                    createDefaultModel().asStatement(testTriple));
        when(mockRetriever.get()).thenReturn(mockRdf);
        new NamedFieldsRetriever(testUri, mockClient, mockRetriever).get();

    }

    @Test(expected = RuntimeException.class)
    public void testBadTransform() throws IOException {
        final String testUri = "indexing:testBadTransform";
        final String testRdf = dc_rdf.replace("<>", "<" + testUri + ">");
        LOGGER.debug("Using test RDF: {}", testRdf);
        try (Reader r = new StringReader(testRdf)) {
            final Model mockRdf = createDefaultModel().read(r, "", "N3");
            when(mockRetriever.get()).thenReturn(mockRdf);
        }

        when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(
                mockResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(SC_NOT_FOUND);
        new NamedFieldsRetriever(testUri, mockClient, mockRetriever).get();

    }

    @Test
    public void testGoodTransform() throws IOException {
        final String testUri = "indexing:testBadTransform";
        final String testRdf = dc_rdf.replace("<>", "<" + testUri + ">");
        LOGGER.debug("Using test RDF: {}", testRdf);
        try (Reader r = new StringReader(testRdf)) {
            final Model mockRdf = createDefaultModel().read(r, "", "N3");
            when(mockRetriever.get()).thenReturn(mockRdf);
        }

        when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(
                mockResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(SC_OK);
        final String fakeJson = "[{\"id\" : [\"" + testUri + "\"]}]";
        LOGGER.debug("Using fake JSON: {}", fakeJson);
        try (
            InputStream mockJson =
                new ByteArrayInputStream(fakeJson.getBytes())) {
            when(mockEntity.getContent()).thenReturn(mockJson);
        }
        final NamedFields results =
            new NamedFieldsRetriever(testUri, mockClient, mockRetriever).get();
        LOGGER.debug("Received results: {}", results);
        assertEquals(testUri, results.get("id").iterator().next());
    }

    private static final Logger LOGGER = getLogger(NamedFieldsRetrieverTest.class);

}
