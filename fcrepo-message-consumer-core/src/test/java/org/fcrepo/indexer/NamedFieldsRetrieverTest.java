/**
 * Copyright 2015 DuraSpace, Inc.
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

import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static com.hp.hpl.jena.graph.Triple.create;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.http.Header;
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

/**
 * @author ajs6f
 */
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
    public void testShouldntRetrieve() throws URISyntaxException {
        final String testUri = "http://example.com/testShouldntRetrieve";
        final Model mockRdf =
            createDefaultModel().add(
                    createDefaultModel().asStatement(testTriple));
        when(mockRetriever.get()).thenReturn(mockRdf);
        new NamedFieldsRetriever(new URI(testUri), mockClient, mockRetriever).get();

    }

    @Test(expected = RuntimeException.class)
    public void testBadTransform() throws Exception {
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
        new NamedFieldsRetriever(new URI(testUri), mockClient, mockRetriever).get();

    }

    @Test
    public void testGoodTransform() throws Exception {
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
            new NamedFieldsRetriever(new URI(testUri), mockClient, mockRetriever).get();
        LOGGER.debug("Received results: {}", results);
        assertEquals(testUri, results.get("id").iterator().next());
    }

    @Test
    public void testTransformPropertyLookup() throws Exception {
        final String testUri = "indexing:goodTransform";
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

        final Model mockModel = mock(Model.class);
        when(mockRetriever.get()).thenReturn(mockModel);
        when(mockModel.contains(any(Resource.class), any(Property.class))).thenReturn(false);
        final Header mockHeader = mock(Header.class);
        when(mockHeader.getValue()).thenReturn("<http://example.com/test>; rel=\"describedby\"");
        final Header[] headers = new Header[] {mockHeader};
        when(mockResponse.getHeaders("Link")).thenReturn(headers);

        final RDFNode mockNode = mock(RDFNode.class);
        final NodeIterator mockNoderIterator = mock(NodeIterator.class);
        when(mockModel.listObjectsOfProperty(any(Resource.class), any(Property.class))).thenReturn(mockNoderIterator);
        when(mockNoderIterator.next()).thenReturn(mockNode);
        when(mockNoderIterator.hasNext()).thenReturn(true);
        final Literal mockLiteral = mock(Literal.class);
        when(mockNode.asLiteral()).thenReturn(mockLiteral);
        when(mockNode.asLiteral().getString()).thenReturn("default");
        final NamedFields results =
            new NamedFieldsRetriever(new URI(testUri), mockClient, mockRetriever).get();
        LOGGER.debug("Received results: {}", results);
        assertEquals(testUri, results.get("id").iterator().next());
    }

    @Test (expected = AbsentTransformPropertyException.class)
    public void testTransformPropertyBadLookup() throws Exception {
        final String testUri = "indexing:goodTransform";
        final String testRdf = dc_rdf.replace("<>", "<" + testUri + ">");
        LOGGER.debug("Using test RDF: {}", testRdf);
        try (Reader r = new StringReader(testRdf)) {
            final Model mockRdf = createDefaultModel().read(r, "", "N3");
            when(mockRetriever.get()).thenReturn(mockRdf);
        }

        final Model mockModel = mock(Model.class);
        when(mockRetriever.get()).thenReturn(mockModel);
        when(mockModel.contains(any(Resource.class), any(Property.class))).thenReturn(false);
        final NodeIterator mockIterator = mock(NodeIterator.class);
        when(mockModel.listObjectsOfProperty(any(Resource.class), any(Property.class))).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(false);
        final Header mockHeader = mock(Header.class);
        when(mockHeader.getValue()).thenReturn("<http://example.com/tet>; rel=\"describedby\"");
        final Header[] headers = new Header[] {mockHeader};
        when(mockResponse.getHeaders("Link")).thenReturn(headers);
        new NamedFieldsRetriever(new URI(testUri), mockClient, mockRetriever).get();
    }

    private static final Logger LOGGER =
        getLogger(NamedFieldsRetrieverTest.class);

}
