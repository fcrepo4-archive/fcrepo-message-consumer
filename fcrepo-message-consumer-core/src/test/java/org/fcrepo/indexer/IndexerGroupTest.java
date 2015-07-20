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

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.fcrepo.kernel.api.utils.EventType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static org.fcrepo.kernel.api.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author Michael Durbin
 */
public class IndexerGroupTest {

    private IndexerGroup indexerGroup;

    private String repoUrl = "http://example.org:80";

    @Mock
    private DefaultHttpClient httpClient;

    private Set<Indexer<Object>> indexers;

    @Mock
    private Indexer<Object> indexer;

    @Before
    public void setUp() {
        initMocks(this);

        indexers = new HashSet<>();
        indexers.add(indexer);

        indexerGroup = new IndexerGroup(indexers, httpClient);
    }

    @Test
    public void testSanityConstructor() {
        indexerGroup = new IndexerGroup(indexers, "user", "pass");
        assertEquals(indexers, indexerGroup.indexers);
    }

    @Test
    public void testHttpClient() {
        final IndexerGroup indexer = new IndexerGroup(null, "user", "pass");
        final DefaultHttpClient client = indexer.httpClient("http://example.org:80");
        assertNotNull(client);

        final CredentialsProvider provider = client.getCredentialsProvider();
        final Credentials credentials = provider.getCredentials(new AuthScope("example.org", 80));
        assertNotNull("Credentials should not be null!", credentials);

        assertEquals("user", credentials.getUserPrincipal().getName());
        assertEquals("pass", credentials.getPassword());
    }

    @Test
    public void testInvalidMessage() {
        try {
            indexerGroup.onMessage(createBrokenMessage());
            fail("Broken message should result in an exception!");
        } catch (Exception e) {
        }
    }

    @Test
    public void testNonIndexableObjectUpdateMessage() throws Exception {
        final String id = "/test1";
        indexerGroup.onMessage(createUnindexableMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(NODE_ADDED).toString(), id));
        verify(indexer, never()).update(any(URI.class), any());
    }

    @Test
    public void testNamedFieldsIndexableObjectUpdateMessage() throws Exception {
        when(indexer.getIndexerType()).thenReturn(Indexer.IndexerType.NAMEDFIELDS);
        final String id = "/test2";
        indexerGroup.onMessage(createIndexableMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(NODE_ADDED).toString(), id));
        verify(indexer, atLeastOnce()).update(any(URI.class), any());
    }

    @Test
    public void testRDFIndexablePropertyUpdateMessage() throws Exception {
        when(indexer.getIndexerType()).thenReturn(Indexer.IndexerType.RDF);
        final String id = "/test3";
        indexerGroup.onMessage(createIndexablePropertyMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(PROPERTY_CHANGED).toString(), id));
        verify(indexer, atLeastOnce()).update(any(URI.class), any());
    }

    @Test
    public void testReindex() throws Exception {
        mockContent("", true, null);
        when(indexer.getIndexerType()).thenReturn(Indexer.IndexerType.RDF);
        indexerGroup.reindex(new URI(repoUrl),true);
        verify(indexer,atLeastOnce()).update(eq(new URI(repoUrl)), any());
    }

    private Message createUnindexableMessage(final String eventType, final String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, false, null, false);
    }

    private Message createIndexableMessage(final String eventType, final String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, true, "default", false);
    }

    private Message createIndexablePropertyMessage(final String eventType, final String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, true, "default", true);
    }

    private Message createBrokenMessage() throws Exception {
        return createMockMessage(true, null, null, false, null, false);
    }

    /**
     * Creates a mock message an updates the mock HTTPClient to respond to
     * the expected request for that object.
     */
    private Message createMockMessage(final boolean jmsExceptionOnGetMessage,
                                      final String eventType,
                                      final String identifier,
                                      final boolean indexable,
                                      final String indexerName,
                                      final boolean property) throws Exception {
        final Message m = mock(Message.class);
        if (jmsExceptionOnGetMessage) {
            final JMSException e = mock(JMSException.class);
            when(m.getJMSMessageID()).thenThrow(e);
        } else {
            when(m.getJMSMessageID()).thenReturn("mocked-message-id");
        }
        if (eventType != null) {
            when(m.getStringProperty(IndexerGroup.EVENT_TYPE_HEADER_NAME)).thenReturn(eventType);
        }
        if (identifier != null) {
            when(m.getStringProperty(IndexerGroup.IDENTIFIER_HEADER_NAME)).thenReturn(identifier);
            mockContent(identifier, indexable, indexerName);
        }
        when(m.getStringProperty(IndexerGroup.BASE_URL_HEADER_NAME)).thenReturn("http://example.org:80");
        return m;
    }

    private void mockContent(final String identifier,
                             final boolean indexable,
                             final String indexerName) throws Exception {
        final CloseableHttpResponse r = mock(CloseableHttpResponse.class);
        final StatusLine s = mock(StatusLine.class);
        when(s.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(r.getStatusLine()).thenReturn(s);
        final HttpEntity e = mock(HttpEntity.class);
        when(e.getContent()).thenReturn(
                new ByteArrayInputStream(getIndexableTriples(identifier, indexable, indexerName).getBytes("UTF-8")));
        when(r.getEntity()).thenReturn(e);
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(r);
    }

    private String getIndexableTriples(final String id, final boolean indexable, final String indexerName) {
        return "\n" +
                "<" + repoUrl + id + "> a <http://fedora.info/definitions/v4/repository#Resource> , " +
                "<http://fedora.info/definitions/v4/repository#Container> ;\n" +
                "\t<http://fedora.info/definitions/v4/repository#primaryType> \"nt:folder\"^^<http://www.w3" +
                ".org/2001/XMLSchema#string> ;\n" +
                (indexerName != null ? "\t<http://fedora.info/definitions/v4/indexing#hasIndexingTransformation> \""
                        + indexerName + "\"^^<http://www.w3.org/2001/XMLSchema#string> ;\n" : "") +
                "\t<http://fedora.info/definitions/v4/repository#uuid> " +
                "\"b1bfd6b8-b821-48c5-8eb9-05ef47e1b6e6\"^^<http://www.w3.org/2001/XMLSchema#string> ;\n" +
                "\ta " + (indexable ? "<http://fedora.info/definitions/v4/indexing#Indexable> , " +
                "" : "") + "<http://fedora.info/definitions/v4/repository#Resource> , " +
                "<http://fedora.info/definitions/v4/repository#Container> .\n";
    }
}
