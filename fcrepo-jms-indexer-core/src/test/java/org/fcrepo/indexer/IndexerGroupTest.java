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

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.fcrepo.kernel.utils.EventType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Set;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static org.fcrepo.kernel.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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

    private String repoUrl;

    @Mock
    private HttpClient httpClient;

    private Set<Indexer<Object>> indexers;

    @Mock
    private Indexer<Object> indexer;

    @Before
    public void setUp() {
        initMocks(this);

        repoUrl = "fake:fake";

        indexers = new HashSet<Indexer<Object>>();
        indexers.add(indexer);

        indexerGroup = new IndexerGroup();
        indexerGroup.setHttpClient(httpClient);
        indexerGroup.setIndexers(indexers);
        indexerGroup.setRepositoryURL(repoUrl);
    }

    @Test
    public void testGetters() {
        assertEquals(repoUrl, indexerGroup.getRepositoryURL());
        assertEquals(indexers, indexerGroup.getIndexers());
        assertEquals(httpClient, indexerGroup.getHttpClient());
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
        String id = "/test";
        indexerGroup.onMessage(createUnindexableMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(NODE_ADDED).toString(), id));
        verify(indexer, never()).update(anyString(), any());
    }

    @Test
    public void testNamedFieldsIndexableObjectUpdateMessage() throws Exception {
        when(indexer.getIndexerType()).thenReturn(Indexer.IndexerType.NAMEDFIELDS);
        String id = "/test";
        indexerGroup.onMessage(createIndexableMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(NODE_ADDED).toString(), id));
        verify(indexer, atLeastOnce()).update(anyString(), any());
    }

    @Test
    public void testRDFIndexablePropertyUpdateMessage() throws Exception {
        when(indexer.getIndexerType()).thenReturn(Indexer.IndexerType.RDF);
        String id = "/test/dc:title";
        indexerGroup.onMessage(createIndexablePropertyMessage(REPOSITORY_NAMESPACE
                + EventType.valueOf(PROPERTY_CHANGED).toString(), id));
        verify(indexer, atLeastOnce()).update(anyString(), any());
    }

    private Message createUnindexableMessage(String eventType, String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, false, null, false);
    }

    private Message createIndexableMessage(String eventType, String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, true, "default", false);
    }

    private Message createIndexablePropertyMessage(String eventType, String identifier) throws Exception {
        return createMockMessage(false, eventType, identifier, true, "default", true);
    }

    private Message createBrokenMessage() throws Exception {
        return createMockMessage(true, null, null, false, null, false);
    }

    /**
     * Creates a mock message an updates the mock HTTPClient to respond to
     * the expected request for that object.
     */
    private Message createMockMessage(boolean jmsExceptionOnGetMessage, String eventType, String identifier, boolean indexable, String indexerName, boolean property) throws Exception {
        Message m = mock(Message.class);
        if (jmsExceptionOnGetMessage) {
            JMSException e = mock(JMSException.class);
            when(m.getJMSMessageID()).thenThrow(e);
        } else {
            when(m.getJMSMessageID()).thenReturn("mocked-message-id");
        }
        if (eventType != null) {
            when(m.getStringProperty(IndexerGroup.EVENT_TYPE_HEADER_NAME)).thenReturn(eventType);
        }
        if (identifier != null) {
            when(m.getStringProperty(IndexerGroup.IDENTIFIER_HEADER_NAME)).thenReturn(identifier);
            final HttpResponse r = mock(HttpResponse.class);
            final StatusLine s = mock(StatusLine.class);
            when(s.getStatusCode()).thenReturn(HttpStatus.SC_OK);
            when(r.getStatusLine()).thenReturn(s);
            final HttpEntity e = mock(HttpEntity.class);
            when(e.getContent()).thenReturn(new ByteArrayInputStream(getIndexableTriples(property ? parentId(identifier) : identifier, indexable, indexerName).getBytes("UTF-8")));
            when(r.getEntity()).thenReturn(e);
            when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(r);
        }
        return m;
    }

    private String parentId(String identifier) {
        return identifier.substring(0, identifier.lastIndexOf('/'));
    }

    private String getIndexableTriples(String id, boolean indexable, String indexerName) {
        return "\n" +
                "<" + repoUrl + id + "> a <http://fedora.info/definitions/v4/rest-api#resource> , <http://fedora.info/definitions/v4/rest-api#object> ;\n" +
                "\t<http://fedora.info/definitions/v4/repository#primaryType> \"nt:folder\"^^<http://www.w3.org/2001/XMLSchema#string> ;\n" +
                (indexerName != null ? "\t<http://fedora.info/definitions/v4/indexing#hasIndexingTransformation> \"" + indexerName + "\"^^<http://www.w3.org/2001/XMLSchema#string> ;\n" : "") +
                "\t<http://fedora.info/definitions/v4/repository#uuid> \"b1bfd6b8-b821-48c5-8eb9-05ef47e1b6e6\"^^<http://www.w3.org/2001/XMLSchema#string> ;\n" +
                "\ta " + (indexable ? "<http://fedora.info/definitions/v4/indexing#indexable> , " : "") + "<http://fedora.info/definitions/v4/rest-api#resource> , <http://fedora.info/definitions/v4/rest-api#object> .\n";
    }
}
