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
package org.fcrepo.indexer;

import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Test retrieve jcr/xml from the repository
 * @author ajs6
 * @author lsitu
 */
public class JcrXmlRetrieverTest {

    private JcrXmlRetriever testRetriever;

    @Mock
    private HttpClient mockClient;

    @Mock
    private HttpResponse mockResponse;

    @Mock
    private HttpEntity mockEntity;

    @Mock
    private StatusLine mockStatusLine;

    private final String testContent =
            "<sv:node xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" sv:name=\"testContent\"></sv:node>";

    @Before
    public void setUp() throws IOException {
        initMocks(this);
        when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    }

    @Test
    public void testSimpleRetrieval() throws Exception {
        final String testId = "testSimpleRetrieval";
        final InputStream input = new ByteArrayInputStream (testContent.getBytes());
        when(mockStatusLine.getStatusCode()).thenReturn(SC_OK);
        when(mockEntity.getContent()).thenReturn(input);

        testRetriever = new JcrXmlRetriever(new URI(testId), mockClient);
        final InputStream result = testRetriever.get();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        int ch;
        while ((ch = result.read()) != -1) {
            out.write(ch);
        }
        out.close();
        result.close();
        assertTrue("Didn't find our test triple!", out.toString().equals(testContent));
    }

    @Test(expected = RuntimeException.class)
    public void testFailedRetrieval() throws URISyntaxException {
        final String testId = "testFailedRetrieval";
        when(mockStatusLine.getStatusCode()).thenReturn(SC_NOT_FOUND);
        new JcrXmlRetriever(new URI(testId), mockClient).get();
    }

    @Test(expected = RuntimeException.class)
    public void testOtherFailedRetrieval() throws Exception {
        final String testId = "testFailedRetrieval";
        when(mockStatusLine.getStatusCode()).thenReturn(SC_OK);
        when(mockEntity.getContent()).thenThrow(new IOException());
        new JcrXmlRetriever(new URI(testId), mockClient).get();
    }

    @Test(expected = RuntimeException.class)
    public void testYetOtherFailedRetrieval() throws Exception {
        final String testId = "testFailedRetrieval";
        reset(mockClient);
        when(mockClient.execute(any(HttpUriRequest.class))).thenThrow(new IOException());
        when(mockStatusLine.getStatusCode()).thenReturn(SC_OK);
        when(mockEntity.getContent()).thenThrow(new IOException());
        new JcrXmlRetriever(new URI(testId), mockClient).get();
    }

    @Test(expected = RuntimeException.class)
    public void testAuthForbiddenRetrieval() throws URISyntaxException {
        final String testId = "testAuthForbiddenRetrieval";
        when(mockStatusLine.getStatusCode()).thenReturn(SC_FORBIDDEN);
        new JcrXmlRetriever(new URI(testId), mockClient).get();
    }
}
