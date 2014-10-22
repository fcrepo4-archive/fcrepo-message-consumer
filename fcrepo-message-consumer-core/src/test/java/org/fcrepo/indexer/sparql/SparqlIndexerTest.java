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
package org.fcrepo.indexer.sparql;

import static com.hp.hpl.jena.graph.NodeFactory.createLiteral;
import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static org.fcrepo.indexer.Indexer.IndexerType.RDF;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.update.UpdateRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;


/**
 * @author Andrew Woods
 * @since Feb 04 2014
 */
public class SparqlIndexerTest {

    private static final Logger LOGGER = getLogger(SparqlIndexerTest.class);

    private SparqlIndexer testIndexer = new MockSparqlIndexer();

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetIndexerType() {
        assertEquals("Got wrong indexer type!", RDF, testIndexer.getIndexerType());
        LOGGER.debug("Received correct indexer type.");
    }

    @Test
    public void testRemoveSynch() {
        testIndexer.removeSynch("info://obj-0");

        final String cmd0 = "DELETE WHERE { <" + createURI("info://obj-0") + "> ?p ?o }";
        final String cmd1 = "DELETE WHERE { <" + createURI("info://obj-0/fcr:content") + "> ?p ?o }";
        final String cmd2 = "DELETE WHERE { <" + createURI("info://obj-0/child") + "> ?p ?o }";
        Mockito.verify(updateRequest).add(cmd0);
        Mockito.verify(updateRequest).add(cmd1);
        Mockito.verify(updateRequest).add(cmd2);
    }

    @Test
    public void testUpdateSynch() {
        // TODO: This is a mere placeholder test to be further implemented later.
        final Model model = ModelFactory.createDefaultModel();
        testIndexer.updateSynch("", model);
    }

    @Mock
    private QueryEngineHTTP queryEngineHTTP;

    @Mock
    private UpdateRequest updateRequest;

    /**
     * Test extension of SparqlIndexer to eliminate HTTP interactions.
     */
    private class MockSparqlIndexer extends SparqlIndexer {

        protected QueryEngineHTTP buildQueryEngineHTTP(final String describeQuery) {
            final Triple t0 = new Triple(createURI("info://sub"), createLiteral("p"), createURI("info://obj-0"));
            final Triple t2 = new Triple(createURI("info://sub"),
                                         createLiteral("p"),
                                         createURI("info://obj-0/fcr:content"));
            final Triple t1 = new Triple(createURI("info://sub"), createLiteral("p"), createURI("info://obj-1"));
            final Triple t3 = new Triple(createURI("info://obj-0/child"),
                                         createLiteral("p"),
                                         createURI("info://obj-1"));

            final Set<Triple> triples = new HashSet<>();
            triples.add(t0);
            triples.add(t1);
            triples.add(t2);
            triples.add(t3);

            Mockito.when(queryEngineHTTP.execDescribeTriples()).thenReturn(triples.iterator());
            return queryEngineHTTP;
        }

        protected UpdateRequest buildUpdateRequest() {
            return updateRequest;
        }
    }

}
