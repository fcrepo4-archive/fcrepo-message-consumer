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

package org.fcrepo.indexer.integration.sparql;

import java.io.StringReader;

import javax.inject.Inject;

import org.fcrepo.indexer.sparql.SparqlIndexer;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import org.junit.Test;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static java.lang.Thread.sleep;
import static org.fcrepo.indexer.integration.sparql.SparqlIndexerITHelper.countQueryTriples;
import static org.fcrepo.indexer.integration.sparql.SparqlIndexerITHelper.countDescribeTriples;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class SparqlIndexerIT {

    private static final String uri = "http://example.com/sparqlIndexerTestURI";


    private static final Logger LOGGER = getLogger(SparqlIndexerIT.class);

    @Inject
    private SparqlIndexer sparqlIndexer;

    private static final String fooRDF =
        "@prefix fcrepo: <http://fedora.info/definitions/v4/repository#> .\n" +
        "@prefix fedora: <http://fedora.info/definitions/v4/repository/rest-api#> .\n" +
        "<" + uri + ">\n" +
        " fcrepo:hasChild <" + uri + "/barDS> ;\n" +
        " fcrepo:hasParent <" + uri + "> ;\n" +
        " fcrepo:uuid \"feb99ff2-455e-4e16-93a0-c0ae8d21b9ae\" .";

    @Test
    public void indexerTest() throws Exception {
        // add triples
        sparqlIndexer.update(uri, createDefaultModel().read(
                new StringReader(fooRDF), "", "N3"));

        waitForTriples(3);

        // triples should be present in the triplestore
        assertEquals("Triples should be present!", 3, countDescribeTriples(uri));

        // SPARQL search should work
        final String sparqlQuery =
                "PREFIX  fcrepo: <http://fedora.info/definitions/v4/repository#> \n" +
                        "SELECT  ?p \n" +
                        "WHERE \n" +
                        "{ ?p fcrepo:hasParent ?c }";
        assertTrue("Triple should return from search!", countQueryTriples(sparqlQuery) > 0);

        // remove object
        sparqlIndexer.remove(uri);

        waitForTriples(0);

        // triples should not be present in the triplestore
        assertTrue("Triples should not be present!", countDescribeTriples(uri) == 0);
    }

    private static void waitForTriples(final int expectTriples) throws InterruptedException {
        long elapsed = 0;
        final long restingWait = 500;
        final long maxWait = 15000; // 15 seconds

        int count = countDescribeTriples(uri);
        while ((count < expectTriples) && (elapsed < maxWait)) {
            LOGGER.debug("Discovered {} triples, waiting for {}...", count, expectTriples);
            sleep(restingWait);
            count = countDescribeTriples(uri);

            elapsed += restingWait;
        }
    }

}
