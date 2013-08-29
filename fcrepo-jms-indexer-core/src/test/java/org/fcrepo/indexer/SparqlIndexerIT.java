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

import javax.inject.Inject;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class SparqlIndexerIT {

    private static final int SERVER_PORT =
            Integer.parseInt(System.getProperty("test.port", "8080"));

    private static final String serverAddress = "http://localhost:" +
            SERVER_PORT + "/rest/objects";

    @Inject
    private SparqlIndexer sparqlIndexer;
    private static final String fooN3 =
        "@prefix fcrepo: <http://fcrepo.org/repository#> .\n" +
        "@prefix fedora: <http://fcrepo.org/repository/rest-api#> .\n" +
                "<" + serverAddress + "/foo>\n" +
        " fcrepo:hasChild\n" +
        "   <" + serverAddress + "/foo/barDS> ;\n" +
        " fcrepo:hasParent\n" +
        "   <" + serverAddress + "> ;\n" +
        " fcrepo:uuid\n" +
        "   \"feb99ff2-455e-4e16-93a0-c0ae8d21b9ae\" .\n" +
        "<" + serverAddress + "/foo/barDS>\n" +
        " fcrepo:hasContent\n" +
        "   <" + serverAddress + "/foo/barDS/fcr:content> ;\n" +
        " fcrepo:hasParent\n" +
        "   <" + serverAddress + "/foo> ;\n" +
        " fcrepo:uuid\n" +
        "   \"d26efce7-1b30-42eb-9236-0e62171e1d6e\" .\n" +
        "<" + serverAddress + "/foo/barDS/fcr:content>\n" +
        " fcrepo:isContentOf\n" +
        "   <" + serverAddress + "/foo/barDS> ;\n" +
        " fedora:digest\n" +
        "   <urn:sha1:b3eab5058657e177f05a39f94944c39086951eab> .\n";

    @Test
    public void indexerTest() throws Exception {
        // add object
        sparqlIndexer.update(serverAddress + "/foo",fooN3);

        waitForTriples(3);

        // triples should be present in the triplestore
        assertEquals("Triples should be present",
                     3, sparqlIndexer.countTriples(serverAddress + "/foo"));

        // remove object
        sparqlIndexer.remove("foo");

        waitForTriples(0);

        // triples should not be present in the triplestore
        assertTrue("Triples should not be present",
                sparqlIndexer.countTriples("foo") == 0 );
    }

    private void waitForTriples(int expectTriples) throws InterruptedException {
        long elapsed = 0;
        long restingWait = 500;
        long maxWait = 15000; // 15 seconds

        int count = sparqlIndexer.countTriples("foo");
        while ((count != expectTriples) && (elapsed < maxWait)) {
            Thread.sleep(restingWait);
            count = sparqlIndexer.countTriples("foo");

            elapsed += restingWait;
        }
    }

}
