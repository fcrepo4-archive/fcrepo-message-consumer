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

import java.io.IOException;
import java.util.Iterator;

import javax.inject.Inject;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class SparqlIndexerTest {
    @Inject
    private SparqlIndexer sparqlIndexer;
    String fooN3 =
        "@prefix fcrepo: <http://fcrepo.org/repository#> .\n" +
        "@prefix fedora: <http://fcrepo.org/repository/rest-api#> .\n" +
        "<http://localhost:8080/rest/objects/foo>\n" +
        " fcrepo:hasChild\n" +
        "   <http://localhost:8080/rest/objects/foo/barDS> ;\n" +
        " fcrepo:hasParent\n" +
        "   <http://localhost:8080/rest/objects> ;\n" +
        " fcrepo:uuid\n" +
        "   \"feb99ff2-455e-4e16-93a0-c0ae8d21b9ae\" .\n" +
        "<http://localhost:8080/rest/objects/foo/barDS>\n" +
        " fcrepo:hasContent\n" +
        "   <http://localhost:8080/rest/objects/foo/barDS/fcr:content> ;\n" +
        " fcrepo:hasParent\n" +
        "   <http://localhost:8080/rest/objects/foo> ;\n" +
        " fcrepo:uuid\n" +
        "   \"d26efce7-1b30-42eb-9236-0e62171e1d6e\" .\n" +
        "<http://localhost:8080/rest/objects/foo/barDS/fcr:content>\n" +
        " fcrepo:isContentOf\n" +
        "   <http://localhost:8080/rest/objects/foo/barDS> ;\n" +
        " fedora:digest\n" +
        "   <urn:sha1:b3eab5058657e177f05a39f94944c39086951eab> .\n";

    @Test
    public void indexerTest() throws IOException {
        // add object
        sparqlIndexer.update("foo",fooN3);

        // triples should be present in the triplestore
        assertTrue("Triples should be present",
                sparqlIndexer.countTriples("foo") > 0 );

        // remove object
        sparqlIndexer.remove("foo");

        // triples should not be present in the triplestore
        assertTrue("Triples should not be present",
                sparqlIndexer.countTriples("foo") == 0 );
    }
}
