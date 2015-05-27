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
package org.fcrepo.indexer.integration.sparql;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

import java.util.Iterator;

import static java.lang.System.getProperty;

/**
 * @author Andrew Woods
 *         Date: 2/11/14
 */
public class SparqlIndexerITHelper {

    private static String queryBase =
        "http://localhost:" + getProperty("fuseki.dynamic.test.port", "3030") + "/test/query";

    /**
     * No public constructor for utility class
     */
    private SparqlIndexerITHelper() {
    }

    /**
     * Count the number of triples in the triplestore for a Fedora object.
     *
     * @param uri URI to object to perform this action on
     * @return the number of triples
     */
    public static int countDescribeTriples(final String uri) {
        // perform describe query
        final String describeQuery = "DESCRIBE <" + uri + ">";
        final QueryEngineHTTP qexec = new QueryEngineHTTP(queryBase, describeQuery);
        final Iterator<Triple> results = qexec.execDescribeTriples();

        final int count = count(results);

        qexec.close();
        return count;
    }

    /**
     * Perform a SPARQL search and return the number of triples returned.
     *
     * @param searchQuery The SPARQL query to perform 
     * @return the number of triples found by the query
     */
    public static int countQueryTriples(final String searchQuery) {
        // perform query
        final QueryEngineHTTP qexec = new QueryEngineHTTP(queryBase, searchQuery);
        final ResultSet results = qexec.execSelect();

        final int count = count(results);

        qexec.close();
        return count;
    }

    private static int count(final Iterator triples) {
        // count triples
        int count = 0;
        while (triples.hasNext()) {
            triples.next();
            count++;
        }
        return count;
    }

}
