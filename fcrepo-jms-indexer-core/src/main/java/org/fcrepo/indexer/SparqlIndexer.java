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

import static com.hp.hpl.jena.sparql.util.Context.emptyContext;
import static com.hp.hpl.jena.update.UpdateExecutionFactory.createRemoteForm;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemote;
import com.hp.hpl.jena.sparql.modify.request.QuadDataAcc;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataInsert;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import org.slf4j.Logger;


/**
 * Indexes triples from Fedora into a triplestore using SPARQL Update.
 *
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
**/
public class SparqlIndexer implements Indexer {

    private String queryBase;
    private String updateBase;
    private boolean formUpdates = false;

    private static final Logger LOGGER = getLogger(SparqlIndexer.class);

    /**
     * Set whether to use SPARQL Update or form updates.
    **/
    public void setFormUpdates( final boolean b ) {
        this.formUpdates = b;
    }

    /**
     * Set base URL for SPARQL Query requests.
    **/
    public void setQueryBase( final String url ) {
        this.queryBase = url;
    }

    /**
     * Set base URL for SPARQL Update requests.
    **/
    public void setUpdateBase( final String url ) {
        this.updateBase = url;
    }

    /**
     * Remove any current triples about the Fedora object and replace them with
     * the provided content.
     * @content RDF in N3 format.
    **/
    @Override
    public ListenableFuture<Void> update( final String pid, final String content ) {
        LOGGER.debug("Received update for: {}", pid);
        // first remove old data
        remove(pid);

        // parse content into a model
        final Model model = ModelFactory.createDefaultModel();
        model.read( new StringReader(content), null, "N3");

        // build a list of triples
        final StmtIterator triples = model.listStatements();
        final QuadDataAcc add = new QuadDataAcc();
        while ( triples.hasNext() ) {
            add.addTriple( triples.nextStatement().asTriple() );
        }

        // send update to server
        LOGGER.debug("Sending update request for pid: {}", pid);
        return exec(new UpdateRequest(new UpdateDataInsert(add)));
    }

    /**
     * Perform a DESCRIBE query for triples about the Fedora object and remove
     * all triples with subjects starting with the same subject.
    **/
    @Override
    public ListenableFuture<Void> remove( final String subject ) {

        LOGGER.debug("Received remove for: {}", subject);
        // find triples/quads to delete
        final String describeQuery = "DESCRIBE <" + subject + ">";
        final QueryEngineHTTP qexec = new QueryEngineHTTP( queryBase, describeQuery );
        final Iterator<Triple> results = qexec.execDescribeTriples();

        // build list of triples to delete
        final HashSet<String> uris = new HashSet<String>();
        while ( results.hasNext() ) {
            final Triple triple = results.next();

            // add subject uri, if it is part of this object
            if ( triple.getSubject().isURI() ) {
                final String uri = ((Node_URI)triple.getSubject()).getURI();
                if ( matches(subject, uri) ) {
                    uris.add(uri);
                }
            }

            // add object uri, if it is part of this object
            if ( triple.getObject().isURI() ) {
                final String uri = ((Node_URI)triple.getObject()).getURI();
                if ( matches(subject, uri) ) {
                    uris.add(uri);
                }
            }
        }
        qexec.close();

        // build update commands
        final UpdateRequest del = new UpdateRequest();
        for ( final String uri : uris ) {
            final String cmd = "delete where { <" + uri + "> ?p ?o }";
            LOGGER.debug(cmd);
            del.add( cmd );
        }

        // send updates
        return exec(del);
    }

    /**
     * Determine whether uri2 is a sub-URI of uri1, defined as uri1 starting
     * with uri2, plus an option suffix starting with a hash (#) or slash (/)
     * suffix.
    **/
    private boolean matches( final String uri1, final String uri2 ) {
        return uri1.equals(uri2) || uri1.startsWith(uri2 + "/")
            || uri1.startsWith(uri2 + "#");
    }

    private ListenableFuture<Void> exec(final UpdateRequest update) {
        final ListenableFutureTask<Void> task =
            ListenableFutureTask.create(new Runnable() {

                @Override
                public void run() {
                    if (formUpdates) {
                        // form updates
                        final UpdateProcessor proc =
                            createRemoteForm(update, updateBase);
                        proc.execute();
                    } else {
                        // normal SPARQL updates
                        final UpdateProcessRemote proc =
                            new UpdateProcessRemote(update, updateBase,
                                    emptyContext);
                        proc.execute();
                    }
                }
            }, null);
        task.run();
        return task;
    }

    /**
     * Count the number of triples in the triplestore for a Fedora object.
    **/
    public int countTriples(final String pid) {
        // perform describe query
        final String describeQuery = "DESCRIBE <" + pid + ">";
        final QueryEngineHTTP qexec = new QueryEngineHTTP( queryBase, describeQuery );
        final Iterator<Triple> results = qexec.execDescribeTriples();

        // count triples
        int triples = 0;
        while ( results.hasNext() ) {
            results.next();
            triples++;
        }
        qexec.close();

        return triples;
    }
}
