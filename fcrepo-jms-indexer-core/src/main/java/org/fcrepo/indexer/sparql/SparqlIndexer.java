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

package org.fcrepo.indexer.sparql;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.hp.hpl.jena.sparql.util.Context.emptyContext;
import static com.hp.hpl.jena.update.UpdateExecutionFactory.createRemoteForm;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.fcrepo.indexer.Indexer.IndexerType.RDF;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemote;
import com.hp.hpl.jena.sparql.modify.request.QuadDataAcc;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataInsert;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import org.apache.jena.atlas.io.IndentedWriter;
import org.fcrepo.indexer.AsynchIndexer;
import org.slf4j.Logger;


/**
 * Indexes triples from Fedora into a triplestore using SPARQL Update.
 *
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19, 2013
**/
public class SparqlIndexer extends AsynchIndexer<Model, Void> {

    private String queryBase;
    private String updateBase;
    private boolean formUpdates = false;

    private static final Logger LOGGER = getLogger(SparqlIndexer.class);

    /**
     * Number of threads to use for operating against the triplestore.
     */
    private static final Integer THREAD_POOL_SIZE = 5;

    private ListeningExecutorService executorService =
        listeningDecorator(newFixedThreadPool(THREAD_POOL_SIZE));

    /**
     * Remove any current triples about the Fedora object and replace them with
     * the provided content.
     * @content RDF in N3 format.
    **/
    @Override
    public Callable<Void> updateSynch(final String pid,
        final Model model) {
        LOGGER.debug("Received update for: {}", pid);
        removeSynch(pid);
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
    public Callable<Void> removeSynch(final String subject) {

        LOGGER.debug("Received remove for: {}", subject);
        // find triples/quads to delete
        final String describeQuery = "DESCRIBE <" + subject + ">";
        final QueryEngineHTTP qexec = buildQueryEngineHTTP(describeQuery);
        final Iterator<Triple> results = qexec.execDescribeTriples();

        // build list of triples to delete
        final Set<String> uris = new HashSet<>();
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
        final UpdateRequest del = buildUpdateRequest();
        for (final String uri : uris) {
            final String cmd = "DELETE WHERE { <" + uri + "> ?p ?o }";
            LOGGER.debug("Executing: {}", cmd);
            del.add(cmd);
        }

        // send updates
        return exec(del);
    }

    /**
     * Determine whether arg candidate is a sub-URI of arg resource, defined as candidate-URI starting
     * with resource-URI, plus an option suffix starting with a hash (#) or slash (/)
     * suffix.
    **/
    private boolean matches( final String resource, final String candidate) {
        // All triples that will match this logic are ones that:
        // - have a candidate subject or object that equals the target resource of removal, or
        // - have a candidate subject or object that is prefixed with the resource of removal
        //    (therefore catching all children).
        return resource.equals(candidate) || candidate.startsWith(resource + "/")
            || candidate.startsWith(resource + "#");
    }

    private Callable<Void> exec(final UpdateRequest update) {
        if (update.getOperations().isEmpty()) {
            LOGGER.debug("Received empty update/remove operation.");
            return new Callable<Void>() {

                @Override
                public Void call() {
                    return null;
                }
            };
        }

        final Callable<Void> callable = new Callable<Void>() {

            @Override
            public Void call() {

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
                    try {
                        proc.execute();
                    } catch (final Exception e) {
                        LOGGER.error(
                                "Error executing Sparql update/remove!", e);
                    }
                }
                return null;
            }
        };

        final ListenableFutureTask<Void> task =
            ListenableFutureTask.create(callable);
        task.addListener(new Runnable() {

            @Override
            public void run() {
                LOGGER.debug("Completed Sparql update/removal.");
                if (LOGGER.isTraceEnabled()) {
                    try (
                        final OutputStream buffer = new ByteArrayOutputStream()) {
                        final IndentedWriter out = new IndentedWriter(buffer);
                        update.output(out);
                        LOGGER.trace("Executed update/remove operation:\n{}",
                                buffer.toString());
                        out.close();
                    } catch (final IOException e) {
                        LOGGER.error(
                                "Couldn't retrieve execution of update/remove operation!",
                                e);
                    }
                }
            }
        }, executorService);
        executorService.submit(task);
        return callable;
    }

    @Override
    public IndexerType getIndexerType() {
        return RDF;
    }

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

    @Override
    public ListeningExecutorService executorService() {
        return executorService;
    }

    /**
     * Note: Protected for Unit Tests to overwrite.
     */
    protected QueryEngineHTTP buildQueryEngineHTTP(String describeQuery) {
        return new QueryEngineHTTP( queryBase, describeQuery );
    }

    /**
     * Note: Protected for Unit Tests to overwrite.
     */
    protected UpdateRequest buildUpdateRequest() {
        return new UpdateRequest();
    }

}
