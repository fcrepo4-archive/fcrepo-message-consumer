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

import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemote;
import com.hp.hpl.jena.sparql.modify.request.QuadDataAcc;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataInsert;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Indexes triples from Fedora into a triplestore using SPARQL Update.
 *
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
**/
public class SparqlIndexer implements Indexer {
    private String prefix;
    private String queryBase;
    private String updateBase;
    private boolean formUpdates = false;

    final private Logger logger = LoggerFactory.getLogger(SparqlIndexer.class);

    /**
     * Set URI prefix for building triplestore subjects.  The fedora PID will
     * be appended to this prefix.
    **/
    public void setPrefix( String s ) {
        this.prefix = s;
    }

    /**
     * Set whether to use SPARQL Update or form updates.
    **/
    public void setFormUpdates( boolean b ) {
        this.formUpdates = b;
    }

    /**
     * Set base URL for SPARQL Query requests.
    **/
    public void setQueryBase( String url ) {
        this.queryBase = url;
    }

    /**
     * Set base URL for SPARQL Update requests.
    **/
    public void setUpdateBase( String url ) {
        this.updateBase = url;
    }

    /**
     * Remove any current triples about the Fedora object and replace them with
     * the provided content.
     * @content RDF in N3 format.
    **/
    public void update( String pid, String content ) {
        // first remove old data
        remove(pid);

        // parse content into a model
        Model model = ModelFactory.createDefaultModel();
        model.read( new StringReader(content), null, "N3");

        // build a list of triples
        StmtIterator triples = model.listStatements();
        QuadDataAcc add = new QuadDataAcc();
        while ( triples.hasNext() ) {
            add.addTriple( triples.nextStatement().asTriple() );
        }

        // send update to server
        logger.debug("Sending update request for pid: {}", pid);
        exec( new UpdateRequest(new UpdateDataInsert(add)) );
    }

    /**
     * Perform a DESCRIBE query for triples about the Fedora object and remove
     * all triples with subjects starting with the same subject.
    **/
    public void remove( String subject ) {

        // find triples/quads to delete
        String describeQuery = "DESCRIBE <" + subject + ">";
        QueryEngineHTTP qexec = new QueryEngineHTTP(
            queryBase, describeQuery );
        Iterator<Triple> results = qexec.execDescribeTriples();

        // build list of triples to delete
        HashSet<String> uris = new HashSet<String>();
        while ( results.hasNext() ) {
            Triple triple = results.next();

            // add subject uri, if it is part of this object
            if ( triple.getSubject().isURI() ) {
                String uri = ((Node_URI)triple.getSubject()).getURI();
                if ( uri.equals(subject) || uri.startsWith(subject + "/") ||
                        uri.startsWith(subject + "#") ) {
                    uris.add(uri);
                }
            }

            // add object uri, if it is part of this object
            if ( triple.getObject().isURI() ) {
                String uri = ((Node_URI)triple.getObject()).getURI();
                if ( uri.equals(subject) || uri.startsWith(subject + "/") ||
                        uri.startsWith(subject + "#") ) {
                    uris.add(uri);
                }
            }
        }
        qexec.close();

        // build update commands
        UpdateRequest del = new UpdateRequest();
        for ( String uri : uris ) {
            String cmd = "delete where { <" + uri + "> ?p ?o }";
            logger.debug(cmd);
            del.add( cmd );
        }

        // send updates
        exec( del );
    }

    private void exec( UpdateRequest update ) {
        if ( formUpdates ) {
            // form updates
            UpdateProcessor proc = UpdateExecutionFactory.createRemoteForm(
                update, updateBase );
            proc.execute();
        } else {
            // normal SPARQL updates
            UpdateProcessRemote proc = new UpdateProcessRemote(
                update, updateBase, Context.emptyContext );
            proc.execute();
        }
    }

    /**
     * Count the number of triples in the triplestore for a Fedora object.
    **/
    public int countTriples(String pid) {
        // perform describe query
        String describeQuery = "DESCRIBE <" + pid + ">";
        QueryEngineHTTP qexec = new QueryEngineHTTP( queryBase, describeQuery );
        Iterator<Triple> results = qexec.execDescribeTriples();

        // count triples
        int triples = 0;
        while ( results.hasNext() ) {
            Triple t = results.next();
            triples++;
        }
        qexec.close();

        return triples;
    }
}
