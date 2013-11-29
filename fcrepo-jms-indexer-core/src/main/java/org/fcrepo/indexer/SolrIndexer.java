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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * A Solr Indexer (stub) implementation that adds some basic information to
 * a Solr index server.
 *
 * @author yecao
 *
 */
public class SolrIndexer implements Indexer {

    private final SolrServer server;

    private final Logger logger = LoggerFactory.getLogger(SolrIndexer.class);

    /**
     * @Autowired solrServer instance is auto-@Autowired in indexer-core.xml
     */
    @Autowired
    public SolrIndexer(final SolrServer solrServer) {
        this.server = solrServer;
    }

    @Override
    public void update(final String pid, final String doc) {
        try {
            SolrInputDocument inputDoc;
            final Collection<SolrInputDocument> docs =
                    new ArrayList<SolrInputDocument>();
            // inputDoc.addField("id", pid);
            // inputDoc.addField("content", doc);
            final HashMap<String, ArrayList<String[]>> tokens = docParser(doc);
            final Iterator it = tokens.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry pairs = (Map.Entry) it.next();
                final String id = (String) pairs.getKey();
                // not index root node
                if (id.equals("http://localhost:9090/rest/")) {
                    break;
                }
                inputDoc = new SolrInputDocument();
                inputDoc.addField("id", id);
                final ArrayList<String[]> fields =
                        (ArrayList<String[]>) pairs.getValue();
                final Iterator<String[]> iterator_fields = fields.iterator();
                while (iterator_fields.hasNext()) {
                    final String[] field = iterator_fields.next();
                    final String fieldname = field[0];
                    final String fieldvalue = field[1];
                    // TODO add selected fields
                    if (fieldname
                            .trim()
                            .equals("http://fedora.info/definitions/v4/repository#mixinTypes")) {
                        inputDoc.addField(fieldname, fieldvalue);
                        logger.debug("pid:" + pid + "||" + fieldname);
                    }
                }
                docs.add(inputDoc);
            }
            final UpdateResponse resp = server.add(docs);
            if (resp.getStatus() == 0) {
                // logger.debug("update request was successful for pid: {}",
                // pid);
                server.commit();
            } else {
                logger.warn(
                        "update request has error, code: {} for pid: {}",
                        resp.getStatus(), pid);
            }
        } catch (final SolrServerException | IOException e) {
            logger.warn("Update Exception: {}", e);
        }

    }

    /**
     * (non-Javadoc)
     * @see org.fcrepo.indexer.Indexer#remove(java.lang.String)
     */
    @Override
    public void remove(final String pid) throws IOException {
        try {
            final UpdateResponse resp = server.deleteById(pid);
            if (resp.getStatus() == 0) {
                logger.debug("remove request was successful for pid: {}", pid);
                server.commit();
            } else {
                logger.warn("remove request has error, code: {} for pid: {}",
                        resp.getStatus(), pid);
            }
        } catch (final SolrServerException | IOException e) {
            logger.warn("Delete Exception: {}", e);
        }

    }

    /*
     * return json
     */
    HashMap<String, ArrayList<String[]>> docParser(final String doc) {
        // parse content into a model
        final Model model = ModelFactory.createDefaultModel();
        // final StringReader sr = new StringReader(doc);
        final InputStream sr = new ByteArrayInputStream(doc.getBytes());
        model.read(sr, "N3");
        final StmtIterator iter = model.listStatements();
        final HashMap<String, ArrayList<String[]>> docs =
                new HashMap<String, ArrayList<String[]>>();
        while (iter.hasNext()) {
            final String str0 = iter.next().toString();
            final String str = str0.substring(1, str0.length() - 1);
            final String delims = ",";
            final StringTokenizer st = new StringTokenizer(str, delims);
            if (st.countTokens() == 3) {
                // insert pid
                final String pid = st.nextToken();
                // insert key
                final String fieldName = st.nextToken();
                // insert value
                final String fieldValue = st.nextToken();
                final ArrayList<String[]> fields;
                if (docs.containsKey(pid)) {
                    fields = docs.get(pid);
                    fields.add(new String[] {fieldName, fieldValue});
                } else {
                    fields = new ArrayList<String[]>();
                }
                docs.put(pid, fields);
            }
        }
        return docs;
    }
}
