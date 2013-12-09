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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    private final List<String> customIndexerFieldsConfig;

    private final Logger logger = LoggerFactory.getLogger(SolrIndexer.class);

    /**
     * @throws IOException
     * @throws URISyntaxException
     * @Autowired solrServer instance is auto-@Autowired in indexer-core.xml
     */
    @Autowired
    public SolrIndexer(final SolrServer solrServer,
            final String customIndexerFieldsConfigFile) throws IOException,
            URISyntaxException {
        this.server = solrServer;
        this.customIndexerFieldsConfig =
                customFieldsConfigReader(customIndexerFieldsConfigFile);
    }

    @Override
    public void update(final String pid, final String doc) {
        try {
            final SolrInputDocument inputDoc;
            final Collection<SolrInputDocument> docs =
                    new ArrayList<SolrInputDocument>();
            final HashMap<String, String> tokens =
                docParser(pid, doc, "N3");
            final Iterator it = tokens.entrySet().iterator();
            inputDoc = new SolrInputDocument();
            final StringBuilder restContent = new StringBuilder("");
            while (it.hasNext()) {
                final Map.Entry pairs = (Map.Entry) it.next();
                final String fieldname = (String) pairs.getKey();
                final String fieldvalue = (String) pairs.getValue();
                // add user selected fields
                for (final String field : this.customIndexerFieldsConfig) {
                    logger.debug("fieldname = " + fieldname);
                    if (fieldname.equals(field)) {
                        inputDoc.addField(fieldname, fieldvalue);
                        logger.debug("pid:" + pid + "||" + fieldname);
                    } else {
                        restContent.append(fieldname + "," + fieldvalue + "||");
                    }
                }
            }
            if (!restContent.toString().equals("")) {
                inputDoc.addField("content", restContent);
            }
            inputDoc.addField("id", pid);
            docs.add(inputDoc);
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
    HashMap<String, String> docParser(final String inputPid,
    final String doc,
            final String format) {
        // parse content into a model
        final Model model = ModelFactory.createDefaultModel();
        // final StringReader sr = new StringReader(doc);
        final InputStream sr = new ByteArrayInputStream(doc.getBytes());
        model.read(sr, null, format);
        final StmtIterator iter = model.listStatements();
        final HashMap<String, String> fields = new HashMap<String, String>();
        while (iter.hasNext()) {
            final String str0 = iter.next().toString();
            // get rid of [... ]
            final String str = str0.substring(1, str0.length() - 1);
            final String delims = ",";
            final StringTokenizer st = new StringTokenizer(str, delims);
            if (st.countTokens() == 3) {
                final String pid = st.nextToken();
                // check if contains pid
                if (inputPid.equals(pid)) {
                    // insert key
                    final String fieldName = st.nextToken().trim();
                    // insert value
                    final String fieldValue = st.nextToken().trim();
                    fields.put(fieldName, fieldValue);
                }
            }
        }
        return fields;
    }

    List<String> customFieldsConfigReader(final String filepath)
        throws IOException, URISyntaxException {
        final List<String> result = new ArrayList<String>();
        final String FIELD_NAME_PATTERN = "name=";
        // final URL fileUrl = SolrIndexer.class.getResource(filepath);
        final BufferedReader br =
                new BufferedReader(new FileReader(filepath));
        try {
            String line = br.readLine();
            while (line != null) {
                logger.warn("line:", line);
                final int fieldnameFound = line.indexOf(FIELD_NAME_PATTERN);
                if (fieldnameFound != -1) {
                    final int start =
                            fieldnameFound + FIELD_NAME_PATTERN.length() + 1;
                    final int end = line.indexOf('"', start);
                    result.add(line.substring(start, end));
                }
                line = br.readLine();
            }
        } finally {
            br.close();
        }
        return result;
    }
}
