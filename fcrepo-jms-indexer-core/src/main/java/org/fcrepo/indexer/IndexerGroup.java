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
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.nio.charset.Charset;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Category;
import org.apache.abdera.model.Document;
import org.apache.abdera.model.Entry;
import org.apache.abdera.parser.Parser;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

/**
 * MessageListener implementation that retrieves objects from the repository and
 * invokes one or more indexers to index the content.
 * 
 * @author Esmé Cowles
 *         Date: Aug 19, 2013
 **/
public class IndexerGroup implements MessageListener {
    private Parser atomParser = new Abdera().getParser();
    private String repositoryURL;
    private Set<Indexer> indexers;

    private HttpClient httpclient;

    /**
     * Default constructor.
     **/
    public IndexerGroup() {
        PoolingClientConnectionManager p = new PoolingClientConnectionManager();
        p.setDefaultMaxPerRoute(5);
        p.closeIdleConnections(3, TimeUnit.SECONDS);
        httpclient = new DefaultHttpClient(p);
    }

    /**
     * Set repository URL.
     **/
    public void setRepositoryURL(String repositoryURL) {
        this.repositoryURL = repositoryURL;
    }

    /**
     * Get repository URL.
     **/
    public String getRepositoryURL() {
        return repositoryURL;
    }

    /**
     * Set indexers for this group.
     **/
    public void setIndexers(Set indexers) {
        for (Iterator it = indexers.iterator(); it.hasNext();) {
            Object o = it.next();
            if (o instanceof Indexer) {
                if (this.indexers == null) {
                    this.indexers = new HashSet<Indexer>();
                }
                this.indexers.add((Indexer) o);
            }
        }
    }

    /**
     * Get indexers set for this group.
     **/
    public Set<Indexer> getIndexers() {
        return indexers;
    }

    /**
     * Extract node path from Atom category list
     * @return Node path or repositoryUrl if it's not found
     */
    private String getPath(java.util.List<Category> categories) {
        for (Category c : categories) {
            if (c.getLabel().equals("path")) {
                return repositoryURL + c.getTerm();
            }
        }
        return repositoryURL;
    }

    /**
     * Handle a JMS message representing an object update or deletion event.
     **/
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                // get pid from message
                final String xml = ((TextMessage) message).getText();
                Document<Entry> doc = atomParser.parse(new StringReader(xml));
                Entry entry = doc.getRoot();
                // if the object is updated, fetch current content
                String content = null;
                if (!"purgeObject".equals(entry.getTitle())) {
                    HttpGet get = new HttpGet(
                            getPath(entry.getCategories("xsd:string")));
                    HttpResponse response = httpclient.execute(get);
                    content = IOUtils.toString(
                            response.getEntity().getContent(),
                            Charset.forName("UTF-8"));
                }
                //pid represents the full path. Alternative would be to send path separately in all calls
                //String pid = getPath(entry.getCategories("xsd:string")).replace("//objects", "/objects");
                String pid = getPath(entry.getCategories("xsd:string"));


                // call each registered indexer
                for (Indexer indexer : indexers) {
                    try {
                        if ("purgeObject".equals(entry.getTitle())) {
                            indexer.remove(pid);
                        } else {
                            indexer.update(pid, content);
                        }
                    } catch (Exception innerex) {
                        innerex.printStackTrace();
                    }
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
