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

import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.HttpStatus.SC_OK;
import static org.fcrepo.kernel.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
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

import org.slf4j.Logger;


/**
 * MessageListener implementation that retrieves objects from the repository and
 * invokes one or more indexers to index the content.
 *
 * documentation:
 * https://wiki.duraspace.org/display/FF/Design+-+Messaging+for+Workflow
 *
 * current message factory:
 * https://github.com/futures/fcrepo4/blob/a9be2e5d8bc3d7d4909a0fdf674d9faa0a37708e/fcrepo-jms/src/main/java/org/fcrepo/jms/headers/DefaultMessageFactory.java
 *
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19 2013
 **/
public class IndexerGroup implements MessageListener {

    private static final Logger LOGGER = getLogger(IndexerGroup.class);

    private Parser atomParser = new Abdera().getParser();

    private String repositoryURL;

    private Set<Indexer> indexers;

    private HttpClient httpclient;

    //timestamp
    private static final String TIMESTAMP_HEADER_NAME = REPOSITORY_NAMESPACE
            + "timestamp";

    //pid
    private static final String IDENTIFIER_HEADER_NAME = REPOSITORY_NAMESPACE
            + "identifier";

    //eventType - purgeObject
    private static final String EVENT_TYPE_HEADER_NAME = REPOSITORY_NAMESPACE
            + "eventType";

    /**
     * Default constructor.
     **/
    public IndexerGroup() {
        LOGGER.debug("Creating IndexerGroup: {}", this);
        final PoolingClientConnectionManager p = new PoolingClientConnectionManager();
        p.setDefaultMaxPerRoute(5);
        p.closeIdleConnections(3, SECONDS);
        httpclient = new DefaultHttpClient(p);
    }

    /**
     * Set repository URL.
     **/
    public void setRepositoryURL(final String repositoryURL) {
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
    public void setIndexers(final Set<Indexer> indexers) {
        this.indexers = indexers;
        LOGGER.debug("Using indexer complement: {} ", indexers);
    }

    /**
     * Get indexers set for this group.
     **/
    public Set<Indexer> getIndexers() {
        return indexers;
    }

    /**
     * Extract node path from Atom category list
     *
     * @return Node path or repositoryUrl if it's not found
     */
    private String getPath(final java.util.List<Category> categories) {
        for (final Category c : categories) {
            if (c.getLabel().equals("path")) {
                return repositoryURL + c.getTerm();
            }
        }
        return repositoryURL;
    }

    /**
     * Handle a JMS message representing an object update or deletion event.
     **/
    @Override
    public void onMessage(final Message message) {
        try {
            LOGGER.debug("Received message: {}", message.getJMSMessageID());
        } catch (final JMSException e) {
            LOGGER.error("Received unparseable message: {}", e);
            propagate(e);
        }
        try {
            // get pid, timestamp, and eventType from message
            try {
                long timestamp = message.getLongProperty(TIMESTAMP_HEADER_NAME);
            } catch (final NumberFormatException e) {
                LOGGER.error("Error getting timestamp property {}", e);
            }
            String pid = message.getStringProperty(IDENTIFIER_HEADER_NAME);
            String eventType = message.getStringProperty(EVENT_TYPE_HEADER_NAME);

            final Boolean removal = "purgeObject".equals(eventType);
            String content = "temp until getting real content from transformer";
            boolean hasContent = true; //temp
            LOGGER.debug("Operating with pid: {}", pid);
            LOGGER.debug("Operation with eventType: {}", eventType);
            LOGGER.debug("It is {} that this is a removal operation.",
                        removal);
            for (final Indexer indexer : indexers) {
                try {
                    if (removal) {
                        indexer.remove(pid);
                    } else {
                        if (hasContent) {
                            indexer.update(pid, content);
                        } else {
                            LOGGER.error(
                                "Received update on {} but was unable to retrieve representation!",
                                pid);
                        }
                    }
                } catch (final Exception e) {
                    LOGGER.error("Error indexing {}: {}!", pid, e);
                }
            }
        } catch (final JMSException e) {
            LOGGER.error("Error processing JMS event!", e);
        }
    }

}
