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

import com.google.common.base.Supplier;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.fcrepo.kernel.utils.EventType;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.Reader;
import java.util.Set;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createProperty;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.vocabulary.RDF.type;
import static java.lang.Integer.MAX_VALUE;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static org.fcrepo.kernel.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * MessageListener implementation that retrieves objects from the repository and
 * invokes one or more indexers to index the content. documentation:
 * https://wiki.duraspace.org/display/FF/Design+-+Messaging+for+Workflow
 *
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19 2013
 **/
public class IndexerGroup implements MessageListener {

    private static final Logger LOGGER = getLogger(IndexerGroup.class);

    private String repositoryURL;

    private Set<Indexer<Object>> indexers;

    private HttpClient httpClient;

    /**
     * Identifier message header
     */
    static final String IDENTIFIER_HEADER_NAME = REPOSITORY_NAMESPACE
            + "identifier";

    /**
     * Event type message header
     */
    static final String EVENT_TYPE_HEADER_NAME = REPOSITORY_NAMESPACE
            + "eventType";

    /**
     * Type of event that qualifies as a removal.
     */
    static final String REMOVAL_EVENT_TYPE = REPOSITORY_NAMESPACE
            + EventType.valueOf(NODE_REMOVED).toString();

    public static final String INDEXER_NAMESPACE =
        "http://fedora.info/definitions/v4/indexing#";

    /**
     * Indicates the transformation to use with this resource to derive indexing
     * information.
     */
    public static final Property INDEXING_TRANSFORM_PREDICATE =
        createProperty(INDEXER_NAMESPACE + "hasIndexingTransformation");

    /**
     * Indicates that a resource is indexable.
     */
    public static final Resource INDEXABLE_MIXIN =
        createResource(INDEXER_NAMESPACE + "indexable");

    private static final Reader EMPTY_CONTENT = null;

    /**
     * Default constructor.
     **/
    public IndexerGroup() {
        LOGGER.debug("Creating IndexerGroup: {}", this);
        final PoolingClientConnectionManager connMann =
            new PoolingClientConnectionManager();
        connMann.setMaxTotal(MAX_VALUE);
        connMann.setDefaultMaxPerRoute(MAX_VALUE);
        this.httpClient = new DefaultHttpClient(connMann);
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
     *
     * @param indexers
     */
    public void setIndexers(final Set<Indexer<Object>> indexers) {
        this.indexers = indexers;
        LOGGER.debug("Using indexer complement: {} ", indexers);
    }

    /**
     * Get indexers set for this group.
     *
     * @return indexers
     */
    public Set<Indexer<Object>> getIndexers() {
        return indexers;
    }

    /**
     * Set HttpClient for this group.  In the constructor a default is set
     * but this allows it to be customized.
     *
     * @param client
     */
    public void setHttpClient(final HttpClient client) {
        this.httpClient = client;
    }

    /**
     * Gets the HttpClient used by this class.
     *
     * @return
     */
    public HttpClient getHttpClient() {
        return this.httpClient;
    }

    /**
     * Handle a JMS message representing an object update or deletion event.
     **/
    @Override
    public void onMessage(final Message message) {
        try {
            LOGGER.debug("Received message: {}", message.getJMSMessageID());
        } catch (final JMSException e) {
            LOGGER.error("Received unintelligible message: {}", e);
            propagate(e);
        }
        try {
            final String pid;
            // get pid and eventType from message
            final String eventType =
                message.getStringProperty(EVENT_TYPE_HEADER_NAME);
            if (eventType.contains("PROPERTY") && !eventType.contains("NODE_ADDED")) {
                // it seems the URL is for the property, not the node on which
                // the property is set...
                final String id = message.getStringProperty(IDENTIFIER_HEADER_NAME);
                pid = id.substring(0, id.lastIndexOf('/'));
            } else {
                pid = message.getStringProperty(IDENTIFIER_HEADER_NAME);
            }


            LOGGER.debug("Discovered pid: {} in message.", pid);
            LOGGER.debug("Discovered event type: {} in message.", eventType);

            final Boolean removal = REMOVAL_EVENT_TYPE.equals(eventType);
            LOGGER.debug("It is {} that this is a removal operation.", removal);
            final String uri = getRepositoryURL() + pid;
            final Supplier<Model> rdfr =
                memoize(new RdfRetriever(uri, httpClient));
            final Supplier<NamedFields> nfr =
                memoize(new NamedFieldsRetriever(uri, httpClient, rdfr));
            Boolean indexable = false;

            if (!removal) {
                final Model rdf = rdfr.get();
                if (rdf.contains(createResource(uri), type, INDEXABLE_MIXIN)) {
                    LOGGER.debug("Resource: {} retrieved with indexable type.",
                            pid);
                    indexable = true;
                } else {
                    LOGGER.debug(
                            "Resource: {} retrieved without indexable type.",
                            pid);
                }
            }

            for (final Indexer<Object> indexer : getIndexers()) {
                LOGGER.debug("Operating for indexer: {}", indexer);
                Boolean hasContent = false;
                Object content = EMPTY_CONTENT;
                if (!removal && indexable) {
                    switch (indexer.getIndexerType()) {
                        case NAMEDFIELDS:
                            LOGGER.debug(
                                    "Retrieving named fields for: {}, (may be cached) to index to {}...",
                                    pid, indexer);
                            try  {
                                content = nfr.get();
                                hasContent = true;
                            } catch (final AbsentTransformPropertyException e) {
                                LOGGER.error("Failed to retrieve indexable content:"
                                        + "could not find transform property!");
                                hasContent = false;
                            }
                            break;
                        case RDF:
                            LOGGER.debug(
                                    "Retrieving RDF for: {}, (may be cached) to index to {}...",
                                    pid, indexer);
                            content = rdfr.get();
                            hasContent = true;
                            break;
                        default:
                            hasContent = true;
                            break;
                    }
                }

                try {
                    if (removal) {
                        LOGGER.debug(
                                "Executing removal of: {} to indexer: {}...",
                                pid, indexer);
                        indexer.remove(uri);
                    } else {
                        if (hasContent) {
                            LOGGER.debug(
                                    "Executing update of: {} to indexer: {}...",
                                    pid, indexer);
                            indexer.update(uri, content);
                        } else if (indexable) {
                            LOGGER.error(
                                    "Received update for: {} but was unable to retrieve "
                                            + "content for update to indexer: {}!",
                                    pid, indexer);
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
