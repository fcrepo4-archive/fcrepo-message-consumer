/**
 * Copyright 2014 DuraSpace, Inc.
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

import com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.fcrepo.kernel.utils.EventType;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createProperty;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static com.hp.hpl.jena.vocabulary.RDF.type;
import static java.lang.Integer.MAX_VALUE;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.fcrepo.kernel.RdfLexicon.CONTAINS;
import static org.fcrepo.kernel.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.fcrepo.kernel.RdfLexicon.RESTAPI_NAMESPACE;
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

    @VisibleForTesting
    protected final Set<Indexer<Object>> indexers;

    private Set<String> reindexed;

    /**
     * Identifier message header
     */
    static final String IDENTIFIER_HEADER_NAME = REPOSITORY_NAMESPACE
            + "identifier";

    /**
     * Properties message header
     */
    static final String PROPERTIES_HEADER_NAME = REPOSITORY_NAMESPACE + "properties";

    /**
     * BaseURL message header
     */
    static final String BASE_URL_HEADER_NAME = REPOSITORY_NAMESPACE + "baseURL";

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

    /**
     * Type of event to indicate reindexing.
     */
    private static final String REINDEX_EVENT_TYPE = REPOSITORY_NAMESPACE
            + "NODE_REINDEXED";

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

    private static final String REST_PREFIX = "/rest/";
    private static final String FCREPO_PREFIX = "/fcrepo/";

    /**
     * Indicates that a resource is a datastream.
    **/
    static final Resource DATASTREAM_TYPE = createResource(RESTAPI_NAMESPACE + "datastream");

    private static final Reader EMPTY_CONTENT = null;

    private final String fedoraUsername;
    private final String fedoraPassword;
    private final Map<String,DefaultHttpClient> clients;
    private final DefaultHttpClient defaultClient;

    /**
     * Default constructor.
     **/
    public IndexerGroup(final Set<Indexer<Object>> indexers,
                        final String fedoraUsername,
                        final String fedoraPassword) {
        this.fedoraUsername = fedoraUsername;
        this.fedoraPassword = fedoraPassword;

        LOGGER.debug("Creating IndexerGroup: {}", this);
        this.indexers = indexers;
        this.clients = new HashMap<>();
        this.defaultClient = null;
    }

    /**
     * Constructor with provided default HttpClient instance added for testing.
    **/
    public IndexerGroup(final Set<Indexer<Object>> indexers, final DefaultHttpClient httpClient) {
        LOGGER.debug("Creating IndexerGroup: {}", this);
        this.indexers = indexers;
        this.clients = new HashMap<>();
        this.fedoraUsername = null;
        this.fedoraPassword = null;
        this.defaultClient = httpClient;
    }

    @VisibleForTesting
    protected DefaultHttpClient httpClient(final String repositoryURL) {
        // try to find existing client
        if ( clients.size() > 0 ) {
            for ( final Iterator<String> it = clients.keySet().iterator(); it.hasNext(); ) {
                final String base = it.next();
                if ( repositoryURL.startsWith(base) ) {
                    return clients.get(base);
                }
            }
        }

        if ( defaultClient != null ) {
            return defaultClient;
        }

        // if no existing client matched, create a new one
        final String baseURL;
        if ( repositoryURL.indexOf(REST_PREFIX) > 0 ) {
            baseURL = repositoryURL.substring(0, repositoryURL.indexOf(REST_PREFIX) + REST_PREFIX.length());
        } else if ( repositoryURL.indexOf("/",FCREPO_PREFIX.length()) > 0 ) {
            baseURL = repositoryURL.substring(0, repositoryURL.indexOf("/",FCREPO_PREFIX.length()) + 1);
        } else {
            baseURL = repositoryURL;
        }

        final PoolingClientConnectionManager connMann = new PoolingClientConnectionManager();
        connMann.setMaxTotal(MAX_VALUE);
        connMann.setDefaultMaxPerRoute(MAX_VALUE);

        final DefaultHttpClient httpClient = new DefaultHttpClient(connMann);
        httpClient.setRedirectStrategy(new DefaultRedirectStrategy());
        httpClient.setHttpRequestRetryHandler(new StandardHttpRequestRetryHandler(0, false));

        // If the Fedora instance requires authentication, set it up here
        if (!isBlank(fedoraUsername) && !isBlank(fedoraPassword)) {
            LOGGER.debug("Adding BASIC credentials to client for repo requests.");

            final URI fedoraUri = URI.create(baseURL);
            final CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(new AuthScope(fedoraUri.getHost(), fedoraUri.getPort()),
                                         new UsernamePasswordCredentials(fedoraUsername, fedoraPassword));

            httpClient.setCredentialsProvider(credsProvider);
        }
        clients.put(baseURL, httpClient);
        return httpClient;
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
            // get id and eventType from message
            final String eventType =
                message.getStringProperty(EVENT_TYPE_HEADER_NAME);
            final String id = message.getStringProperty(IDENTIFIER_HEADER_NAME);
            String baseURL = message.getStringProperty(BASE_URL_HEADER_NAME);

            LOGGER.debug("Discovered id: {} in message.", id);
            LOGGER.debug("Discovered event type: {} in message.", eventType);
            LOGGER.debug("Discovered baseURL: {} in message.", baseURL);
            LOGGER.debug("Discovered properties: {} in message.", message.getStringProperty(PROPERTIES_HEADER_NAME));

            // Trim trailing '/'
            while (!Strings.isNullOrEmpty(baseURL) && baseURL.endsWith("/")) {
                baseURL = baseURL.substring(0, baseURL.length() - 1);
            }

            index( baseURL + id, eventType );
        } catch (final JMSException e) {
            LOGGER.error("Error processing JMS event!", e);
        }
    }

    /**
     * Index a resource.
    **/
    private void index( final String uri, final String eventType ) {
        final Boolean removal = REMOVAL_EVENT_TYPE.equals(eventType);
        final HttpClient httpClient = httpClient(uri);
        LOGGER.debug("It is {} that this is a removal operation.", removal);
        final Supplier<Model> rdfr =
            memoize(new RdfRetriever(uri, httpClient));
        final Supplier<NamedFields> nfr =
            memoize(new NamedFieldsRetriever(uri, httpClient, rdfr));
        final Supplier<InputStream> jcrfr =
             memoize(new JcrXmlRetriever(uri, httpClient));
        Boolean indexable = false;

        if (!removal) {
            final Model rdf = rdfr.get();
            if (rdf.contains(createResource(uri), type, INDEXABLE_MIXIN)) {
                LOGGER.debug("Resource: {} retrieved with indexable type.",
                        uri);
                indexable = true;
            } else {
                LOGGER.debug(
                        "Resource: {} retrieved without indexable type.",
                        uri);
            }

            // if this is a datastream, also index the parent object
            if (rdf.contains(createResource(uri), type, DATASTREAM_TYPE) && uri.indexOf("/fedora:system/") == -1 ) {
                final String parent = uri.substring(0, uri.lastIndexOf("/"));
                LOGGER.info("Datastream found, also indexing parent {}", parent);
                index( parent, "NODE_UPDATED");
            }
        }

        for (final Indexer<Object> indexer : indexers) {
            LOGGER.debug("Operating for indexer: {}", indexer);
            Boolean hasContent = false;
            Object content = EMPTY_CONTENT;
            if (!removal && indexable) {
                switch (indexer.getIndexerType()) {
                    case NAMEDFIELDS:
                        LOGGER.debug(
                                "Retrieving named fields for: {}, (may be cached) to index to {}...",
                                uri, indexer);
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
                                uri, indexer);
                        content = rdfr.get();
                        hasContent = true;
                        break;
                    case JCRXML_PERSISTENCE:
                        LOGGER.debug(
                                "Retrieving jcr/xml for: {} and persist it to {}...",
                                uri, indexer);
                        content = jcrfr.get();
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
                            uri, indexer);
                    indexer.remove(uri);
                } else {
                    if (hasContent) {
                        LOGGER.debug(
                                "Executing update of: {} to indexer: {}...",
                                uri, indexer);
                        indexer.update(uri, content);
                    } else if (indexable) {
                        LOGGER.error(
                                "Received update for: {} but was unable to retrieve "
                                        + "content for update to indexer: {}!",
                                uri, indexer);
                    }
                }
            } catch (final Exception e) {
                LOGGER.error("Error {} indexing {}: {}!", indexer.getClass().getName(), uri, e);
            }
        }
    }

    /**
     * Reindex a resource (and optionally all of its children).
     * @param uri The resource URI to reindex.
     * @param recursive If true, also recursively reindex all children.
    **/
    public void reindex( final String uri, final boolean recursive ) {
        reindexed = new HashSet<>();
        reindexURI( uri, recursive );
    }

    private void reindexURI( final String uri, final boolean recursive ) {
        LOGGER.debug("Reindexing {}, recursive: {}", uri, recursive);
        if ( !reindexed.contains(uri) ) {
            // index() will check for indexable mixin
            index( uri, REINDEX_EVENT_TYPE );
        }

        // prevent infinite recursion
        reindexed.add( uri );

        // check for children (rdf should be cached...)
        if ( recursive ) {
            final Supplier<Model> rdfr
                = memoize(new RdfRetriever(uri, httpClient(uri)));
            final Model model = rdfr.get();
            final NodeIterator children = model.listObjectsOfProperty( CONTAINS );
            while ( children.hasNext() ) {
                final String child = children.nextNode().asResource().getURI();
                if ( !reindexed.contains(child) ) {
                    reindexURI( child, true );
                }
            }
        }
    }
}
