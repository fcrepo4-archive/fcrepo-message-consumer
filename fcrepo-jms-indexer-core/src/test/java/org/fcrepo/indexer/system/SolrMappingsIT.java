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

package org.fcrepo.indexer.system;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.nio.charset.Charset.defaultCharset;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.fcrepo.indexer.solr.SolrIndexer.CONFIGURATION_FOLDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.jena.riot.WebContent;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.fcrepo.indexer.solr.SolrIndexer;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.io.Files;

/**
 * @author ajs6f
 * @date Dec 7, 2013
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/test-container.xml"})
public class SolrMappingsIT extends IndexingIT {

    @Inject
    private SolrServer indexServer;

    private static final Logger LOGGER = getLogger(SolrMappingsIT.class);

    private static final long TIMEOUT = 15000;

    private static final long TIME_TO_WAIT_STEP = 1000;

    @Inject
    private SolrIndexer testSolrIndexer;

    @Test
    public void testOneResource() throws ClientProtocolException, IOException,
        SolrServerException, InterruptedException {

        LOGGER.debug("Creating indexable resource...");
        final String uri = serverAddress + "testCollection/testOneResourcePid";
        final HttpPost createRequest = new HttpPost(uri);
        final String objectRdf =
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."
                    + "@prefix dc:<http://purl.org/dc/elements/1.1/> ."
                    + "@prefix indexing:<http://fedora.info/definitions/v4/indexing#>."
                    + "<" + uri + ">  dc:title        \"500 Easy Microwave Meals\" ; "
                    + "dc:creator      \"Yubulac Xorhorisa\" ; "
                    + "dc:subject      <http://id.loc.gov/authorities/subjects/sh2012004374> ;"
                    + "rdf:type  <http://fedora.info/definitions/v4/indexing#indexable> ;"
                    + "indexing:hasIndexingTransformation \"default\".";

        createRequest.setEntity(new StringEntity(objectRdf));
        createRequest.addHeader("Content-Type", WebContent.contentTypeN3Alt1);
        LOGGER.debug("Creating object with RDF:\n{}", objectRdf);
        HttpResponse response = client.execute(createRequest);
        assertEquals("Failed to create test resource!", SC_CREATED, response
                .getStatusLine().getStatusCode());

        LOGGER.debug("Waiting for our resource to be indexed...");
        final String q = "title:\"500 Easy Microwave Meals\"";
        LOGGER.debug("Checking for our record with query: {}", q);
        final SolrQuery query = new SolrQuery(q);
        List<SolrDocument> results = indexServer.query(query).getResults();
        Boolean success = results.size() == 1;
        Long start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to appear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for presence of appropriate index record...");
            results = indexServer.query(query).getResults();
            success = results.size() == 1;
        }
        assertTrue(
                "Got other-than-one documents from index under our title!",
                success);
        LOGGER.debug("Discovered one record for {} in index.", uri);
        assertEquals("Found wrong title in record!",
                "500 Easy Microwave Meals", results.get(0).getFirstValue(
                        "title"));

        final HttpDelete deleteRequest = new HttpDelete(uri);
        response = client.execute(deleteRequest);
        assertEquals("Failed to delete test resource!", SC_NO_CONTENT, response
                .getStatusLine().getStatusCode());
        results = indexServer.query(query).getResults();
        success = results.size() == 0;
        start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to disappear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for absence of appropriate index record...");
            results = indexServer.query(query).getResults();
            success = results.size() == 0;
        }
        assertTrue(
                "Found too many documents from index under our title!",
                success);
    }

    /**
     * Index the Dublin Core properties of a single resource, then remove the
     * index record.
     *
     * @throws IOException
     * @throws ClientProtocolException
     * @throws SolrServerException
     * @throws InterruptedException
     */
    @Ignore("Waiting on https://www.pivotaltracker.com/story/show/61942638")
    @Test
    public void testOneResourceCustomMapping() throws ClientProtocolException, IOException,
        SolrServerException, InterruptedException {

        final String mappingUrl =
            serverAddress + CONFIGURATION_FOLDER + "dc/fedora:object";

        LOGGER.debug("Creating index mapping at URL: {}...", mappingUrl);

        final HttpPost indexCreateRequest = new HttpPost(mappingUrl);
        final HttpEntity indexMapping =
            new StringEntity(Files.toString(new File(
                    "target/test-classes/ldpath/dublin_core.ldpath"),
                    defaultCharset()));
        indexCreateRequest.setEntity(indexMapping);

        HttpResponse response = client.execute(indexCreateRequest);
        assertEquals("Failed to create index mapping!", SC_CREATED, response
                .getStatusLine().getStatusCode());

        LOGGER.debug("Creating indexable resource...");
        final String uri = serverAddress + "testCollection/testOneResourcePid";
        final HttpPost createRequest = new HttpPost(uri);
        final String objectRdf =
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."
                    + "@prefix dc:<http://purl.org/dc/elements/1.1/> ."
                    + "@prefix indexing:<http://fedora.info/definitions/v4/indexing#>."
                    + "<" + uri + ">  dc:title        \"500 Easy Microwave Meals\" ; "
                    + "dc:creator      \"Yubulac Xorhorisa\" ; "
                    + "dc:subject      <http://id.loc.gov/authorities/subjects/sh2012004374> ;"
                    + "rdf:type  <http://fedora.info/definitions/v4/indexing#indexable> ;"
                    + "indexing:hasIndexingTransformation \"dc\".";

        createRequest.setEntity(new StringEntity(objectRdf));
        createRequest.addHeader("Content-Type", WebContent.contentTypeN3Alt1);
        LOGGER.debug("Creating object with RDF:\n{}", objectRdf);
        response = client.execute(createRequest);
        assertEquals("Failed to create test resource!", SC_CREATED, response
                .getStatusLine().getStatusCode());

        LOGGER.debug("Waiting for our resource to be indexed...");
        final String q = "title:\"500 Easy Microwave Meals\"";
        LOGGER.debug("Checking for our record with query: {}", q);
        final SolrQuery query = new SolrQuery(q);
        List<SolrDocument> results = indexServer.query(query).getResults();
        Boolean success = results.size() == 1;
        Long start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to appear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for presence of appropriate index record...");
            results = indexServer.query(query).getResults();
            success = results.size() == 1;
        }
        assertTrue(
                "Got other-than-one documents from index under our title!",
                success);
        LOGGER.debug("Discovered one record for {} in index.", uri);
        assertEquals("Found wrong title in record!",
                "500 Easy Microwave Meals", results.get(0).getFirstValue(
                        "title"));

        final HttpDelete deleteRequest = new HttpDelete(uri);
        response = client.execute(deleteRequest);
        assertEquals("Failed to delete test resource!", SC_NO_CONTENT, response
                .getStatusLine().getStatusCode());
        results = indexServer.query(query).getResults();
        success = results.size() == 0;
        start = currentTimeMillis();
        while ((currentTimeMillis() - start < TIMEOUT) && !success) {
            LOGGER.debug("Waiting for index record to disappear...");
            sleep(TIME_TO_WAIT_STEP);
            LOGGER.debug("Checking for absence of appropriate index record...");
            results = indexServer.query(query).getResults();
            success = results.size() == 0;
        }
        assertTrue(
                "Found too many documents from index under our title!",
                success);
    }

}
