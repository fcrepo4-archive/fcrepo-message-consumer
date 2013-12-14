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

package org.fcrepo.indexer.elastic.integration;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Collection;

import javax.inject.Inject;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.fcrepo.indexer.NamedFields;
import org.fcrepo.indexer.elastic.ElasticIndexer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/spring-test/elastic.xml"})
public class ElasticIndexerIT {

    @Inject
    ElasticIndexer testIndexer = new ElasticIndexer();

    @Inject
    Client client;

    @Before
    public void configureIndexer() {
        testIndexer.setIndexName("testindex");
        testIndexer.setSearchIndexType("testType");
    }

    @Test
    public void testAddition() throws IOException {
        doAddition("testAddition");
    }

    public void doAddition(final String id) throws IOException {
        final Collection<String> values = asList(id);
        final NamedFields testContent = new NamedFields(of("id", values));
        testIndexer.update(id, testContent);
        final GetResponse response =
            client.prepareGet(testIndexer.getIndexName(),
                    testIndexer.getSearchIndexType(), id).execute().actionGet();
        assertEquals("Didn't find our resource indexed!", id, response.getId());
    }

    @Test
    public void testRemoval() throws IOException {
        final String id = "testRemoval";
        doAddition(id);
        client.prepareDelete(testIndexer.getIndexName(),
                    testIndexer.getSearchIndexType(), id).execute().actionGet();
        final GetResponse response =
            client.prepareGet(testIndexer.getIndexName(),
                    testIndexer.getSearchIndexType(), id).execute().actionGet();
        assertFalse("Record existed when it should have been deleted!",
                response.isExists());
    }

}
