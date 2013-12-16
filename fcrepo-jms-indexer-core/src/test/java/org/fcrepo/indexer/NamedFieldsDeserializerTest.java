
package org.fcrepo.indexer;

import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.junit.Test;
import org.slf4j.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class NamedFieldsDeserializerTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testWrite() throws IOException {
        new NamedFieldsDeserializer().write(null, null);
    }

    @Test
    public void testReadGoodJson() throws IOException{
        final String testUri = "testUri";
        final String fakeJson = "[{\"id\" : [\"" + testUri + "\"]}]";
        LOGGER.debug("Using fake JSON: {}", fakeJson);
        try (
            Reader r = new StringReader(fakeJson);
            JsonReader jr = new JsonReader(r)) {
            final NamedFields results = new NamedFieldsDeserializer().setGson(new Gson()).read(jr);
            LOGGER.debug("Received results: {}", results);
            assertEquals(testUri, results.get("id").iterator().next());
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testReadBadJson() throws IOException{
        final String testUri = "testUri";
        final String fakeJson = "[{\"id\" : \"" + testUri + "\"}]";
        LOGGER.debug("Using fake JSON: {}", fakeJson);
        try (
            Reader r = new StringReader(fakeJson);
            JsonReader jr = new JsonReader(r)) {
            final NamedFields results = new NamedFieldsDeserializer().setGson(new Gson()).read(jr);
            LOGGER.debug("Received results: {}", results);
            assertEquals(testUri, results.get("id").iterator().next());
        }
    }

    private static final Logger LOGGER =
            getLogger(NamedFieldsDeserializerTest.class);


}
