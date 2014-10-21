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
package org.fcrepo.indexer.persistence;

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.fcrepo.indexer.Indexer.IndexerType.RDF;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import com.hp.hpl.jena.rdf.model.Model;

import org.fcrepo.indexer.Indexer.IndexerType;
import org.slf4j.Logger;

/**
 * RDF serializer
 * @author ajs6f
 * @author Esmé Cowles
 * @author lsitu
 * @date 2014-10-20
**/
public class RdfPersistenceIndexer extends BasePersistenceIndexer<Model, File> {

    private static final Logger LOGGER = getLogger(RdfPersistenceIndexer.class);

    private String rdfLang = null;

    /**
     * Constructor
     * @param pathName of directory in which jcr/xml exports will be stored
     * @param rdfLang RDF language name ("Turtle", "RDF/XML", "N-Triples", etc.)
     * @param extension Filename extension (".ttl", ".rdf.xml", ".nt", etc.)
     */
    public RdfPersistenceIndexer(final String pathName, final String rdfLang, final String extension) {
        super(pathName, extension);
        this.rdfLang = rdfLang;
    }

    @Override
    public IndexerType getIndexerType() {
        return RDF;
    }

    /**
     * Update a record with the content provided.
     * @param id The record's URI
     * @param model Updated RDF model
     * @return The file where the RDF was written.
    **/
    @Override
    public Callable<File> updateSynch(final String id, final Model model) {
        if (id.endsWith("/")) {
            throw new IllegalArgumentException("Identifiers for use with this indexer may not end in '/'!");
        }

        return new Callable<File>() {
            @Override
            public File call() throws IOException {
                final Path p = pathFor(id);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                model.write(out, rdfLang);
                LOGGER.debug("Updating {} to file: {}", id, p.toAbsolutePath().toString());
                Files.copy(new ByteArrayInputStream(out.toByteArray()), p, new CopyOption[]{});
                return p.toFile();
            }
        };
    }

    /**
     * Remove the record.
     * @param id the record's URI
    **/
    @Override
    public Callable<File> removeSynch(final String id) {
        // empty update
        LOGGER.debug("Received remove for identifier: {}", id);
        return updateSynch(id, createDefaultModel());
    }
}
