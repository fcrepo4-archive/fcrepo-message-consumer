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

import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.fcrepo.indexer.SynchIndexer;
import org.slf4j.Logger;

/**
 * Base indexer class to extend for persisting various forms of metadata to disk.
 * @author ajs6f
 * @author Esmé Cowles
 * @author lsitu
 * @since 2014-10-20
**/
public abstract class BasePersistenceIndexer<Content, File> extends SynchIndexer<Content, File> {

    private static final Logger LOGGER = getLogger(BasePersistenceIndexer.class);

    private final String pathName;
    private final String extension;

    /**
     * Constructor.
     * @param pathName Base directory for persisting files.
     * @param extension Filename extension to append to file names.
    **/
    public BasePersistenceIndexer( final String pathName, final String extension ) {
        this.pathName = pathName;
        this.extension = extension;
    }

    /**
     * Return the path where a given record should be persisted.
     * @param id The record's URI
    **/
    protected Path pathFor(final URI id) throws IOException {

        // strip the http protocol and replace column(:) in front of the port number
        String fullPath = id.toString().substring(id.toString().indexOf("//") + 2);
        fullPath = StringUtils.substringBefore(fullPath, "/").replace(":", "/") +
                "/" + StringUtils.substringAfter(fullPath, "/");
        // URL encode the id
        final String idPath = URLEncoder.encode(substringAfterLast(fullPath, "/"), "UTF-8");

        // URL encode and build the file path
        final String[] pathTokens = StringUtils.substringBeforeLast(fullPath, "/").split("/");
        final StringBuilder pathBuilder = new StringBuilder();
        for (final String token : pathTokens) {
            if (StringUtils.isNotBlank(token)) {
                pathBuilder.append(URLEncoder.encode(token, "UTF-8") + "/");
            }
        }

        fullPath = pathBuilder.substring(0, pathBuilder.length() - 1).toString();

        final Path dir = Paths.get(pathName, fullPath);
        if (Files.notExists(dir, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(Paths.get(pathName, fullPath));
        }
        return Paths.get(dir.toString(), idPath + extension);
    }
}
