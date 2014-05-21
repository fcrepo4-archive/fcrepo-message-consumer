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
package org.fcrepo.indexer.webapp;

import static org.slf4j.LoggerFactory.getLogger;
import org.slf4j.Logger;

import java.io.PrintWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;


import org.fcrepo.indexer.IndexerGroup;

/**
 * Servlet to trigger reindexing.
 *
 * @author escowles
**/
public class FedoraIndexer extends HttpServlet {

    private static final Logger LOGGER = getLogger(IndexerGroup.class);
    private IndexerGroup indexer;

    /**
     * Servlet initialization.
    **/
    public void init( final ServletConfig sc ) throws ServletException {
        super.init(sc);
        final WebApplicationContext ctx
            = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
        indexer = (IndexerGroup)ctx.getBean( sc.getInitParameter("beanName") );
    }

    /**
     * Trigger reindexing.
    **/
    public void doPost(final HttpServletRequest request, final HttpServletResponse response)
        throws ServletException {
        final String recurParam = request.getParameter("recursive");
        final boolean recursive = (recurParam == null || recurParam.equals("true"));
        final String path = request.getPathInfo();
        indexer.reindex( indexer.getRepositoryURL() + path, recursive );

        try {
            response.setContentType("text/plain");
            final PrintWriter out = response.getWriter();
            out.println("Reindexing started");
        } catch ( Exception ex ) {
            LOGGER.warn("Error sending output");
        }
    }
}
