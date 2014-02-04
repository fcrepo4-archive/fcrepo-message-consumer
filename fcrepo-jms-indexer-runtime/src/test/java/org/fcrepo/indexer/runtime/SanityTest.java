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

package org.fcrepo.indexer.runtime;

import static java.lang.System.setProperty;
import static java.util.Collections.list;
import static org.apache.felix.main.AutoProcessor.process;
import static org.fcrepo.indexer.runtime.Main.AUTODEPLOY_DIR_PROP_NAME;
import static org.fcrepo.indexer.runtime.Main.INDEXER_HOME_PROP_NAME;
import static org.fcrepo.indexer.runtime.Main.getConfig;
import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Dictionary;

import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;

public class SanityTest {

    private static final Logger LOGGER = getLogger(SanityTest.class);

    @Before
    public void setDirectories() {
        setProperty(AUTODEPLOY_DIR_PROP_NAME, "target/bundle");
        setProperty(INDEXER_HOME_PROP_NAME, "target/indexer");
    }

    @Test
    public void runOnce() throws BundleException {
        final Main m = new Main();
        m.init();
        process(getConfig(), m.framework().getBundleContext());
        m.start();
        for (final Bundle b : m.framework().getBundleContext().getBundles()) {
            LOGGER.debug("Found bundle: {} at: {} in state: {}", b
                    .getSymbolicName(), b.getLocation(), b.getState());
            final Dictionary<String, String> d = b.getHeaders();

            for (final String key : list(d.keys())) {
                LOGGER.debug("with header: {} = {}", key, d.get(key));
            }
        }
        try {
            m.stop();
        } catch (final Throwable e) {
            fail("Failed to launch and shutdown a runtime.");
        }

    }
}
