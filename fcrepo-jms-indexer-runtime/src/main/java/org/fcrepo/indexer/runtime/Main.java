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
/**
 *
 */

package org.fcrepo.indexer.runtime;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.exit;
import static java.lang.System.getProperty;
import static java.util.ServiceLoader.load;
import static org.apache.felix.main.AutoProcessor.AUTO_DEPLOY_ACTION_PROPERY;
import static org.apache.felix.main.AutoProcessor.AUTO_DEPLOY_DIR_PROPERY;
import static org.apache.felix.main.AutoProcessor.process;
import static org.osgi.framework.Bundle.STOPPING;
import static org.osgi.framework.Constants.FRAMEWORK_STORAGE;
import static org.osgi.framework.FrameworkEvent.WAIT_TIMEDOUT;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.Map;

import org.osgi.framework.BundleException;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.slf4j.Logger;

/**
 * @author ajs6f
 * @date Jan 8, 2014
 */
public class Main {

    private static final int SHUTDOWN_TIMEOUT = 20000;

    public static final String INDEXER_HOME_PROP_NAME =
        "org.fcrepo.indexer.home";

    public static final String AUTODEPLOY_DIR_PROP_NAME =
        "felix.auto.deploy.dir";

    // take the first available
    private final FrameworkFactory frameworkFactory = load(
            FrameworkFactory.class).iterator().next();

    private Framework framework;

    private static final Logger LOGGER = getLogger(Main.class);

    /**
     * Default constructor.
     */
    public Main() {
        framework = frameworkFactory.newFramework(getConfig());
    }

    /**
     * @return Parsed configuration from System properties
     */
    public static Map<String, String> getConfig() {
        final Map<String, String> config = new HashMap<>();
        final String indexerHome =
            getProperty(INDEXER_HOME_PROP_NAME, "indexer");
        config.put(FRAMEWORK_STORAGE, indexerHome);
        LOGGER.info("Using indexer home: {}", indexerHome);
        final String autoDeployDir =
            getProperty(AUTODEPLOY_DIR_PROP_NAME, "bundle");
        config.put(AUTO_DEPLOY_DIR_PROPERY, autoDeployDir);
        LOGGER.info("Using auto-deploy directory: {}", autoDeployDir);
        config.put(AUTO_DEPLOY_ACTION_PROPERY, "install,update,start,uninstall");
        return config;
    }

    /**
     * @throws BundleException
     */
    public void start() throws BundleException {
        framework.start();
        LOGGER.info("Started internal OSGi framework...");
        getRuntime().addShutdownHook(new Thread("OSGi Framework Shutdown Hook") {

            @Override
            public void run() {
                try {
                    if (framework != null) {
                        if (framework.getState() != STOPPING) {

                            framework.stop();
                            framework.waitForStop(0);
                        }
                    }
                } catch (final Exception e) {
                    LOGGER.error("Error stopping internal OSGi framework: ", e);
                }
            }
        });
    }

    /**
     * @throws BundleException
     */
    public void init() throws BundleException {
        framework.init();
        LOGGER.info("Initialized internal OSGi framework...");
    }

    /**
     * @throws Throwable
     */
    public void stop() throws Throwable {
        framework.stop();
        LOGGER.info("Stopping internal OSGi framework...");
        final FrameworkEvent result = framework.waitForStop(SHUTDOWN_TIMEOUT);
        final Throwable t = result.getThrowable();
        if (t == null) {
            if (result.getType() == WAIT_TIMEDOUT) {
                throw new Timeout("Failed to shut down in " + SHUTDOWN_TIMEOUT
                        + " ms!");
            }
        } else {
            throw t;
        }

    }

    /**
     * @return the {@link Framework} in use
     */
    public Framework framework() {
        return framework;
    }

    /**
     * @param args
     */
    public static void main(final String[] args) {

        final Main m = new Main();

        try {
            m.init();
            process(getConfig(), m.framework().getBundleContext());
            m.start();
            System.in.read();
            m.stop();
            exit(0);
        } catch (final Throwable t) {
            LOGGER.error("", t);
            exit(1);
        }
    }

    private class Timeout extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public Timeout(final String msg) {
            super(msg);
        }
    }

}
