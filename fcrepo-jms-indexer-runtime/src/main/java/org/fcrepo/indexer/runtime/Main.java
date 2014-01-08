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

import static java.lang.System.exit;
import static java.lang.System.out;
import static org.fcrepo.indexer.runtime.OSGiUtils.getFrameworkFactory;
import static org.osgi.framework.FrameworkEvent.WAIT_TIMEDOUT;

import org.osgi.framework.BundleException;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;

/**
 * @author ajs6f
 * @date Jan 8, 2014
 */
public class Main {

    private static final int SHUTDOWN_TIMEOUT = 20000;

    private final FrameworkFactory frameworkFactory = getFrameworkFactory();

    private Framework framework;

    /**
     * Default constructor.
     *
     * @param args
     */
    public Main(final String[] args) {
        framework = frameworkFactory.newFramework(null);
    }

    /**
     * @throws BundleException
     */
    public void start() throws BundleException {
        framework.start();
        out.println("Started internal OSGi framework...");

    }

    /**
     * @throws BundleException
     * @throws InterruptedException
     */
    public Integer stop() throws BundleException, InterruptedException {
        framework.stop();
        out.println("Stopping internal OSGi framework...");
        final FrameworkEvent result = framework.waitForStop(SHUTDOWN_TIMEOUT);
        final Throwable t = result.getThrowable();
        if (t == null) {
            if (result.getType() == WAIT_TIMEDOUT) {
                out.println("Failed to shut down in " + SHUTDOWN_TIMEOUT
                        + " ms!");
                return 1;
            } else {
                return 0;
            }
        } else {
            out.println(t.getLocalizedMessage());
            return 1;
        }

    }

    /**
     * @param args
     * @throws BundleException
     */
    public static void main(final String[] args) {
        final Main m = new Main(args);

        try {
            m.start();
            exit(m.stop());
        } catch (final BundleException | InterruptedException e) {
            e.printStackTrace();
        }

    }

}
