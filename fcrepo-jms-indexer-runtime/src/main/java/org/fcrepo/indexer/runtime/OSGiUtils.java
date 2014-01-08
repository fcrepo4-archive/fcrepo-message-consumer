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

import static java.util.ServiceLoader.load;

import java.util.ServiceLoader;

import org.osgi.framework.launch.FrameworkFactory;

/**
 * @author ajs6f
 * @date Jan 8, 2014
 */
public abstract class OSGiUtils {

    private static ServiceLoader<FrameworkFactory> frameworkFactoryLoader =
        load(FrameworkFactory.class);

    /**
     * @return A {@link FrameworkFactory}
     */
    public static FrameworkFactory getFrameworkFactory() {
        // we take the first available FrameworkFactory
        return frameworkFactoryLoader.iterator().next();
    }
}
