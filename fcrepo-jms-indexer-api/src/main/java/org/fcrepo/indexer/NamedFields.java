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
package org.fcrepo.indexer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * A group of named fields.
 *
 * @author ajs6f
 * @date Dec 13, 2013
 */
public class NamedFields extends HashMap<String, Collection<String>> {

    /**
     * Default constructor
     *
     * @param values
     */
    public NamedFields(final Map<String, Collection<String>> values) {
        super(values);
    }

    /**
     * Constructor for empty object.
     */
    public NamedFields() {
        super();
    }

    private static final long serialVersionUID = 1L;

}
