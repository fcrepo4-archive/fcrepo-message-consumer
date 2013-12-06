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

/**
 * Indicates that a resource was not designated for named-field indexing.
 * Typically this would be because there has been assigned no appropriate
 * transformation.
 *
 * @author ajs6f
 * @date Dec 4, 2013
 */
public class AbsentTransformPropertyException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param msg
     */
    public AbsentTransformPropertyException(final String msg) {
        super(msg);
    }

}
