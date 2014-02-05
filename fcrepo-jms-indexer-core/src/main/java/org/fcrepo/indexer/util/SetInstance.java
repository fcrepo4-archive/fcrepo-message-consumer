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

package org.fcrepo.indexer.util;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.Set;

import javax.enterprise.inject.Instance;
import javax.enterprise.util.TypeLiteral;

/**
 * @author ajs6f
 * @date Feb 4, 2014
 */
public class SetInstance<T> implements Instance<T> {

    private Set<T> backingSet;

    @Override
    public Iterator<T> iterator() {
        return backingSet.iterator();
    }

    @Override
    public T get() {
        return backingSet.iterator().next();
    }

    @Override
    public Instance<T> select(final Annotation... qualifiers) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <U extends T> Instance<U> select(final Class<U> subtype,
            final Annotation... qualifiers) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <U extends T> Instance<U> select(final TypeLiteral<U> subtype,
            final Annotation... qualifiers) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isUnsatisfied() {
        return false;
    }

    @Override
    public boolean isAmbiguous() {
        return true;
    }


    /**
     * @param set the backing set to use
     * @return this object for continued use
     */
    public SetInstance<T> setBackingSet(final Set<T> set) {
        this.backingSet = set;
        return this;
    }

}
