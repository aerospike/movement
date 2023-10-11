/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.aerospike.movement.util.core.iterator;


import com.aerospike.movement.util.core.exception.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ArrayIterator<T> implements Iterator<T>, Serializable {

    private final T[] array;
    private int current = 0;

    public ArrayIterator(final T[] array) {
        this.array = Objects.requireNonNull(array);
    }

    @Override
    public boolean hasNext() {
        return this.current < this.array.length;
    }

    @Override
    public T next() {
        if (this.hasNext()) {
            this.current++;
            return this.array[this.current - 1];
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public String toString() {
        return array.length == 1 && null == array[0] ? "[null]" : Arrays.asList(array).toString();
    }
}
