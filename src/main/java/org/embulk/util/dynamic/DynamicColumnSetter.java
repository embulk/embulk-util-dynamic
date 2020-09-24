/*
 * Copyright 2015 The Embulk project
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

package org.embulk.util.dynamic;

import java.time.Instant;
import org.msgpack.value.Value;

public interface DynamicColumnSetter {
    void setNull();

    void set(boolean value);

    void set(long value);

    void set(double value);

    void set(String value);

    @Deprecated
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    void set(org.embulk.spi.time.Timestamp value);

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    default void set(Instant value) {
        this.set(org.embulk.spi.time.Timestamp.ofInstant(value));
    }

    void set(Value value);
}