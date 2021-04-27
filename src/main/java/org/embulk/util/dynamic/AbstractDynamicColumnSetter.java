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
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

public abstract class AbstractDynamicColumnSetter implements DynamicColumnSetter {
    protected final PageBuilder pageBuilder;
    protected final Column column;
    protected final DefaultValueSetter defaultValue;

    protected AbstractDynamicColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue) {
        this.pageBuilder = pageBuilder;
        this.column = column;
        this.defaultValue = defaultValue;
    }

    public abstract void setNull();

    public abstract void set(boolean value);

    public abstract void set(long value);

    public abstract void set(double value);

    public abstract void set(String value);

    @Deprecated
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public abstract void set(org.embulk.spi.time.Timestamp value);

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(Instant value) {
        this.set(org.embulk.spi.time.Timestamp.ofInstant(value));
    }

    public abstract void set(Value value);
}
