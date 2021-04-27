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
    protected AbstractDynamicColumnSetter(
            final PageBuilder pageBuilder,
            final Column column,
            final DefaultValueSetter defaultValueSetter) {
        this.pageBuilder = pageBuilder;
        this.column = column;
        this.defaultValueSetter = defaultValueSetter;
    }

    @Override
    public abstract void setNull();

    @Override
    public abstract void set(boolean value);

    @Override
    public abstract void set(long value);

    @Override
    public abstract void set(double value);

    @Override
    public abstract void set(String value);

    @Override
    public abstract void set(Instant value);

    @Override
    public abstract void set(Value value);

    protected final PageBuilder pageBuilder;
    protected final Column column;
    protected final DefaultValueSetter defaultValueSetter;
}
