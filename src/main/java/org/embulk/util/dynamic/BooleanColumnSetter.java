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

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

public class BooleanColumnSetter extends AbstractDynamicColumnSetter {
    public BooleanColumnSetter(
            final PageBuilder pageBuilder,
            final Column column,
            final DefaultValueSetter defaultValueSetter) {
        super(pageBuilder, column, defaultValueSetter);
    }

    @Override
    public void setNull() {
        this.pageBuilder.setNull(this.column);
    }

    @Override
    public void set(final boolean v) {
        this.pageBuilder.setBoolean(this.column, v);
    }

    @Override
    public void set(final long v) {
        this.pageBuilder.setBoolean(this.column, v > 0);
    }

    @Override
    public void set(final double v) {
        this.pageBuilder.setBoolean(this.column, v > 0.0);
    }

    @Override
    public void set(final String v) {
        if (TRUE_STRINGS.contains(v)) {
            this.pageBuilder.setBoolean(this.column, true);
        } else {
            this.defaultValueSetter.setBoolean(this.pageBuilder, column);
        }
    }

    @Override
    public void set(final Instant v) {
        this.defaultValueSetter.setBoolean(this.pageBuilder, this.column);
    }

    @Override
    public void set(final Value v) {
        this.defaultValueSetter.setBoolean(this.pageBuilder, this.column);
    }

    private static final ImmutableSet<String> TRUE_STRINGS = ImmutableSet.of(
            "true", "True", "TRUE",
            "yes", "Yes", "YES",
            "t", "T", "y", "Y",
            "on", "On",
            "ON", "1");
}
