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
    private static final ImmutableSet<String> TRUE_STRINGS =
            ImmutableSet.of(
                    "true", "True", "TRUE",
                    "yes", "Yes", "YES",
                    "t", "T", "y", "Y",
                    "on", "On",
                    "ON", "1");

    public BooleanColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue) {
        super(pageBuilder, column, defaultValue);
    }

    @Override
    public void setNull() {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v) {
        pageBuilder.setBoolean(column, v);
    }

    @Override
    public void set(long v) {
        pageBuilder.setBoolean(column, v > 0);
    }

    @Override
    public void set(double v) {
        pageBuilder.setBoolean(column, v > 0.0);
    }

    @Override
    public void set(String v) {
        if (TRUE_STRINGS.contains(v)) {
            pageBuilder.setBoolean(column, true);
        } else {
            defaultValue.setBoolean(pageBuilder, column);
        }
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(org.embulk.spi.time.Timestamp v) {
        defaultValue.setBoolean(pageBuilder, column);
    }

    @Override
    public void set(Instant v) {
        defaultValue.setBoolean(pageBuilder, column);
    }

    @Override
    public void set(Value v) {
        defaultValue.setBoolean(pageBuilder, column);
    }
}
