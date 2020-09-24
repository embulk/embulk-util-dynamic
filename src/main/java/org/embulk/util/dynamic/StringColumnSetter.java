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

public class StringColumnSetter extends AbstractDynamicColumnSetter {
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    private final org.embulk.spi.time.TimestampFormatter timestampFormatter;

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public StringColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue,
            org.embulk.spi.time.TimestampFormatter timestampFormatter) {
        super(pageBuilder, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void setNull() {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v) {
        pageBuilder.setString(column, Boolean.toString(v));
    }

    @Override
    public void set(long v) {
        pageBuilder.setString(column, Long.toString(v));
    }

    @Override
    public void set(double v) {
        pageBuilder.setString(column, Double.toString(v));
    }

    @Override
    public void set(String v) {
        pageBuilder.setString(column, v);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        pageBuilder.setString(column, timestampFormatter.format(v));
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(Instant v) {
        pageBuilder.setString(column, timestampFormatter.format(org.embulk.spi.time.Timestamp.ofInstant(v)));
    }

    @Override
    public void set(Value v) {
        pageBuilder.setString(column, v.toJson());
    }
}
