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

public class TimestampColumnSetter extends AbstractDynamicColumnSetter {
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public TimestampColumnSetter(
            final PageBuilder pageBuilder,
            final Column column,
            final DefaultValueSetter defaultValueSetter,
            final org.embulk.spi.time.TimestampParser timestampParser) {
        super(pageBuilder, column, defaultValueSetter);
        this.timestampParser = timestampParser;
    }

    @Override
    public void setNull() {
        this.pageBuilder.setNull(this.column);
    }

    @Override
    public void set(final boolean v) {
        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final long v) {
        this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofEpochSecond(v));
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final double v) {
        final long sec = (long) v;
        final int nsec = (int) ((v - (double) sec) * 1000000000);
        this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofEpochSecond(sec, nsec));
        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public void set(final String v) {
        try {
            this.pageBuilder.setTimestamp(this.column, this.timestampParser.parse(v));
        } catch (final org.embulk.spi.time.TimestampParseException ex) {
            this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
        }
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        this.pageBuilder.setTimestamp(this.column, v);
    }

    @Override
    public void set(final Instant v) {
        this.pageBuilder.setTimestamp(this.column, v);
    }

    @Override
    public void set(final Value v) {
        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    private final org.embulk.spi.time.TimestampParser timestampParser;
}
