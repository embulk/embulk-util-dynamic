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
    public TimestampColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue,
            org.embulk.spi.time.TimestampParser timestampParser) {
        super(pageBuilder, column, defaultValue);
        this.timestampParser = timestampParser;
    }

    @Override
    public void setNull() {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v) {
        defaultValue.setTimestamp(pageBuilder, column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(long v) {
        pageBuilder.setTimestamp(column, org.embulk.spi.time.Timestamp.ofEpochSecond(v));
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(double v) {
        long sec = (long) v;
        int nsec = (int) ((v - (double) sec) * 1000000000);
        pageBuilder.setTimestamp(column, org.embulk.spi.time.Timestamp.ofEpochSecond(sec, nsec));
        defaultValue.setTimestamp(pageBuilder, column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public void set(String v) {
        try {
            pageBuilder.setTimestamp(column, timestampParser.parse(v));
        } catch (org.embulk.spi.time.TimestampParseException e) {
            defaultValue.setTimestamp(pageBuilder, column);
        }
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(org.embulk.spi.time.Timestamp v) {
        pageBuilder.setTimestamp(column, v);
    }

    @Override
    public void set(Instant v) {
        pageBuilder.setTimestamp(column, v);
    }

    @Override
    public void set(Value v) {
        defaultValue.setTimestamp(pageBuilder, column);
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    private final org.embulk.spi.time.TimestampParser timestampParser;
}
