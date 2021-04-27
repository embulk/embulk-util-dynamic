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

public class DoubleColumnSetter extends AbstractDynamicColumnSetter {
    public DoubleColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue) {
        super(pageBuilder, column, defaultValue);
    }

    @Override
    public void setNull() {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v) {
        pageBuilder.setDouble(column, v ? 1.0 : 0.0);
    }

    @Override
    public void set(long v) {
        pageBuilder.setDouble(column, (double) v);
    }

    @Override
    public void set(double v) {
        pageBuilder.setDouble(column, v);
    }

    @Override
    public void set(String v) {
        double dv;
        try {
            dv = Double.parseDouble(v);
        } catch (NumberFormatException e) {
            defaultValue.setDouble(pageBuilder, column);
            return;
        }
        pageBuilder.setDouble(column, dv);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        double sec = (double) v.getEpochSecond();
        double frac = v.getNano() / 1000000000.0;
        pageBuilder.setDouble(column, sec + frac);
    }

    @Override
    public void set(Instant v) {
        double sec = (double) v.getEpochSecond();
        double frac = v.getNano() / 1000000000.0;
        pageBuilder.setDouble(column, sec + frac);
    }

    @Override
    public void set(Value v) {
        defaultValue.setDouble(pageBuilder, column);
    }
}
