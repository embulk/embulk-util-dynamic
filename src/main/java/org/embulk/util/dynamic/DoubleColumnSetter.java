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
    public DoubleColumnSetter(
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
        this.pageBuilder.setDouble(this.column, v ? 1.0 : 0.0);
    }

    @Override
    public void set(final long v) {
        this.pageBuilder.setDouble(this.column, (double) v);
    }

    @Override
    public void set(final double v) {
        this.pageBuilder.setDouble(this.column, v);
    }

    @Override
    public void set(final String v) {
        final double dv;
        try {
            dv = Double.parseDouble(v);
        } catch (final NumberFormatException ex) {
            this.defaultValueSetter.setDouble(this.pageBuilder, this.column);
            return;
        }
        this.pageBuilder.setDouble(this.column, dv);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        final double sec = (double) v.getEpochSecond();
        final double frac = v.getNano() / 1000000000.0;
        this.pageBuilder.setDouble(this.column, sec + frac);
    }

    @Override
    public void set(final Instant v) {
        final double sec = (double) v.getEpochSecond();
        final double frac = v.getNano() / 1000000000.0;
        this.pageBuilder.setDouble(this.column, sec + frac);
    }

    @Override
    public void set(final Value v) {
        this.defaultValueSetter.setDouble(this.pageBuilder, this.column);
    }
}
