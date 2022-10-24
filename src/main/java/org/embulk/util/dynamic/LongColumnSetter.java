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

public class LongColumnSetter extends AbstractDynamicColumnSetter {
    public LongColumnSetter(
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
        this.pageBuilder.setLong(this.column, v ? 1L : 0L);
    }

    @Override
    public void set(final long v) {
        this.pageBuilder.setLong(this.column, v);
    }

    @Override
    public void set(final double v) {
        final long lv;
        try {
            final double roundedDouble = Math.rint(v);
            final double diff = v - roundedDouble;
            if (Math.abs(diff) == 0.5) {
                lv = (long) (v + Math.copySign(0.5, v));
            } else {
                lv = (long) roundedDouble;
            }
        } catch (final ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            this.defaultValueSetter.setLong(this.pageBuilder, this.column);
            return;
        }
        this.pageBuilder.setLong(this.column, lv);
    }

    @Override
    public void set(final String v) {
        final long lv;
        try {
            lv = Long.parseLong(v);
        } catch (final NumberFormatException ex) {
            this.defaultValueSetter.setLong(this.pageBuilder, this.column);
            return;
        }
        this.pageBuilder.setLong(this.column, lv);
    }

    @Override
    public void set(final Instant v) {
        this.pageBuilder.setLong(this.column, v.getEpochSecond());
    }

    @Override
    public void set(final Value v) {
        this.defaultValueSetter.setLong(this.pageBuilder, this.column);
    }
}
