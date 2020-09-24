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

import com.google.common.math.DoubleMath;
import java.math.RoundingMode;
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
            // TODO configurable rounding mode
            lv = DoubleMath.roundToLong(v, RoundingMode.HALF_UP);
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
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        this.pageBuilder.setLong(this.column, v.getEpochSecond());
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