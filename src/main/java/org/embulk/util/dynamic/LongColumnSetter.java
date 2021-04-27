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
    public LongColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue) {
        super(pageBuilder, column, defaultValue);
    }

    @Override
    public void setNull() {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v) {
        pageBuilder.setLong(column, v ? 1L : 0L);
    }

    @Override
    public void set(long v) {
        pageBuilder.setLong(column, v);
    }

    @Override
    public void set(double v) {
        long lv;
        try {
            // TODO configurable rounding mode
            lv = DoubleMath.roundToLong(v, RoundingMode.HALF_UP);
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            defaultValue.setLong(pageBuilder, column);
            return;
        }
        pageBuilder.setLong(column, lv);
    }

    @Override
    public void set(String v) {
        long lv;
        try {
            lv = Long.parseLong(v);
        } catch (NumberFormatException e) {
            defaultValue.setLong(pageBuilder, column);
            return;
        }
        pageBuilder.setLong(column, lv);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {
        pageBuilder.setLong(column, v.getEpochSecond());
    }

    @Override
    public void set(Instant v) {
        pageBuilder.setLong(column, v.getEpochSecond());
    }

    @Override
    public void set(Value v) {
        defaultValue.setLong(pageBuilder, column);
    }
}
