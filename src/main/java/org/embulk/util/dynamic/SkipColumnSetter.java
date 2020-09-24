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
import org.msgpack.value.Value;

public class SkipColumnSetter extends AbstractDynamicColumnSetter {
    private SkipColumnSetter() {
        super(null, null, null);
    }

    public static SkipColumnSetter get() {
        return instance;
    }

    @Override
    public void setNull() {}

    @Override
    public void set(boolean v) {}

    @Override
    public void set(long v) {}

    @Override
    public void set(double v) {}

    @Override
    public void set(String v) {}

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final org.embulk.spi.time.Timestamp v) {}

    @Override
    public void set(Instant v) {}

    @Override
    public void set(Value v) {}

    private static final SkipColumnSetter instance = new SkipColumnSetter();
}
