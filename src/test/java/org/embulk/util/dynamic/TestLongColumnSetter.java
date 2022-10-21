/*
 * Copyright 2021 The Embulk project
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.math.DoubleMath;
import java.math.RoundingMode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestLongColumnSetter {
    @ParameterizedTest
    @CsvSource({
            "-1.500000000001, -2",
            "-1.5, -2",
            "-0.5, -1",
            "-0.499999999999, 0",
            "0.0, 0",
            "0.2, 0",
            "0.499999999999, 0",
            "0.5, 1",
            "0.7, 1",
            "1.3, 1",
            "1.5, 2",
            "1.500000000001, 2",
            "2.499999999999, 2",
            "2.5, 3",
    })
    public void testDoubles(final String value, final String expected) {
        // At first, it should be equal to Guava's DoubleMath.roundToLong(v, RoundingMode.HALF_UP).
        assertEquals(Long.parseLong(expected), DoubleMath.roundToLong(Double.parseDouble(value), RoundingMode.HALF_UP));

        final PageBuilder mockedPageBuilder = mock(PageBuilder.class);
        final LongColumnSetter setter = create(mockedPageBuilder);
        setter.set(Double.parseDouble(value));
        verify(mockedPageBuilder).setLong(any(Column.class), eq(Long.parseLong(expected)));
    }

    private static LongColumnSetter create(final PageBuilder pageBuilder) {
        return new LongColumnSetter(
                pageBuilder,
                new Column(1, "foo", Types.LONG),
                new NullDefaultValueSetter());
    }
}
