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

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class NullDefaultValueSetter implements DefaultValueSetter {
    @Override
    public void setBoolean(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }

    @Override
    public void setLong(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }

    @Override
    public void setDouble(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }

    @Override
    public void setString(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }

    @Override
    public void setTimestamp(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }

    @Override
    public void setJson(final PageBuilder pageBuilder, final Column c) {
        pageBuilder.setNull(c);
    }
}
