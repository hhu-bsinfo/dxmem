/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.cli.types;

import picocli.CommandLine;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class TypeConverterStorageUnit implements CommandLine.ITypeConverter<StorageUnit> {
    @Override
    public StorageUnit convert(final String p_value) throws Exception {
        String[] splitSize = p_value.split("-");

        if (splitSize.length > 2) {
            throw new IllegalArgumentException();
        }

        if (splitSize.length == 2) {
            return new StorageUnit(Long.parseLong(splitSize[0]), splitSize[1]);
        } else {
            return new StorageUnit(Long.parseLong(splitSize[0]), StorageUnit.BYTE);
        }
    }
}
