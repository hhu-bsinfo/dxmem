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
