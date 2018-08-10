package de.hhu.bsinfo.dxmem.cli.types;

import picocli.CommandLine;

import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class TypeConverterNodeId implements CommandLine.ITypeConverter<Short> {
    @Override
    public Short convert(final String p_value) throws Exception {
        return NodeID.parse(p_value);
    }
}
