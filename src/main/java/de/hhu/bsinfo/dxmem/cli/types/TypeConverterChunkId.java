package de.hhu.bsinfo.dxmem.cli.types;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.NodeID;

public class TypeConverterChunkId implements CommandLine.ITypeConverter<Long> {
    @Override
    public Long convert(final String p_value) throws Exception {
        String[] args = p_value.split("/");

        if (args.length == 2) {
            return ChunkID.getChunkID(NodeID.parse(args[0]), ChunkID.parse(args[1]));
        } else {
            return ChunkID.parse(p_value);
        }
    }
}
