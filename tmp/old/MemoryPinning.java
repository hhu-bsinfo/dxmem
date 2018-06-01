package de.hhu.bsinfo.dxmem.old;

import de.hhu.bsinfo.dxmem.core.CIDTable;
import de.hhu.bsinfo.dxmem.core.Heap;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 03.03.18
 * @projectname dxmem-memory
 */
public class MemoryPinning {
    private final Heap heap;
    private final CIDTable cidTable;

    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    MemoryPinning(MemoryManager memoryManager){
        heap = memoryManager.heap;
        cidTable = memoryManager.cidTable;
    }

    /**
     * Pin a chunk for direct access
     *
     * @param chunkID Chunk ID to pin
     * @return The CIDTable entry
     */
    public long pinChunk(final long chunkID){
        long directEntryAddress = cidTable.getAddressOfEntry(chunkID);

        if(cidTable.directSetState(directEntryAddress, STATE_NOT_MOVEABLE, true)){
            return WRITE_ACCESS.set(READ_ACCESS.set(cidTable.directGet(directEntryAddress), 0), false);
        }

        return 0;
    }

    /**
     * Unpin a Chunks (slow operation need a DFS over all CIDTable tables)
     *
     * @param cidTableEntry CIDTable entry.
     * @return The corresponding chunk ID or -1 if no suitable chunk ID was found
     */
    public long unpinChunk(final long cidTableEntry){
        long cid = cidTable.reverseSearch(cidTableEntry);
        if(cid != 0)
            cidTable.setState(cid, STATE_NOT_MOVEABLE, false);

        return cid;
    }

    /**
     * Get the data for a entry
     *
     * @param cidTableEntry CIDTable entry
     * @return Chunk data
     */
    public byte[] get(final long cidTableEntry){
        assert heap.assertMemoryBounds(ADDRESS.get(cidTableEntry));

        int size = heap.getSizeDataBlock(cidTableEntry);

        byte[] data = new byte[size];
        heap.readBytes(cidTableEntry, 0, data, 0, size);

        return data;
    }

    /**
     * Put data to a entry
     *
     * @param cidTableEntry Entry
     * @param data Data to put
     */
    public boolean put(final long cidTableEntry, final byte[] data){
        assert data != null;
        boolean ok = heap.assertMemoryBounds(ADDRESS.get(cidTableEntry));
        if(ok)
            heap.writeBytes(cidTableEntry, 0, data, 0, data.length);

        return ok;
    }
}
