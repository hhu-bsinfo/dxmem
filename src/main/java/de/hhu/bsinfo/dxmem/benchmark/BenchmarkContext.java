package de.hhu.bsinfo.dxmem.benchmark;

import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;

/**
 * Interface for context used by benchmark and its operations
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.2018
 */
public interface BenchmarkContext {
    /**
     * Get the heap status of the current local instance
     *
     * @return HeapStatus
     */
    HeapStatus getHeapStatus();

    /**
     * Get the CID table status of the current local instance
     *
     * @return CIDTableStatus
     */
    CIDTableStatus getCIDTableStatus();

    /**
     * Get the LID store status of the current local instance
     *
     * @return LIDStoreStatus
     */
    LIDStoreStatus getLIDStoreStatus();

    /**
     * Create one or multiple chunks on the current instance
     *
     * @param p_cids
     *         Chunk ids of chunks created
     * @param p_sizes
     *         Sizes of chunks
     */
    void create(final long[] p_cids, final int[] p_sizes);

    /**
     * Dump the heap of the current instance to a file
     *
     * @param p_outFile
     *         File to dump to
     */
    void dump(final String p_outFile);

    /**
     * Get the data of chunks
     *
     * @param p_chunks
     *         Chunks to get
     */
    void get(final AbstractChunk[] p_chunks);

    /**
     * Put the data of chunks
     *
     * @param p_chunks
     *         Chunks to put
     */
    void put(final AbstractChunk[] p_chunks);

    /**
     * Remove chunks
     *
     * @param p_chunks
     *         Chunks to remove
     */
    void remove(final AbstractChunk[] p_chunks);
}
