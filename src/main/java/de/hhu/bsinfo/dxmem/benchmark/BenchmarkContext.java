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
     * Create a single chunk on the current instance
     *
     * @param p_size
     *         Size of chunk
     * @return CID of chunk created
     */
    long create(final int p_size);

    /**
     * Dump the heap of the current instance to a file
     *
     * @param p_outFile
     *         File to dump to
     */
    void dump(final String p_outFile);

    /**
     * Get the data of a chunk
     *
     * @param p_chunk
     *         Chunk to get
     */
    void get(final AbstractChunk p_chunk);

    /**
     * Put the data of a chunk
     *
     * @param p_chunk
     *         Chunk to put
     */
    void put(final AbstractChunk p_chunk);

    /**
     * Remove a chunk
     *
     * @param p_chunk
     *         Chunk to remove
     */
    void remove(final AbstractChunk p_chunk);
}
