package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.HeapDataStructureImExporter;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

public class Recovery {
    private static final ThroughputPool SOP_CREATE_AND_PUT_RAW = new ThroughputPool(Create.class,
            "CreateAndPutRecoveredRaw", Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE_AND_PUT_DS = new ThroughputPool(Create.class,
            "CreateAndPutRecoveredDS", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(Create.class, SOP_CREATE_AND_PUT_RAW);
        StatisticsManager.get().registerOperation(Create.class, SOP_CREATE_AND_PUT_DS);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context with core components
     */
    public Recovery(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkIDs
     *         List of recovered chunk ids
     * @param p_dataAddress
     *         The address of the recovered data block
     * @param p_offsets
     *         Offset list for chunks to address the data array
     * @param p_lengths
     *         List of chunk sizes
     * @param p_usedEntries
     *         Specifies the actual number of slots used in the array (may be less than p_lengths)
     */
    public long createAndPutRecovered(final long[] p_cids, final long p_dataAddress, final int[] p_offsets,
            final int[] p_lengths, final int p_usedEntries) {
        long totalSize = 0;

        SOP_CREATE_AND_PUT_RAW.start(p_usedEntries);

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_usedEntries];

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getHeap().malloc(entries, p_lengths, 0, p_usedEntries);

        for (int i = 0; i < p_usedEntries; i++) {
            m_context.getCIDTable().insert(p_cids[i], entries[i]);
        }

        for (int i = 0; i < p_usedEntries; i++) {
            m_context.getHeap().copyNative(entries[i].getAddress(), 0, p_dataAddress, p_offsets[i], p_lengths[i]);
            totalSize += p_lengths[i];
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_AND_PUT_RAW.stop();

        return totalSize;
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunks
     *         All data structure to create and put
     * @return number of written bytes
     */
    public long createAndPutRecovered(final AbstractChunk... p_chunks) {
        long totalSize = 0;
        int[] sizes = new int[p_chunks.length];

        for (int i = 0; i < p_chunks.length; i++) {
            sizes[i] = p_chunks[i].sizeofObject();
            totalSize += sizes[i];
        }

        SOP_CREATE_AND_PUT_DS.start(sizes.length);

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_chunks.length];

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getHeap().malloc(entries, sizes);

        for (int i = 0; i < p_chunks.length; i++) {
            m_context.getCIDTable().insert(p_chunks[i].getID(), entries[i]);
        }

        HeapDataStructureImExporter exporter = m_context.getDataStructureImExporterPool().get();

        for (int i = 0; i < p_chunks.length; i++) {
            exporter.setHeapAddress(entries[i].getAddress());
            exporter.exportObject(p_chunks[i]);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_AND_PUT_DS.stop();

        return totalSize;
    }
}
