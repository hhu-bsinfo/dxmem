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

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class CreateMulti {
    private final Context m_context;

    public CreateMulti(final Context p_context) {
        m_context = p_context;
    }

    public int create(final long[] p_chunkIDs, final int p_offset, final int p_count, final int p_size,
            final boolean p_consecutiveIDs) {
        assert p_size > 0;
        assert p_count > 0;
        assert p_offset >= 0;
        assert p_chunkIDs.length >= p_count;

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (p_consecutiveIDs) {
            m_context.getLIDStore().getConsecutive(p_chunkIDs, p_offset, p_count);
        } else {
            m_context.getLIDStore().get(p_chunkIDs, p_offset, p_count);
        }

        // create CIDs from LIDs
        for (int i = 0; i < p_count; i++) {
            p_chunkIDs[p_offset + i] = ChunkID.getChunkID(m_context.getNodeId(), p_chunkIDs[p_offset + i]);
        }

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_count];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int successfulMallocs = m_context.getHeap().malloc(p_size, p_count, entries);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            m_context.getCIDTable().insert(p_chunkIDs[p_offset + i], entries[i]);
        }

        // put back or flag as zombies: entries of non successful allocs (rare case)
        if (successfulMallocs != p_count) {
            // put back LIDs that could not be used (after they were added to the table because they might be marked
            // as zombies if LID store is full)
            for (int i = successfulMallocs; i < p_count; i++) {
                if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_chunkIDs[p_offset + i]))) {
                    // lid store full, flag as zombie
                    m_context.getCIDTable().entryFlagZombie(entries[i]);
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return successfulMallocs;
    }

    public int create(final long[] p_chunkIDs, final int p_offset, final boolean p_consecutiveIDs,
            final int... p_sizes) {
        assert p_chunkIDs != null;
        assert p_offset >= 0;
        assert p_sizes != null;
        assert p_chunkIDs.length >= p_sizes.length;

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (p_consecutiveIDs) {
            m_context.getLIDStore().getConsecutive(p_chunkIDs, p_offset, p_sizes.length);
        } else {
            m_context.getLIDStore().get(p_chunkIDs, p_offset, p_sizes.length);
        }

        // create CIDs from LIDs
        for (int i = 0; i < p_sizes.length; i++) {
            p_chunkIDs[p_offset + i] = ChunkID.getChunkID(m_context.getNodeId(), p_chunkIDs[p_offset + i]);
        }

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_sizes.length];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int successfulMallocs = m_context.getHeap().malloc(entries, p_sizes);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            m_context.getCIDTable().insert(p_chunkIDs[p_offset + i], entries[i]);
        }

        // put back or flag as zombies: entries of non successful allocs (rare case)
        if (successfulMallocs != p_sizes.length) {
            // put back LIDs that could not be used (after they were added to the table because they might be marked
            // as zombies if LID store is full)
            for (int i = successfulMallocs; i < p_sizes.length; i++) {
                if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_chunkIDs[p_offset + i]))) {
                    // lid store full, flag as zombie
                    m_context.getCIDTable().entryFlagZombie(entries[i]);
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return successfulMallocs;
    }

    public int create(final int p_offset, final int p_count, final boolean p_consecutiveIDs,
            final AbstractChunk... p_chunks) {
        assert p_chunks != null;

        long[] cids = new long[p_count];
        int[] sizes = new int[p_count];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = p_chunks[p_offset + i].sizeofObject();
        }

        int successfullMallocs = create(cids, 0, p_consecutiveIDs, sizes);

        for (int i = 0; i < successfullMallocs; i++) {
            p_chunks[p_offset + i].setID(cids[i]);
            p_chunks[p_offset + i].setState(ChunkState.OK);
        }

        for (int i = successfullMallocs; i < p_count; i++) {
            p_chunks[p_offset + i].setID(ChunkID.INVALID_ID);
            p_chunks[p_offset + i].setState(ChunkState.DOES_NOT_EXIST);
        }

        return successfullMallocs;
    }
}
