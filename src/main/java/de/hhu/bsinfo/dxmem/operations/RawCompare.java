package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.Context;

/**
 * A class for compare Operations based on physical addresses.
 *
 * @author Lars Mehnert
 */
@PinnedMemory
public class RawCompare {

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context Context
     */
    public RawCompare(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Compare a byte at a given address + offset with a given byte.
     *
     * @param p_address physical address
     * @param p_offset  offset that will be added on p_address
     * @param p_suspect byte which will be compared
     * @return true if the given byte are equal to the read byte
     */
    public boolean compare(final long p_address, final int p_offset, final byte p_suspect) {
        return m_context.getHeap().readByte(p_address, p_offset) == p_suspect;
    }

    /**
     * Compares a byte array at a given address + offset with a given byte array.
     * It iterates over the byte array and will return if it found a unequal at current iteration.
     * <p>
     * Special attention to the range it will ce compare. The length of the given byte array is equal to the
     * number of iterations.
     *
     * @param p_address physical address
     * @param p_offset  offset that will be added on p_address
     * @param p_suspect byte array which will be compared
     * @return true if the byte array is completely equal.
     * @see {@link #compare(long, int, byte)}
     */
    public boolean compare(final long p_address, final int p_offset, final byte[] p_suspect) {
        int offset = p_offset;
        for (int i = 0; i < p_suspect.length; i++) {
            if (compare(p_address, offset, p_suspect[i]))
                return false;
            offset += Byte.BYTES;
        }
        return true;
    }

}
