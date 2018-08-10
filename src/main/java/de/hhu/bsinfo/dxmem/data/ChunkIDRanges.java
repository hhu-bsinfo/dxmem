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

package de.hhu.bsinfo.dxmem.data;

import java.util.concurrent.ThreadLocalRandom;

import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Store one or multiple chunk ID ranges. All longs are treated unsigned!
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 16.03.2017
 */
public class ChunkIDRanges implements Importable, Exportable {
    private ArrayListLong m_ranges;

    /**
     * Default constructor, contains no ranges.
     */
    public ChunkIDRanges() {
        m_ranges = new ArrayListLong(2);
    }

    /**
     * Constructor with one range
     *
     * @param p_start
     *         Start of range (including)
     * @param p_end
     *         End of range (including)
     * @throws IllegalArgumentException
     *         If p_start > p_end
     */
    public ChunkIDRanges(final long p_start, final long p_end) {
        if (!isLessThanOrEqualsUnsigned(p_start, p_end)) {
            throw new IllegalArgumentException(p_start + " > " + p_end);
        }

        m_ranges = new ArrayListLong(2);
        m_ranges.add(p_start);
        m_ranges.add(p_end);
    }

    /**
     * Constructor
     *
     * @param p_ranges
     *         Ranges to use (either copy or wrap)
     * @param p_copy
     *         True to copy the contents of the array, false to wrap the array
     * @throws IllegalArgumentException
     *         If Range size % 2 != 0
     */
    private ChunkIDRanges(final long[] p_ranges, final boolean p_copy) {
        if (p_ranges.length % 2 != 0) {
            throw new IllegalArgumentException("Ranges size % 2 != 0");
        }

        if (p_copy) {
            m_ranges = ArrayListLong.copy(p_ranges);
        } else {
            m_ranges = ArrayListLong.wrap(p_ranges);
        }
    }

    /**
     * Constructor
     *
     * @param p_arrayList
     *         ArrayListLong to use (either copy or wrap)
     * @param p_copy
     *         True to copy the contents of the list, false to wrap the list
     * @throws IllegalArgumentException
     *         If Range size % 2 != 0
     */
    private ChunkIDRanges(final ArrayListLong p_arrayList, final boolean p_copy) {
        if (p_arrayList.getSize() % 2 != 0) {
            throw new IllegalArgumentException("Ranges size % 2 != 0");
        }

        if (p_copy) {
            m_ranges = new ArrayListLong(p_arrayList);
        } else {
            m_ranges = p_arrayList;
        }
    }

    /**
     * Create a new ChunkIDRanges object based on a copy of a provided range array
     *
     * @param p_ranges
     *         Range array with contents to copy
     * @return New ChunkIDRanges object with copied contents of array
     */
    public static ChunkIDRanges copy(final long[] p_ranges) {
        return new ChunkIDRanges(p_ranges, true);
    }

    /**
     * Create a new ChunkIDRanges object wrapping an existing array with ranges
     *
     * @param p_ranges
     *         Array with ranges to wrap
     * @return ChunkIDRanges Object with wrapped array
     */
    public static ChunkIDRanges wrap(final long[] p_ranges) {
        return new ChunkIDRanges(p_ranges, false);
    }

    /**
     * Create a new ChunkIDRanges object based on a copy of a provided range list
     *
     * @param p_arrayList
     *         ArrayListLong with ranges to copy
     * @return ChunkIDRanges object with copied contents of the list
     */
    public static ChunkIDRanges copy(final ArrayListLong p_arrayList) {
        return new ChunkIDRanges(p_arrayList, true);
    }

    /**
     * Create a new ChunkIDRanges object wrapping an existing ArrayListLong with ranges
     *
     * @param p_arrayList
     *         ArrayListLong with ranges to wrap
     * @return ChunkIDRanges Object with wrapped array
     */
    public static ChunkIDRanges wrap(final ArrayListLong p_arrayList) {
        return new ChunkIDRanges(p_arrayList, false);
    }

    /**
     * Get the number of ranges
     *
     * @return Num of ranges
     */
    public int size() {
        return m_ranges.getSize() / 2;
    }

    /**
     * Check if no ranges available
     *
     * @return True if no ranges available, false otherwise
     */
    public boolean isEmpty() {
        return m_ranges.isEmpty();
    }

    /**
     * Get the start of a range
     *
     * @param p_rangeIndex
     *         Index of the range
     * @return Start value
     */
    public long getRangeStart(final int p_rangeIndex) {
        return m_ranges.get(p_rangeIndex * 2);
    }

    /**
     * Get the end of a range
     *
     * @param p_rangeIndex
     *         Index of the range
     * @return End value
     */
    public long getRangeEnd(final int p_rangeIndex) {
        return m_ranges.get(p_rangeIndex * 2 + 1);
    }

    /**
     * Add a new cid to the range
     *
     * @param p_cid
     *         Cid to add
     */
    public void add(final long p_cid) {
        add(p_cid, p_cid);
    }

    /**
     * Add a range
     *
     * @param p_start
     *         Start of the range (including)
     * @param p_end
     *         End of the range (including)
     * @throws IllegalArgumentException
     *         If p_start > p_end
     */
    public void add(final long p_start, final long p_end) {
        assert p_start <= p_end;

        // empty range
        if (m_ranges.isEmpty()) {
            m_ranges.add(p_start);
            m_ranges.add(p_end);
            return;
        }

        int posStart = -1;

        // iterate existing range list and try to find position of the predecessor for the new entry
        for (int i = m_ranges.getSize() - 2; i >= 0; i -= 2) {
            if (m_ranges.get(i) <= p_start) {
                posStart = i;
                break;
            }
        }

        long[] curRange = new long[2];
        long[] nextRange = new long[2];

        // add to front
        if (posStart == -1) {
            curRange[0] = p_start;
            curRange[1] = p_end;

            posStart = 0;

            nextRange[0] = m_ranges.get(posStart);
            nextRange[1] = m_ranges.get(posStart + 1);

        } else {
            curRange[0] = m_ranges.get(posStart);
            curRange[1] = m_ranges.get(posStart + 1);

            nextRange[0] = p_start;
            nextRange[1] = p_end;
        }

        int entryCount = m_ranges.getSize();
        int nextPos = posStart + 2;

        while (true) {
            // compare current and next range

            // isolated, e.g. [5,7][9,11] -> [5,7][9,11]

            // full overlap (1), e.g. [5,7][5,7] -> [5,7]
            // full overlap (2), e.g. [5,12][6,11] -> [5,12]
            // partial overlap (1), e.g. [5,7][6,9] -> [5,9]
            // partial overlap (2), e.g. [5,7][6,7] -> [5,7]
            // adjacent, e.g. [5,7][8,11] -> [5,11]

            // isolated/non isolated
            if (curRange[1] + 1 < nextRange[0]) {
                m_ranges.add(posStart++, curRange[0]);
                m_ranges.add(posStart++, curRange[1]);

                curRange[0] = nextRange[0];
                curRange[1] = nextRange[1];

                // no more next values, add remainder range to end
                if (nextPos >= entryCount) {
                    m_ranges.add(posStart++, curRange[0]);
                    m_ranges.add(posStart++, curRange[1]);
                    break;
                } else {
                    nextRange[0] = m_ranges.get(nextPos++);
                    nextRange[1] = m_ranges.get(nextPos++);
                }
            } else {
                // have to merge two ranges
                long start = Math.min(curRange[0], Math.min(curRange[1], Math.min(nextRange[0], nextRange[1])));
                long end = Math.max(curRange[0], Math.max(curRange[1], Math.max(nextRange[0], nextRange[1])));

                m_ranges.add(posStart, start);
                m_ranges.add(posStart + 1, end);

                curRange[0] = start;
                curRange[1] = end;

                // no more next values
                if (nextPos >= entryCount) {
                    // avoid trim of current vals
                    posStart += 2;
                    break;
                } else {
                    nextRange[0] = m_ranges.get(nextPos++);
                    nextRange[1] = m_ranges.get(nextPos++);
                }
            }
        }

        // removed unused values at the end
        m_ranges.trim(posStart);
    }

    /**
     * Add all ranges of another ChunkIDRanges instance
     *
     * @param p_other
     *         Other instance to add the ranges of
     */
    public void add(final ChunkIDRanges p_other) {
        assert p_other.m_ranges.getSize() % 2 == 0;

        for (int i = 0; i < p_other.m_ranges.getSize(); i += 2) {
            add(p_other.m_ranges.get(i), p_other.m_ranges.get(i + 1));
        }
    }

    /**
     * Remove a cid from the range list
     *
     * @param p_cid
     *         Cid to remove from ranges
     */
    public boolean remove(final long p_cid) {
        // [1,1] - 1 - []
        // [1,10] - 1 -> [2,10]
        // [1,10] - 10 -> [1,9]
        // [1,10] - 5 -> [1,4][6,10]

        if (m_ranges.isEmpty()) {
            return false;
        }

        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            long start = m_ranges.get(i);
            long end = m_ranges.get(i + 1);

            if (start <= p_cid && p_cid <= end) {
                // delete single size range
                if (start == end) {
                    // [1,1] - 1 - []
                    m_ranges.remove(i);
                    m_ranges.remove(i + 1);
                    return true;
                } else {
                    // [1,10] - 1 -> [2,10]
                    if (p_cid == start) {
                        m_ranges.set(i, start + 1);
                        return true;
                    }

                    // [1,10] - 10 -> [1,9]
                    if (p_cid == end) {
                        m_ranges.set(i + 1, end - 1);
                        return true;
                    }

                    // [1,10] - 5 -> [1,4][6,10]
                    long start2 = p_cid + 1;
                    long end2 = end;
                    end = p_cid - 1;

                    // insert new range and shift all following ranges
                    m_ranges.set(i + 1, end);
                    m_ranges.insert(i + 2, start2, end2);

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Remove a range of cids from the ranges list
     *
     * @param p_start
     *         Start cid of range to remove
     * @param p_end
     *         End cid of range to remove
     * @return Number of elements removed
     */
    public long remove(final long p_start, final long p_end) {
        assert p_start <= p_end;

        long count = 0;

        for (long l = p_start; l <= p_end; l++) {
            if (remove(l)) {
                count++;
            }
        }

        return count;
    }

    /**
     * Check if a chunk ID is within the ranges
     *
     * @param p_chunkID
     *         Chunk ID to test
     * @return True if chunk ID is within a range (including)
     */
    public boolean isInRange(final long p_chunkID) {
        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            if (m_ranges.get(i) <= p_chunkID && p_chunkID <= m_ranges.get(i + 1)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if a given range is within the ranges
     *
     * @param p_start
     *         Range start
     * @param p_end
     *         Range end
     * @return Number of items within the range specified
     */
    public long isInRange(final long p_start, final long p_end) {
        assert p_start <= p_end;

        if (m_ranges.isEmpty()) {
            return 0;
        }

        // full overlap (1), e.g. [1,10] [2,5] -> 4
        // full overlap (2), e.g. [1,4] [1,4] -> 4
        // partial overlap (1), e.g. [1,7] [3,8] -> 5
        // partial overlap (2), e.g. [5,10] [1,8] -> 4
        // multi overlap, e.g. [1,5][7,10] [0,15] -> 10
        // gap'd, e.g. [1,5][7,10][12,15] [6,11] -> 4

        long overlapCount = 0;

        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            long curStart = m_ranges.get(i);
            long curEnd = m_ranges.get(i + 1);

            // intersect cur range with range to check

            // range to check full overlap
            if (curStart <= p_start && p_end <= curEnd) {
                return p_end - p_start + 1;
            }

            if (p_start <= curStart && curEnd <= p_end) {
                overlapCount += curEnd - curStart + 1;
            } else if (curStart <= p_start && p_start <= curEnd) {
                // start overlapping with range
                overlapCount += curEnd - p_start + 1;
            } else if (curStart <= p_end && p_end <= curEnd) {
                overlapCount += p_end - curStart + 1;
            }
        }

        return overlapCount;
    }

    /**
     * Get a random chunk ID from a random range
     *
     * @return Random chunk ID of random range
     */
    public long getRandomCidWithinRanges() {
        if (isEmpty()) {
            return ChunkID.INVALID_ID;
        }

        int rangeIdx = getRandomRangeExclEnd(0, m_ranges.getSize() / 2);

        return getRandomChunkId(m_ranges.get(rangeIdx * 2), m_ranges.get(rangeIdx * 2 + 1));
    }

    /**
     * Get the total number of chunk IDs covered by the ranges
     *
     * @return Total number of chunk IDs covered
     */
    public long getTotalCidsOfRanges() {
        long count = 0;

        if (isEmpty()) {
            return 0;
        }

        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            long rangeStart = m_ranges.get(i);
            long rangeEnd = m_ranges.get(i + 1);

            if (rangeEnd != rangeStart) {
                count += rangeEnd - rangeStart + 1;
            } else {
                count++;
            }
        }

        return count;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.exportObject(m_ranges);
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.importObject(m_ranges);
    }

    @Override
    public int sizeofObject() {
        return m_ranges.sizeofObject();
    }

    @Override
    public String toString() {
        if (!isEmpty()) {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < m_ranges.getSize(); i += 2) {
                builder.append(String.format("[0x%X, 0x%X]", m_ranges.get(i), m_ranges.get(i + 1)));
            }

            return builder.toString();
        } else {
            return "[]";
        }
    }

    @Override
    public boolean equals(final Object p_object) {
        return p_object instanceof ChunkIDRanges && ((ChunkIDRanges) p_object).m_ranges.equals(m_ranges);
    }

    /**
     * Get a random chunk ID from a range
     *
     * @param p_start
     *         Start of the range (including)
     * @param p_end
     *         End of the range (including)
     * @return Random chunk ID of range
     */
    private static long getRandomChunkId(final long p_start, final long p_end) {
        if (ChunkID.getCreatorID(p_start) != ChunkID.getCreatorID(p_end)) {
            return ChunkID.INVALID_ID;
        }

        return getRandomRange(p_start, p_end);
    }

    /**
     * Get a random range
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (including)
     * @return Random range
     */
    private static int getRandomRange(final int p_start, final int p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        return (int) (ThreadLocalRandom.current().nextDouble() * (p_end - p_start + 1) + p_start);
    }

    /**
     * Get a random range excluding the end
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (excluding)
     * @return Random range
     */
    private static int getRandomRangeExclEnd(final int p_start, final int p_end) {
        return (int) (ThreadLocalRandom.current().nextDouble() * (p_end - p_start) + p_start);
    }

    /**
     * Get a random range
     *
     * @param p_start
     *         Start (including)
     * @param p_end
     *         End (including)
     * @return Random range
     */
    private static long getRandomRange(final long p_start, final long p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        long tmp = (long) (ThreadLocalRandom.current().nextDouble() * (p_end - p_start + 1));
        return tmp + p_start;
    }

    /**
     * Unsigned comparison of two long values
     *
     * @param p_n1
     *         Value 1
     * @param p_n2
     *         Value 2
     * @return True if p_n1 <= p_n2
     */
    private static boolean isLessThanOrEqualsUnsigned(final long p_n1, final long p_n2) {
        return p_n1 <= p_n2 ^ p_n1 < 0 != p_n2 < 0;
    }
}
