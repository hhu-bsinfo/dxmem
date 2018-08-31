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

package de.hhu.bsinfo.dxmem;

public final class DXMemoryTestConstants {
    public static final short NODE_ID = (short) 0xBEEF;

    public static final long HEAP_SIZE_SMALL = 1024 * 1024;
    public static final long HEAP_SIZE_MEDIUM = 1024 * 1024 * 128;
    public static final long HEAP_SIZE_LARGE = 1024 * 1024 * 1024 * 4L;

    public static final int CHUNK_SIZE_1 = 1;
    public static final int CHUNK_SIZE_2 = 16;
    public static final int CHUNK_SIZE_3 = 64;
    public static final int CHUNK_SIZE_4 = 1024;
    public static final int CHUNK_SIZE_5 = 1024 * 1024;
    public static final int CHUNK_SIZE_6 = 1024 * 1024 * 128;

    public static final int CHUNK_SIZE_1_COUNT = 100;
    public static final int CHUNK_SIZE_2_COUNT = 100;
    public static final int CHUNK_SIZE_3_COUNT = 100;
    public static final int CHUNK_SIZE_4_COUNT = 30;
    public static final int CHUNK_SIZE_5_COUNT = 10;
    public static final int CHUNK_SIZE_6_COUNT = 4;

    public static final int CHUNK_SIZE_1_MANY_COUNT = 10000000;
    public static final int CHUNK_SIZE_2_MANY_COUNT = 1024 * 1024 * 4;
    public static final int CHUNK_SIZE_3_MANY_COUNT = 1024 * 1024;
    public static final int CHUNK_SIZE_4_MANY_COUNT = 1024 * 128;
    public static final int CHUNK_SIZE_5_MANY_COUNT = 4000;
    public static final int CHUNK_SIZE_6_MANY_COUNT = 12;

    private DXMemoryTestConstants() {

    }
}
