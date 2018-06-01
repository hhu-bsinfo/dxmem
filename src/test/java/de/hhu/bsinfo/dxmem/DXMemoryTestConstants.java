package de.hhu.bsinfo.dxmem;

public final class DXMemoryTestConstants {
    static final short NODE_ID = (short) 0xBEEF;

    static final long HEAP_SIZE_SMALL = 1024 * 1024;
    static final long HEAP_SIZE_MEDIUM = 1024 * 1024 * 128;
    static final long HEAP_SIZE_LARGE = 1024 * 1024 * 1024 * 4L;

    static final int CHUNK_SIZE_1 = 1;
    static final int CHUNK_SIZE_2 = 16;
    static final int CHUNK_SIZE_3 = 64;
    static final int CHUNK_SIZE_4 = 1024;
    static final int CHUNK_SIZE_5 = 1024 * 1024;
    static final int CHUNK_SIZE_6 = 1024 * 1024 * 128;

    static final int CHUNK_SIZE_1_COUNT = 100;
    static final int CHUNK_SIZE_2_COUNT = 100;
    static final int CHUNK_SIZE_3_COUNT = 100;
    static final int CHUNK_SIZE_4_COUNT = 30;
    static final int CHUNK_SIZE_5_COUNT = 10;
    static final int CHUNK_SIZE_6_COUNT = 4;

    static final int CHUNK_SIZE_1_MANY_COUNT = 10000000;
    static final int CHUNK_SIZE_2_MANY_COUNT = 1024 * 1024 * 4;
    static final int CHUNK_SIZE_3_MANY_COUNT = 1024 * 1024;
    static final int CHUNK_SIZE_4_MANY_COUNT = 1024 * 128;
    static final int CHUNK_SIZE_5_MANY_COUNT = 4000;
    static final int CHUNK_SIZE_6_MANY_COUNT = 12;

    private DXMemoryTestConstants() {

    }
}
