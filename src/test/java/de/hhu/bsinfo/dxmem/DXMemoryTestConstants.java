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
    public  static final int CHUNK_SIZE_3_COUNT = 100;
    public static final int CHUNK_SIZE_4_COUNT = 30;
    public static final int CHUNK_SIZE_5_COUNT = 10;
    public static final int CHUNK_SIZE_6_COUNT = 4;

    public static final int CHUNK_SIZE_1_MANY_COUNT = 10000000;
    public static final int CHUNK_SIZE_2_MANY_COUNT = 1024 * 1024 * 4;
    public  static final int CHUNK_SIZE_3_MANY_COUNT = 1024 * 1024;
    public static final int CHUNK_SIZE_4_MANY_COUNT = 1024 * 128;
    public static final int CHUNK_SIZE_5_MANY_COUNT = 4000;
    public static final int CHUNK_SIZE_6_MANY_COUNT = 12;

    private DXMemoryTestConstants() {

    }
}
