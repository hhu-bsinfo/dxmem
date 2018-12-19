package de.hhu.bsinfo.dxmem.data;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Special Im-/Exporter for testing the import and export methods of chunk implementations
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.11.2018
 */
class ChunkTesterImExporter implements Importer, Exporter {
    private final int m_initialSize;
    private final ByteBuffer m_buffer;
    private final ByteBufferImExporter m_imExporter;

    private final int m_randomAbortAtPos;

    private int m_currentPos;

    /**
     * Constructor
     *
     * @param p_buffer
     *         ByteBuffer with serialized chunk data for importing
     * @param p_randomAbort
     *         True to randomly abort serialization, false otherwise
     */
    ChunkTesterImExporter(final ByteBuffer p_buffer, final boolean p_randomAbort) {
        m_buffer = ByteBuffer.allocate(p_buffer.capacity());
        p_buffer.rewind();
        m_buffer.put(p_buffer);
        p_buffer.rewind();
        m_buffer.flip();

        m_initialSize = p_buffer.capacity();
        m_imExporter = new ByteBufferImExporter(m_buffer);

        if (p_randomAbort) {
            m_randomAbortAtPos = RandomUtils.getRandomValue(0, m_initialSize);
        } else {
            m_randomAbortAtPos = -1;
        }
    }

    /**
     * Constructor
     *
     * @param p_chunk
     *         Chunk to export
     * @param p_randomAbort
     *         True to randomly abort serialization, false otherwise
     */
    ChunkTesterImExporter(final AbstractChunk p_chunk, final boolean p_randomAbort) {
        m_initialSize = p_chunk.sizeofObject();
        m_buffer = ByteBuffer.allocate(m_initialSize);
        m_imExporter = new ByteBufferImExporter(m_buffer);

        if (p_randomAbort) {
            m_randomAbortAtPos = RandomUtils.getRandomValue(0, m_initialSize);
        } else {
            m_randomAbortAtPos = -1;
        }
    }

    /**
     * Get the backed byte buffer with the serialized data
     *
     * @return ByteBuffer with serialized data
     */
    public ByteBuffer getBuffer() {
        return m_buffer;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        checkIfRandomAbort();
        m_imExporter.exportObject(p_object);
        m_currentPos += p_object.sizeofObject();
    }

    @Override
    public void writeBoolean(final boolean p_v) {
        checkIfRandomAbort();
        m_imExporter.writeBoolean(p_v);
        m_currentPos += Byte.BYTES;
    }

    @Override
    public void writeByte(final byte p_v) {
        checkIfRandomAbort();
        m_imExporter.writeByte(p_v);
        m_currentPos += Byte.BYTES;
    }

    @Override
    public void writeShort(final short p_v) {
        checkIfRandomAbort();
        m_imExporter.writeShort(p_v);
        m_currentPos += Short.BYTES;
    }

    @Override
    public void writeChar(final char p_v) {
        checkIfRandomAbort();
        m_imExporter.writeChar(p_v);
        m_currentPos += Character.BYTES;
    }

    @Override
    public void writeInt(final int p_v) {
        checkIfRandomAbort();
        m_imExporter.writeInt(p_v);
        m_currentPos += Integer.BYTES;
    }

    @Override
    public void writeLong(final long p_v) {
        checkIfRandomAbort();
        m_imExporter.writeLong(p_v);
        m_currentPos += Long.BYTES;
    }

    @Override
    public void writeFloat(final float p_v) {
        checkIfRandomAbort();
        m_imExporter.writeFloat(p_v);
        m_currentPos += Float.BYTES;
    }

    @Override
    public void writeDouble(final double p_v) {
        checkIfRandomAbort();
        m_imExporter.writeDouble(p_v);
        m_currentPos += Double.BYTES;
    }

    @Override
    public void writeCompactNumber(final int p_v) {
        checkIfRandomAbort();
        m_imExporter.writeCompactNumber(p_v);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_v);
    }

    @Override
    public void writeString(final String p_str) {
        checkIfRandomAbort();
        m_imExporter.writeString(p_str);
        m_currentPos += ObjectSizeUtil.sizeofString(p_str);
    }

    @Override
    public int writeBytes(final byte[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeBytes(p_array);
        m_currentPos += p_array.length * Byte.BYTES;
        return ret;
    }

    @Override
    public int writeShorts(final short[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeShorts(p_array);
        m_currentPos += p_array.length * Short.BYTES;
        return ret;
    }

    @Override
    public int writeChars(final char[] p_array) {
        checkIfRandomAbort();
        return m_imExporter.writeChars(p_array);
    }

    @Override
    public int writeInts(final int[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeInts(p_array);
        m_currentPos += p_array.length * Integer.BYTES;
        return ret;
    }

    @Override
    public int writeLongs(final long[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeLongs(p_array);
        m_currentPos += p_array.length * Long.BYTES;
        return ret;
    }

    @Override
    public int writeFloats(final float[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeFloats(p_array);
        m_currentPos += p_array.length * Float.BYTES;
        return ret;
    }

    @Override
    public int writeDoubles(final double[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeDoubles(p_array);
        m_currentPos += p_array.length * Double.BYTES;
        return ret;
    }

    @Override
    public int writeBytes(final byte[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeBytes(p_array, p_offset, p_length);
        m_currentPos += p_length * Byte.BYTES;
        return ret;
    }

    @Override
    public int writeShorts(final short[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeShorts(p_array, p_offset, p_length);
        m_currentPos += p_length * Short.BYTES;
        return ret;
    }

    @Override
    public int writeChars(final char[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeChars(p_array, p_offset, p_length);
        m_currentPos += p_length * Character.BYTES;
        return ret;
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeInts(p_array, p_offset, p_length);
        m_currentPos += p_length * Integer.BYTES;
        return ret;
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeLongs(p_array, p_offset, p_length);
        m_currentPos += p_length * Long.BYTES;
        return ret;
    }

    @Override
    public int writeFloats(final float[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeFloats(p_array, p_offset, p_length);
        m_currentPos += p_length * Float.BYTES;
        return ret;
    }

    @Override
    public int writeDoubles(final double[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.writeDoubles(p_array, p_offset, p_length);
        m_currentPos += p_length * Double.BYTES;
        return ret;
    }

    @Override
    public void writeByteArray(final byte[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeByteArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Byte.BYTES;
    }

    @Override
    public void writeShortArray(final short[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeShortArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Short.BYTES;
    }

    @Override
    public void writeCharArray(final char[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeCharArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Character.BYTES;
    }

    @Override
    public void writeIntArray(final int[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeIntArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Integer.BYTES;
    }

    @Override
    public void writeLongArray(final long[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeLongArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Long.BYTES;
    }

    @Override
    public void writeFloatArray(final float[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeFloatArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Float.BYTES;
    }

    @Override
    public void writeDoubleArray(final double[] p_array) {
        checkIfRandomAbort();
        m_imExporter.writeDoubleArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Double.BYTES;
    }

    @Override
    public void importObject(final Importable p_object) {
        checkIfRandomAbort();
        m_imExporter.importObject(p_object);
        m_currentPos += p_object.sizeofObject();
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        checkIfRandomAbort();
        boolean val = m_imExporter.readBoolean(p_bool);
        m_currentPos += Byte.BYTES;
        return val;
    }

    @Override
    public byte readByte(final byte p_byte) {
        checkIfRandomAbort();
        byte val = m_imExporter.readByte(p_byte);
        m_currentPos += Byte.BYTES;
        return val;
    }

    @Override
    public short readShort(final short p_short) {
        checkIfRandomAbort();
        short val = m_imExporter.readShort(p_short);
        m_currentPos += Short.BYTES;
        return val;
    }

    @Override
    public char readChar(final char p_char) {
        checkIfRandomAbort();
        char val = m_imExporter.readChar(p_char);
        m_currentPos += Character.BYTES;
        return val;
    }

    @Override
    public int readInt(final int p_int) {
        checkIfRandomAbort();
        int val = m_imExporter.readInt(p_int);
        m_currentPos += Integer.BYTES;
        return val;
    }

    @Override
    public long readLong(final long p_long) {
        checkIfRandomAbort();
        long val = m_imExporter.readLong(p_long);
        m_currentPos += Long.BYTES;
        return val;
    }

    @Override
    public float readFloat(final float p_float) {
        checkIfRandomAbort();
        float val = m_imExporter.readFloat(p_float);
        m_currentPos += Float.BYTES;
        return val;
    }

    @Override
    public double readDouble(final double p_double) {
        checkIfRandomAbort();
        double val = m_imExporter.readDouble(p_double);
        m_currentPos += Double.BYTES;
        return val;
    }

    @Override
    public int readCompactNumber(final int p_int) {
        checkIfRandomAbort();
        int val = m_imExporter.readCompactNumber(p_int);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(val);
        return val;
    }

    @Override
    public String readString(final String p_string) {
        checkIfRandomAbort();
        String val = m_imExporter.readString(p_string);
        m_currentPos += ObjectSizeUtil.sizeofString(val);
        return val;
    }

    @Override
    public int readBytes(final byte[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readBytes(p_array);
        m_currentPos += p_array.length * Byte.BYTES;
        return ret;
    }

    @Override
    public int readShorts(final short[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readShorts(p_array);
        m_currentPos += p_array.length * Short.BYTES;
        return ret;
    }

    @Override
    public int readChars(final char[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readChars(p_array);
        m_currentPos += p_array.length * Character.BYTES;
        return ret;
    }

    @Override
    public int readInts(final int[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readInts(p_array);
        m_currentPos += p_array.length * Integer.BYTES;
        return ret;
    }

    @Override
    public int readLongs(final long[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readLongs(p_array);
        m_currentPos += p_array.length * Long.BYTES;
        return ret;
    }

    @Override
    public int readFloats(final float[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readFloats(p_array);
        m_currentPos += p_array.length * Float.BYTES;
        return ret;
    }

    @Override
    public int readDoubles(final double[] p_array) {
        checkIfRandomAbort();
        int ret = m_imExporter.readDoubles(p_array);
        m_currentPos += p_array.length * Double.BYTES;
        return ret;
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readBytes(p_array, p_offset, p_length);
        m_currentPos += p_length * Byte.BYTES;
        return ret;
    }

    @Override
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readShorts(p_array, p_offset, p_length);
        m_currentPos += p_length * Short.BYTES;
        return ret;
    }

    @Override
    public int readChars(final char[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readChars(p_array, p_offset, p_length);
        m_currentPos += p_length * Character.BYTES;
        return ret;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readInts(p_array, p_offset, p_length);
        m_currentPos += p_length * Integer.BYTES;
        return ret;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readLongs(p_array, p_offset, p_length);
        m_currentPos += p_length * Long.BYTES;
        return ret;
    }

    @Override
    public int readFloats(final float[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readFloats(p_array, p_offset, p_length);
        m_currentPos += p_length * Float.BYTES;
        return ret;
    }

    @Override
    public int readDoubles(final double[] p_array, final int p_offset, final int p_length) {
        checkIfRandomAbort();
        int ret = m_imExporter.readDoubles(p_array, p_offset, p_length);
        m_currentPos += p_length * Double.BYTES;
        return ret;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        checkIfRandomAbort();
        byte[] ret = m_imExporter.readByteArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Byte.BYTES;
        return ret;
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        checkIfRandomAbort();
        short[] ret = m_imExporter.readShortArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Short.BYTES;
        return ret;
    }

    @Override
    public char[] readCharArray(final char[] p_array) {
        checkIfRandomAbort();
        char[] ret = m_imExporter.readCharArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Character.BYTES;
        return ret;
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        checkIfRandomAbort();
        int[] ret = m_imExporter.readIntArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Integer.BYTES;
        return ret;
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        checkIfRandomAbort();
        long[] ret = m_imExporter.readLongArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Long.BYTES;
        return ret;
    }

    @Override
    public float[] readFloatArray(final float[] p_array) {
        checkIfRandomAbort();
        float[] ret = m_imExporter.readFloatArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Long.BYTES;
        return ret;
    }

    @Override
    public double[] readDoubleArray(final double[] p_array) {
        checkIfRandomAbort();
        double[] ret = m_imExporter.readDoubleArray(p_array);
        m_currentPos += ObjectSizeUtil.sizeofCompactedNumber(ret.length) + ret.length * Long.BYTES;
        return ret;
    }

    /**
     * Check if we have to abort the serialization and throw an ArrayIndexOutOfBoundsException
     */
    private void checkIfRandomAbort() {
        if (m_randomAbortAtPos != -1) {
            if (m_currentPos >= m_randomAbortAtPos) {
                throw new ArrayIndexOutOfBoundsException("Random abort at " + m_randomAbortAtPos);
            }
        }
    }
}
