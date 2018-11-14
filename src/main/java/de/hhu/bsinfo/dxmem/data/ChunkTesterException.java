package de.hhu.bsinfo.dxmem.data;

/**
 * Exception thrown on ChunkTester errors
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.11.2018
 */
public class ChunkTesterException extends Exception {
    private final AbstractChunk m_newChunk;

    /**
     * Constructor
     *
     * @param p_msg Error message
     */
    ChunkTesterException(final String p_msg) {
        super(p_msg);

        m_newChunk = null;
    }

    /**
     * Constructor
     *
     * @param p_msg Error message
     * @param p_newChunk Faulty chunk involved
     */
    ChunkTesterException(final String p_msg, final AbstractChunk p_newChunk) {
        super(p_msg);

        m_newChunk = p_newChunk;
    }

    /**
     * Constructor
     *
     * @param p_e Exception to forward
     */
    ChunkTesterException(final Exception p_e) {
        super(p_e);

        m_newChunk = null;
    }

    /**
     * Get the chunk attached to the error for analysis. Can be null.
     *
     * @return Chunk attached to error or null if none attached.
     */
    public AbstractChunk getNewChunk() {
        return m_newChunk;
    }
}
