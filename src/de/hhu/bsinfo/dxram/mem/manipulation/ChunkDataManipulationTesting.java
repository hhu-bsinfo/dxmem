package de.hhu.bsinfo.dxram.mem.manipulation;

/**
 * A interface to manipulate chunk data in a locked area.
 * This interface has been extended for testing purposes
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 25.02.18
 * @projectname dxram-memory
 */
@FunctionalInterface
public interface ChunkDataManipulationTesting {

    /**
     * Method to manipulate data
     *
     * @param oldData Data to be manipulated
     * @param selected index for cid array
     * @return Manipulated data
     */
    byte[] getNewData(final byte[] oldData, final int selected);
}
