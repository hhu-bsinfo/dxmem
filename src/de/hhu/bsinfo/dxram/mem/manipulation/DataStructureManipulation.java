package de.hhu.bsinfo.dxram.mem.manipulation;

import de.hhu.bsinfo.dxram.data.DataStructure;

/**
 * A interface to manipulate chunk data in a locked area
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 25.02.18
 * @projectname dxram-memory
 */
@FunctionalInterface
public interface DataStructureManipulation <T extends DataStructure>{

    /**
     * Method to manipulate data
     *
     * @param oldData Data to be manipulated
     * @return Manipulated data
     */
    T getNewData(T oldData);
}
