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

package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Dump the heap to a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Dump extends AbstractOperation {
    private final String m_outputFile;

    /**
     * Constructor
     *
     * @param p_outputFile
     *         Path to output file to dump to
     */
    public Dump(final String p_outputFile) {
        super("dump", 1.0f, 1, false);

        m_outputFile = p_outputFile;
    }

    @Override
    protected ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        executeTimeStart();
        p_memory.dump().dump(m_outputFile);
        executeTimeEnd();

        return ChunkState.OK;
    }
}
