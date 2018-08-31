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

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.MemoryDumper;

/**
 * Dump the heap to a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Dump {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Dump(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Dump the heap to a file
     *
     * @param p_file
     *         Path to file to dump to
     */
    public void dump(final String p_file) {
        m_context.getDefragmenter().acquireApplicationThreadLock();

        MemoryDumper dumper = new MemoryDumper(m_context.getHeap(), m_context.getCIDTable(), m_context.getLIDStore());
        dumper.dump(p_file);

        m_context.getDefragmenter().releaseApplicationThreadLock();
    }
}
