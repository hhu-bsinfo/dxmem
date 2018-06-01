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

import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.operations.Analyze;
import de.hhu.bsinfo.dxmem.operations.CIDStatus;
import de.hhu.bsinfo.dxmem.operations.Create;
import de.hhu.bsinfo.dxmem.operations.CreateMulti;
import de.hhu.bsinfo.dxmem.operations.CreateReserved;
import de.hhu.bsinfo.dxmem.operations.CreateReservedMulti;
import de.hhu.bsinfo.dxmem.operations.Dump;
import de.hhu.bsinfo.dxmem.operations.Get;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.Put;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxmem.operations.Remove;
import de.hhu.bsinfo.dxmem.operations.Stats;

public class DXMemory {
    private final short m_nodeId;
    private final long m_heapSize;

    private Context m_context;

    private Create m_create;
    private CreateReserved m_createReserved;
    private CreateMulti m_createMulti;
    private CreateReservedMulti m_createReservedMulti;
    private Get m_get;
    private Put m_put;
    private Remove m_remove;

    private Pinning m_pinning;
    private RawRead m_rawRead;
    private RawWrite m_rawWrite;

    private CIDStatus m_cidStatus;
    private Stats m_stats;

    private Recovery m_recovery;

    private Analyze m_analyze;
    private Dump m_dump;

    public DXMemory(final short p_nodeId, final long p_heapSize) {
        m_nodeId = p_nodeId;
        m_heapSize = p_heapSize;

        init(m_nodeId, m_heapSize);
    }

    public void shutdown() {
        m_context.destroy();
        m_context = null;
    }

    public void reset() {
        shutdown();
        init(m_nodeId, m_heapSize);
    }

    public Create create() {
        return m_create;
    }

    public CreateReserved createReserved() {
        return m_createReserved;
    }

    public CreateMulti createMulti() {
        return m_createMulti;
    }

    public CreateReservedMulti createReservedMulti() {
        return m_createReservedMulti;
    }

    public Get get() {
        return m_get;
    }

    public Put put() {
        return m_put;
    }

    public Remove remove() {
        return m_remove;
    }

    public Pinning pinning() {
        return m_pinning;
    }

    public RawRead rawRead() {
        return m_rawRead;
    }

    public RawWrite rawWrite() {
        return m_rawWrite;
    }

    public CIDStatus cidStatus() {
        return m_cidStatus;
    }

    public Stats stats() {
        return m_stats;
    }

    public Recovery recovery() {
        return m_recovery;
    }

    public Analyze analyze() {
        return m_analyze;
    }

    public Dump dump() {
        return m_dump;
    }

    private void init(final short p_nodeId, final long p_heapSize) {
        m_context = new Context(p_nodeId, p_heapSize);

        m_create = new Create(m_context);
        m_createReserved = new CreateReserved(m_context);
        m_createMulti = new CreateMulti(m_context);
        m_createReservedMulti = new CreateReservedMulti(m_context);
        m_get = new Get(m_context);
        m_put = new Put(m_context);
        m_remove = new Remove(m_context);

        m_pinning = new Pinning(m_context);
        m_rawRead = new RawRead(m_context);
        m_rawWrite = new RawWrite(m_context);

        m_cidStatus = new CIDStatus(m_context);
        m_stats = new Stats(m_context);

        m_recovery = new Recovery(m_context);

        m_analyze = new Analyze(m_context);
        m_dump = new Dump(m_context);
    }
}
