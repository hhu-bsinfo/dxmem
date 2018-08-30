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
import de.hhu.bsinfo.dxmem.operations.CreateReserved;
import de.hhu.bsinfo.dxmem.operations.Dump;
import de.hhu.bsinfo.dxmem.operations.Exists;
import de.hhu.bsinfo.dxmem.operations.Get;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.Put;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxmem.operations.Remove;
import de.hhu.bsinfo.dxmem.operations.Reserve;
import de.hhu.bsinfo.dxmem.operations.Stats;

public class DXMem {
    private Context m_context;

    private Create m_create;
    private CreateReserved m_createReserved;
    private Get m_get;
    private Put m_put;
    private Remove m_remove;
    private Reserve m_reserve;
    private Exists m_exists;

    private Pinning m_pinning;
    private RawRead m_rawRead;
    private RawWrite m_rawWrite;

    private CIDStatus m_cidStatus;
    private Stats m_stats;

    private Recovery m_recovery;

    private Analyze m_analyze;
    private Dump m_dump;

    public DXMem(final String p_memdumpFile) {
        m_context = new Context(p_memdumpFile);

        initOperations();
    }

    public DXMem(final short p_nodeId, final long p_heapSize) {
        m_context = new Context(p_nodeId, p_heapSize);

        initOperations();
    }

    public void shutdown() {
        m_context.destroy();
        m_context = null;
    }

    public void reset() {
        short nodeId = m_context.getNodeId();
        long heapSize = m_context.getHeap().getStatus().getTotalSizeBytes();

        shutdown();
        m_context = new Context(nodeId, heapSize);
        initOperations();
    }

    public Create create() {
        return m_create;
    }

    public CreateReserved createReserved() {
        return m_createReserved;
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

    public Reserve reserve() {
        return m_reserve;
    }

    public Exists exists() {
        return m_exists;
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

    private void initOperations() {
        m_create = new Create(m_context);
        m_createReserved = new CreateReserved(m_context);
        m_get = new Get(m_context);
        m_put = new Put(m_context);
        m_remove = new Remove(m_context);
        m_reserve = new Reserve(m_context);
        m_exists = new Exists(m_context);

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
