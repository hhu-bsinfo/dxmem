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

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.operations.Analyze;
import de.hhu.bsinfo.dxmem.operations.CIDStatus;
import de.hhu.bsinfo.dxmem.operations.Create;
import de.hhu.bsinfo.dxmem.operations.CreateReserved;
import de.hhu.bsinfo.dxmem.operations.Dump;
import de.hhu.bsinfo.dxmem.operations.Exists;
import de.hhu.bsinfo.dxmem.operations.Get;
import de.hhu.bsinfo.dxmem.operations.Lock;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.Put;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxmem.operations.Remove;
import de.hhu.bsinfo.dxmem.operations.Reserve;
import de.hhu.bsinfo.dxmem.operations.Resize;
import de.hhu.bsinfo.dxmem.operations.Size;
import de.hhu.bsinfo.dxmem.operations.Stats;
import de.hhu.bsinfo.dxmonitor.state.MemState;
import de.hhu.bsinfo.dxmonitor.state.StateUpdateException;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * DXMem "main" class. Access to all operations offered by the the memory management
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class DXMem {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXMem.class.getSimpleName());

    private Context m_context;

    private Create m_create;
    private CreateReserved m_createReserved;
    private Get m_get;
    private Put m_put;
    private Remove m_remove;
    private Reserve m_reserve;
    private Exists m_exists;
    private Size m_size;
    private Resize m_resize;
    private Lock m_lock;

    private Pinning m_pinning;
    private RawRead m_rawRead;
    private RawWrite m_rawWrite;

    private CIDStatus m_cidStatus;
    private Stats m_stats;

    private Recovery m_recovery;

    private Analyze m_analyze;
    private Dump m_dump;

    /**
     * Constructor
     * Load a memory dump from a file and initialize DXMem with it.
     *
     * @param p_memdumpFile
     *         Path to memory dump file
     */
    public DXMem(final String p_memdumpFile) {
        this(p_memdumpFile, false);
    }

    /**
     * Constructor
     * Load a memory dump from a file and initialize DXMem with it.
     *
     * @param p_memdumpFile
     *         Path to memory dump file
     * @param p_disableChunkLock
     *         Disable the chunk lock mechanism which increases performance but blocks the remove
     *         and resize operations. All lock operation arguments provided on operation calls are
     *         ignored. DXMem cannot guarantee application data consistency on parallel writes to
     *         the same chunk. Useful for read only applications or if the application handles
     *         synchronization when writing to chunks.
     */
    public DXMem(final String p_memdumpFile, final boolean p_disableChunkLock) {
        checkSufficientMemory(new StorageUnit(new File(p_memdumpFile).length(), StorageUnit.BYTE));

        if (p_disableChunkLock) {
            LOGGER.warn("Chunk locks are disabled. Remove and resize operations cannot be used and throw errors");
        }

        m_context = new Context(p_memdumpFile, p_disableChunkLock);

        initOperations();
    }

    /**
     * Constructor
     * Create a new empty heap and initialize DXMem.
     *
     * @param p_nodeId
     *         Node id of current instance
     * @param p_heapSize
     *         Size of heap to create (in bytes)
     */
    public DXMem(final short p_nodeId, final long p_heapSize) {
        this(p_nodeId, p_heapSize, false);
    }

    /**
     * Constructor
     * Create a new empty heap and initialize DXMem.
     *
     * @param p_nodeId
     *         Node id of current instance
     * @param p_heapSize
     *         Size of heap to create (in bytes)
     * @param p_disableChunkLock
     *         Disable the chunk lock mechanism which increases performance but blocks the remove
     *         and resize operations. All lock operation arguments provided on operation calls are
     *         ignored. DXMem cannot guarantee application data consistency on parallel writes to
     *         the same chunk. Useful for read only applications or if the application handles
     *         synchronization when writing to chunks.
     */
    public DXMem(final short p_nodeId, final long p_heapSize, final boolean p_disableChunkLock) {
        checkSufficientMemory(new StorageUnit(p_heapSize, StorageUnit.BYTE));

        if (p_disableChunkLock) {
            LOGGER.warn("Chunk locks are disabled. Remove and resize operations cannot be used and throw errors");
        }

        m_context = new Context(p_nodeId, p_heapSize, p_disableChunkLock);

        initOperations();
    }

    /**
     * Shutdown and cleanup
     */
    public void shutdown() {
        m_context.destroy();
        m_context = null;
    }

    /**
     * Reset DXMem (shutdown + init)
     */
    public void reset() {
        short nodeId = m_context.getNodeId();
        long heapSize = m_context.getHeap().getStatus().getTotalSizeBytes();
        boolean disableChunkLock = m_context.isChunkLockDisabled();

        shutdown();
        m_context = new Context(nodeId, heapSize, disableChunkLock);
        initOperations();
    }

    /**
     * Get the create operation
     *
     * @return Operation
     */
    public Create create() {
        return m_create;
    }

    /**
     * Get the createReserved operation
     *
     * @return Operation
     */
    public CreateReserved createReserved() {
        return m_createReserved;
    }

    /**
     * Get the get operation
     *
     * @return Operation
     */
    public Get get() {
        return m_get;
    }

    /**
     * Get the put operation
     *
     * @return Operation
     */
    public Put put() {
        return m_put;
    }

    /**
     * Get the remove operation
     *
     * @return Operation
     */
    public Remove remove() {
        return m_remove;
    }

    /**
     * Get the reserve operation
     *
     * @return Operation
     */
    public Reserve reserve() {
        return m_reserve;
    }

    /**
     * Get the exists operation
     *
     * @return Operation
     */
    public Exists exists() {
        return m_exists;
    }

    /**
     * Get the size operation
     *
     * @return Operation
     */
    public Size size() {
        return m_size;
    }

    /**
     * Get the resize operation
     *
     * @return Operation
     */
    public Resize resize() {
        return m_resize;
    }

    /**
     * Get the lock operation
     *
     * @return Operation
     */
    public Lock lock() {
        return m_lock;
    }

    /**
     * Get the pinning operation
     *
     * @return Operation
     */
    public Pinning pinning() {
        return m_pinning;
    }

    /**
     * Get the rawRead operation
     *
     * @return Operation
     */
    public RawRead rawRead() {
        return m_rawRead;
    }

    /**
     * Get the rawWrite operation
     *
     * @return Operation
     */
    public RawWrite rawWrite() {
        return m_rawWrite;
    }

    /**
     * Get the cidStatus operation
     *
     * @return Operation
     */
    public CIDStatus cidStatus() {
        return m_cidStatus;
    }

    /**
     * Get the stats operation
     *
     * @return Operation
     */
    public Stats stats() {
        return m_stats;
    }

    /**
     * Get the recovery operation
     *
     * @return Operation
     */
    public Recovery recovery() {
        return m_recovery;
    }

    /**
     * Get the analyze operation
     *
     * @return Operation
     */
    public Analyze analyze() {
        return m_analyze;
    }

    /**
     * Get the dump operation
     *
     * @return Operation
     */
    public Dump dump() {
        return m_dump;
    }

    /**
     * Initialize all operations
     */
    private void initOperations() {
        m_create = new Create(m_context);
        m_createReserved = new CreateReserved(m_context);
        m_get = new Get(m_context);
        m_put = new Put(m_context);
        m_remove = new Remove(m_context);
        m_reserve = new Reserve(m_context);
        m_exists = new Exists(m_context);
        m_size = new Size(m_context);
        m_resize = new Resize(m_context);
        m_lock = new Lock(m_context);

        m_pinning = new Pinning(m_context);
        m_rawRead = new RawRead(m_context);
        m_rawWrite = new RawWrite(m_context);

        m_cidStatus = new CIDStatus(m_context);
        m_stats = new Stats(m_context);

        m_recovery = new Recovery(m_context);

        m_analyze = new Analyze(m_context);
        m_dump = new Dump(m_context);
    }

    private void checkSufficientMemory(final StorageUnit p_heapSize) {
        MemState state = new MemState();

        try {
            state.update();
        } catch (StateUpdateException e) {
            throw new MemoryRuntimeException(e.getMessage());
        }

        StorageUnit freeMem = state.getFree();

        if (p_heapSize.getMBDouble() > freeMem.getMBDouble()) {
            throw new MemoryRuntimeException(
                    "Cannot create heap insufficient free RAM: " + p_heapSize + " > " + freeMem);
        }

        if (freeMem.getGBDouble() - p_heapSize.getGBDouble() < 1.0) {
            LOGGER.warn("Less than 1 GB of free RAM available after allocating heap. This might lead to performance " +
                    "issues or even out of memory. Consider creating a smaller heap.");
        }
    }
}
