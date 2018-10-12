package de.hhu.bsinfo.dxmem;

import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmonitor.state.MemState;
import de.hhu.bsinfo.dxmonitor.state.StateUpdateException;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public final class DXMemTestUtils {
    private DXMemTestUtils() {

    }

    public static boolean sufficientMemoryForBenchmark(final long p_heapSizeBytes) {
        return sufficientMemoryForBenchmark(new StorageUnit(p_heapSizeBytes, StorageUnit.BYTE));
    }

    public static boolean sufficientMemoryForBenchmark(final StorageUnit p_heapSize) {
        MemState state = new MemState();

        try {
            state.update();
        } catch (StateUpdateException e) {
            throw new MemoryRuntimeException(e.getMessage());
        }

        StorageUnit freeMem = state.getFree();

        if (p_heapSize.getMBDouble() > freeMem.getMBDouble()) {
            return false;
        }

        // don't execute benchmark if there isn't at least another 1 GB of RAM available once heap is allocated
        return !(freeMem.getGBDouble() - p_heapSize.getGBDouble() < 1.0);
    }
}
