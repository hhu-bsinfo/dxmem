package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryManager;
import de.hhu.bsinfo.dxram.mem.MemoryTesting;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 18.03.18
 * @projectname dxram-memory
 */
public class DXMemoryTesting {
    public static void main(String[] args) throws InterruptedException {
        MemoryManager memory = new MemoryManager((short)0, (long)Math.pow(2,30), (int)Math.pow(2,22));
        MemoryTesting testing = new MemoryTesting(memory);

        testing.initHeap(50, 1);
        testing.lockTestFunctionality(100000000, 2, 0.5);
        testing.analyze();

        testing.createDeleteTest(10000, 2, 0.5, 16, 2048);
        testing.analyze();

        testing.pinningFunctional(1000, 50);
        testing.analyze();

        testing.createAndWriteStringObjects(100000000, 2,
                new String[]{"test", "längerer test", " noch längerer test", "kurz", "kur"},
                true);

        testing.analyze();


    }
}
