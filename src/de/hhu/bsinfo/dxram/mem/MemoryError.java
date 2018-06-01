package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import org.apache.logging.log4j.Logger;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 01.03.18
 * @projectname dxram-memory
 */
public class MemoryError {

    /**
     * Execute a memory dump (if enabled) on a memory error (corruption)
     * Note: MemoryRuntimeException is only thrown if assertions are enabled (disabled for performance)
     * Otherwise, some memory access errors result in segmentation faults, others aren't detected.
     *
     * @param p_e
     *         Exception thrown on memory error
     */
    static void handleMemDumpOnError(final SmallObjectHeap p_smallObjectHeap, final MemoryRuntimeException p_e,
                                     final String p_dumpFolder, final boolean p_acquireManageLock,
                                     final Logger LOGGER) {
        // #if LOGGER == ERROR
        LOGGER.fatal("Encountered memory error (most likely corruption)", p_e);
        // #endif /* LOGGER == ERROR */

        //->if (!getConfig().getMemDumpFolderOnError().isEmpty()) {
        if (!p_dumpFolder.isEmpty()) {//<<
            //->String folder = getConfig().getMemDumpFolderOnError();
            String folder = p_dumpFolder;//<<

            if (!folder.endsWith("/")) {
                folder += "/";
            }

            // create unique file name for each thread to avoid collisions
            String fileName = folder + "memdump-" + Thread.currentThread().getId() + '-' + System.currentTimeMillis() + ".soh";

            // #if LOGGER == ERROR
            LOGGER.fatal("Full memory dump to file: %s...", fileName);
            // #endif /* LOGGER == ERROR */

            // ugly: we entered this with a access lock, acquire the managed lock to ensure full blocking of the memory before dumping
            //if (p_acquireManageLock) {
            //    unlockAccess();
            //    lockManage();
            //}

            // #if LOGGER == ERROR
            LOGGER.fatal("Dumping...");
            // #endif /* LOGGER == ERROR */
            p_smallObjectHeap.dump(fileName);

            //if (p_acquireManageLock) {
            //    unlockManage();
            //    lockAccess();
            //}

            // #if LOGGER == ERROR
            LOGGER.fatal("Memory dump to file finished: %s", fileName);
            // #endif /* LOGGER == ERROR */
        } else {
            // #if LOGGER == ERROR
            LOGGER.fatal("Memory dump to file disabled");
            // #endif /* LOGGER == ERROR */
        }
    }
}
