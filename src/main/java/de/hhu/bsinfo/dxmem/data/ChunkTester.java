package de.hhu.bsinfo.dxmem.data;

/**
 * Utility class that allows you to test the import/export methods of your chunk implementation. This runs a full
 * export and re-import including aborting serialization randomly because this can happen when transferring chunks
 * using DXNet.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.11.2018
 */
public class ChunkTester {
    private static final int DEFAULT_NUM_ABORTS = 100;

    /**
     * Utility class, private constructor
     */
    private ChunkTester() {

    }

    /**
     * Cut down version of the method with the same name but more parameters. Number of serialization aborts defaults
     * to 100.
     *
     * @param p_instantiator Implementation of the function interface to provide the instances to test
     * @param p_numIterations Number of iterations to run
     * @throws ChunkTesterException Thrown on any error with a (hopefully) detailed explanation of what happened
     */
    public static void testChunkInstance(final ChunkInstantiator p_instantiator, final int p_numIterations)
            throws ChunkTesterException {
        testChunkInstance(p_instantiator, p_numIterations, DEFAULT_NUM_ABORTS);
    }

    /**
     * Test a chunk instance. The chunk instance is provided via a function interface because the test method has
     * to create multiple instances for testing and using reflection is not possible because there are no rules to
     * have specific constructors available.
     *
     * Your instances returned by the functional interface must be equal and your class extending AbstractChunk must
     * implement the equals method to allow comparing the chunk instances. Ensure that your equal method is implemented
     * properly and compares all states (do not include the CID).
     *
     * This method runs multiple export iterations with a specified number of random serialization aborts to test if
     * your methods are implemented correctly (because aborting serialization is a thing for DXNet). Afterwards, the
     * serialized data is re-imported, again multiple times with mid-serialization aborts. At the end, the re-imported
     * object must match the original/initial object.
     *
     * @param p_instantiator Implementation of the function interface to provide the instances to test
     * @param p_numIterations Number of iterations to run
     * @param p_numSerializationAborts Number of random serialization aborts to inject. Make sure to test against a
     *                                 high number (e.g. 100 should be fine) to ensure different cases are triggered
     * @throws ChunkTesterException Thrown on any error with a (hopefully) detailed explanation of what happened
     */
    public static void testChunkInstance(final ChunkInstantiator p_instantiator, final int p_numIterations,
            final int p_numSerializationAborts) throws ChunkTesterException {
        AbstractChunk originalChunk = p_instantiator.newInstance();
        AbstractChunk workingChunk = p_instantiator.newInstance();
        AbstractChunk workingChunk2 = p_instantiator.newInstance();

        // check if chunk implements equals method and abort because we can't compare them otherwise
        try {
            if (!workingChunk.getClass().getMethod("equals", Object.class).getDeclaringClass()
                    .equals(originalChunk.getClass())) {
                throw new ChunkTesterException("Class " + workingChunk.getClass().getName() + " of chunk instance does not" +
                        " implement equals method which is required to check if exporting and re-importing worked");
            }
        } catch (final NoSuchMethodException e) {
            throw new ChunkTesterException(e);
        }

        int initialSize = workingChunk.sizeofObject();

        for (int i = 0; i < p_numIterations; i++) {
            // first run a bunch of iterations with serialization aborting randomly
            ChunkTesterImExporter imExporter = new ChunkTesterImExporter(workingChunk, true);

            // export phase with multiple aborts
            for (int j = 0; j < p_numSerializationAborts; j++) {
                try {
                    imExporter.exportObject(workingChunk);
                } catch (final ArrayIndexOutOfBoundsException e) {
                    // ignore
                }
            }

            // now run one clean iteration exporting everything
            imExporter = new ChunkTesterImExporter(workingChunk, false);

            imExporter.exportObject(workingChunk);

            if (!originalChunk.equals(workingChunk)) {
                throw new ChunkTesterException("Chunk was altered on export. This should not happen. Ensure that " +
                        "exportObject does not alter any state of your chunk", workingChunk);
            }

            int curSize = workingChunk.sizeofObject();

            if (initialSize != workingChunk.sizeofObject()) {
                throw new ChunkTesterException("Size of chunk after exporting phase changed (" + curSize + ") " +
                        "and does not match the initial size (" + initialSize + " anymore. It's likely that something" +
                        "in your sizeofObject or exportObject method is faulty.");
            }

            ChunkTesterImExporter newImExporter;

            // next import phase, try to import the chunk with multiple aborts first
            for (int j = 0; j < p_numSerializationAborts; j++) {
                newImExporter = new ChunkTesterImExporter(imExporter.getBuffer(), true);

                try {
                    newImExporter.importObject(workingChunk2);
                } catch (final ArrayIndexOutOfBoundsException e) {
                    // ignore
                }
            }

            // now run one clean iteration importing everything
            newImExporter = new ChunkTesterImExporter(imExporter.getBuffer(), false);
            newImExporter.importObject(workingChunk2);

            curSize = workingChunk2.sizeofObject();

            if (curSize != initialSize) {
                throw new ChunkTesterException("Size of chunk after re-importing phase changed (" + curSize + ") " +
                        "and does not match the initial size (" + initialSize + " anymore. It's likely that something" +
                        "in your sizeofObject, exportObject or ImportObject method is faulty.", workingChunk2);
            }

            if (!originalChunk.equals(workingChunk2)) {
                throw new ChunkTesterException("Exported and re-imported chunk does not match the initial chunk. One" +
                        "of the following methods is likely faulty: exportObject or importObject", originalChunk);
            }
         }
    }

    @FunctionalInterface
    public interface ChunkInstantiator {
        /**
         * Create a new chunk instance to be used for testing. Ensure that you always return newly allocated and
         * state-wise identical instances.
         *
         * @return New chunk instance
         */
        AbstractChunk newInstance();
    }
}
