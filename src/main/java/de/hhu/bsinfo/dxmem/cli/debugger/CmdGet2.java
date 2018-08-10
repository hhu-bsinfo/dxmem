package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import java.lang.reflect.InvocationTargetException;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterChunkId;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;

@CommandLine.Command(
        name = "get2",
        description = "Get the contents of an existing chunk"
)
public class CmdGet2 implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterChunkId.class,
            paramLabel = "cid",
            description = "CID of the chunk to get. nid/lid also allowed, e.g. 0x1234/0x10")
    private long m_cid;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "className",
            description = "Full name of a java class that implements AbstractChunk (with package path)")
    private String m_className;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        AbstractChunk chunk = newChunk(m_className);

        if (chunk == null) {
            return;
        }

        chunk.setID(m_cid);

        CliContext.getInstance().getMemory().get().get(chunk);

        System.out.printf("Get status (%X): %s\n", chunk.getID(), chunk.getState());
        System.out.printf("Chunk: %s", chunk);
    }

    private static AbstractChunk newChunk(final String p_className) {
        Class<?> clazz;

        try {
            clazz = Class.forName(p_className);
        } catch (final ClassNotFoundException ignored) {
            System.out.printf("Cannot find class with name %s\n", p_className);
            return null;
        }

        if (!AbstractChunk.class.isAssignableFrom(clazz)) {
            System.out.printf("Class %s is not implementing the DataStructure interface\n", p_className);
            return null;
        }

        AbstractChunk chunk;

        try {
            chunk = (AbstractChunk) clazz.getConstructor().newInstance();
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            System.out.printf("Creating instance of %s failed: %s\n", p_className, e.getMessage());
            return null;
        }

        return chunk;
    }
}
