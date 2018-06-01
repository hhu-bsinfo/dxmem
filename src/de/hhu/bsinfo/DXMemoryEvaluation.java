package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryEvaluation;
import de.hhu.bsinfo.dxram.mem.MemoryManager;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 19.03.18
 * @projectname dxram-memory
 */
public class DXMemoryEvaluation {
    public static void main(String[] argv) {

        if(argv.length != 9){
            argv = new String[]{"0", "1073741824", "master", "100", "16", "64", "2", "1000000", "3"};
        }

        int argc = 0;
        short nodeID = Short.parseShort(argv[argc++]);
        long heapSize = Long.parseLong(argv[argc++]);
        String branch = argv[argc++];
        int initChunks = Integer.parseInt(argv[argc++]);
        int initMin = Integer.parseInt(argv[argc++]);
        int initMax = Integer.parseInt(argv[argc++]);
        int threads = Integer.parseInt(argv[argc++]);
        long operations = Long.parseLong(argv[argc++]);
        int rounds = Integer.parseInt(argv[argc]);

        boolean[][] locks = {{true, true}, {true, false}, {false, true}};
        double[][] probabilities = {{0.0,0.0,0.0}, {0.0,0.0,0.1}, {0.0,0.0,0.5}, {0.05,0.05,0.1}, {0.1,0.1,0.4}};


        MemoryManager memory = new MemoryManager(nodeID, heapSize, (int)Math.pow(2,22));
        MemoryEvaluation evaluation = new MemoryEvaluation(memory,
                System.getProperty("user.home") + "/eval/" + branch,
                initChunks, initMin, initMax);
        evaluation.setThreads(threads);
        evaluation.setOperations(operations);
        evaluation.setRounds(rounds);

        for (double[] prob : probabilities) {
            if (prob[0] == 0.0 && prob[1] == 0.0){
                //evaluate pinning
                evaluation.accessSimulationPinning(prob[2]);

                //test no locks
                evaluation.setLocks(true, true, true, true, 0);
                evaluation.accessSimulation(prob[0], prob[1], prob[2], 16, 2048);
            }

            for(int waitHandle = 0; waitHandle < 3; waitHandle++) {
                //evaluate weak consistency
                evaluation.setLocks(true, true, true, false, waitHandle);
                evaluation.accessSimulation(prob[0], prob[1], prob[2], 16, 2048);

                //evaluate strong consistency
                for (boolean[] lock : locks) {
                    evaluation.setLocks(lock[0], lock[1], false, false, waitHandle);
                    evaluation.accessSimulation(prob[0], prob[1], prob[2], 16, 2048);
                }
            }
        }

        memory.shutdownMemory();
    }
}
