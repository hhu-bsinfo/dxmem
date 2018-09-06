package de.hhu.bsinfo.dxmem.benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark defined by name and a list of phases
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.09.2018
 */
public class Benchmark {
    private final String m_name;
    private final List<BenchmarkPhase> m_phases;

    /**
     * Constructor
     *
     * @param p_name
     *         Name of the benchmark
     */
    public Benchmark(final String p_name) {
        m_name = p_name;
        m_phases = new ArrayList<>();
    }

    /**
     * Get the name of the benchmark
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the phases of the benchmark
     *
     * @return List of phases to execute
     */
    public List<BenchmarkPhase> getPhases() {
        return m_phases;
    }

    /**
     * Add a phase to the benchmark
     *
     * @param p_phase
     *         Phase to add
     */
    public void addPhase(final BenchmarkPhase p_phase) {
        m_phases.add(p_phase);
    }
}
