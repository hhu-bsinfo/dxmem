package de.hhu.bsinfo.dxmem.benchmark;

public class TestBenchmarkRunner extends AbstractLocalBenchmarkRunner {
    protected TestBenchmarkRunner(final BenchmarkContext p_context) {
        super(p_context);
    }

    @Override
    public boolean init() {
        return true;
    }
}
