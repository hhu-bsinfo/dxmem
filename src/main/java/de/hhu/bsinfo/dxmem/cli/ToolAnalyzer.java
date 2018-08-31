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

package de.hhu.bsinfo.dxmem.cli;

import picocli.CommandLine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

import de.hhu.bsinfo.dxmem.core.Analyzer;
import de.hhu.bsinfo.dxmem.core.MemoryLoader;

@CommandLine.Command(
        name = "analyzer",
        description = "Analyze a memory dump to detect memory errors"
)
public class ToolAnalyzer implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            paramLabel = "inputFile",
            description = "Path to memory dump file")
    private String m_inputFile;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "printStatus",
            description = "Print the status of CIDTable and Heap (true/false)")
    private boolean m_printStatus;

    @CommandLine.Parameters(
            index = "2",
            paramLabel = "analyze",
            description = "Analyze the loaded memory dump and check for errors (true/false)")
    private boolean m_analyze;

    @CommandLine.Parameters(
            index = "3",
            paramLabel = "verbose",
            description = "Be verbose when analyzing the dump (true/false)")
    private boolean m_verbose;

    @Override
    public void run() {
        System.out.println("analyzer");

        MemoryLoader loader = new MemoryLoader();

        System.out.println("Loading memory dump from file " + m_inputFile + "...");

        try {
            loader.load(m_inputFile);
        } catch (Throwable e) {
            System.out.println("Loading failed: " + e.getMessage());
            System.exit(-1);
        }

        if (m_printStatus) {
            System.out.println("Status");
            System.out.println("========================== CIDTable ==========================");
            System.out.println(loader.getCIDTable());
            System.out.println("========================== Heap ==========================");
            System.out.println(loader.getHeap());
        }

        if (m_analyze) {
            // force trace to get detailed output
            if (m_verbose) {
                Configurator.setRootLevel(Level.TRACE);
            }

            System.out.println("Analyzing memory dump (this may take a while) ...");

            Analyzer analyzer = new Analyzer(loader.getHeap(), loader.getCIDTable());

            boolean ret = analyzer.analyze();

            // force flush before printing result
            LogManager.shutdown();

            if (!ret) {
                System.out.println("Analyzing memory dump not successful, errors detected (see logger output)");
            } else {
                System.out.println("Analyzing memory dump successful, no errors detected");
            }
        }
    }
}
