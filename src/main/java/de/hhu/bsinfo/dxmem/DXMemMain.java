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

import picocli.CommandLine;

import java.util.Locale;

import de.hhu.bsinfo.dxmem.cli.ToolRoot;

/**
 * DXMem benchmark, debugging and development tools.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.07.2018
 */
public final class DXMemMain {

    private DXMemMain() {

    }

    /**
     * Application entry point
     *
     * @param p_args
     *         Cmd args
     */
    public static void main(final String[] p_args) {
        Locale.setDefault(new Locale("en", "US"));
        CommandLine.run(new ToolRoot(), p_args);
        System.exit(0);
    }
}
