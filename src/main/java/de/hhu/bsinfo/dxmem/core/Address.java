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

package de.hhu.bsinfo.dxmem.core;

/**
 * Utility class for handling native addresses, pointers and constants used for the heap and CID table
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public final class Address {
    public static final long INVALID = 0;
    public static final int POINTER_SIZE = 6;
    public static final long WIDTH_BITS = 43;

    /**
     * Private constructor, utility class
     */
    private Address() {

    }

    /**
     * Convert an address to a hex string
     *
     * @param p_address
     *         Address to convert
     * @return Hex string of address
     */
    public static String toHexString(final long p_address) {
        return String.format("0x%X", p_address);
    }
}
