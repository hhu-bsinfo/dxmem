package de.hhu.bsinfo.dxmem.operations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Indicates that the class or the package presuppose, that they have to use in an context, where the memory is pinned.
 * So you can use the target while you have pinned the memory but for other uses it is not safe that it will work.
 *
 * @author Lars Mehnert
 * @see de.hhu.bsinfo.dxmem.operations.Pinning.PinnedMemory
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value = {TYPE})
public @interface PinnedMemory {
}
