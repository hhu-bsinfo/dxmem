package de.hhu.bsinfo.dxmem.operations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Indicates that the parameters of the targets are not checked for having a better performance.
 * It means that u have to take look into javadoc for this target how u use should use it. Remember it while debugging.
 * <p>
 * In context of DXMem especially the Raw operations you can damage the memory structure. So use the target carefully
 *
 * @author Lars Mehnert
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value = {TYPE, METHOD, CONSTRUCTOR})
public @interface NoParamCheck {
}
