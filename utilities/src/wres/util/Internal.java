package wres.util;

import java.lang.annotation.*;

/**
 * This class should only ever be used in the project that defined it
 */
@Target({ElementType.TYPE, ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.SOURCE)
@Inherited
@Documented public @interface Internal
{
    /**
     * @return The name of the parent package that should have exclusive rights to use this class
     */
    String exclusivePackage();
}

