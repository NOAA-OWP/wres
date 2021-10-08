package wres.metrics;

import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import wres.metrics.config.MetricConfigHelper;

/**
 * Convenience class for achieving coverage of hidden factory constructors, in order to clean-up the reported test
 * coverage statistics. Add new factory classes as they are implemented. TODO: remove this class when test coverage can
 * be selectively suppressed for factory constructors.
 * 
 * @author James Brown
 */

public final class FactoryConstructionTest
{

    /**
     * Test coverage for factory constructors.
     * 
     * @throws SecurityException if reflection fails
     * @throws NoSuchMethodException if reflection fails
     * @throws InstantiationException if reflection fails
     * @throws IllegalAccessException if reflection fails
     * @throws InvocationTargetException if reflection fails
     */

    @Test
    public void testCoverage() throws NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            InvocationTargetException
    {
        assertNotNull( this.coverageSingleton( FunctionFactory.class ) );
        assertNotNull( this.coverageSingleton( MetricConfigHelper.class ) );
    }

    /**
     * Test the construction of a factory class.
     * 
     * @param <S> the generic type of the class
     * @param singletonClass the class to test
     * @throws SecurityException if reflection fails
     * @throws NoSuchMethodException if reflection fails
     * @throws InstantiationException if reflection fails
     * @throws IllegalAccessException if reflection fails
     * @throws InvocationTargetException if reflection fails
     */

    private <S> S coverageSingleton( final Class<S> singletonClass ) throws NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            InvocationTargetException
    {
        final Constructor<S> constructor = singletonClass.getDeclaredConstructor();
        constructor.setAccessible( true );
        return constructor.newInstance();
    }

}
