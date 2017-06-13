package wres.engine.statistics.metric;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import wres.engine.statistics.metric.inputs.MetricInputFactory;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;

/**
 * Convenience class for achieving coverage of hidden factory constructors, in order to clean-up the reported test
 * coverage statistics. Add new factory classes as they are implemented. TODO: remove this class when test coverage can
 * be selectively suppressed for factory constructors.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
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
    public void testCoverage() throws SecurityException,
                               NoSuchMethodException,
                               InstantiationException,
                               IllegalAccessException,
                               InvocationTargetException
    {
        coverageSingleton(MetricInputFactory.class);
        coverageSingleton(MetricOutputFactory.class);
        coverageSingleton(MetricConstants.class);
        coverageSingleton(MetricFactory.class);
        coverageSingleton(FunctionFactory.class);
    }

    /**
     * Test the construction of a factory class.
     * 
     * @param singletonClass the class to test
     * @throws SecurityException if reflection fails
     * @throws NoSuchMethodException if reflection fails
     * @throws InstantiationException if reflection fails
     * @throws IllegalAccessException if reflection fails
     * @throws InvocationTargetException if reflection fails
     */

    private <S> void coverageSingleton(final Class<S> singletonClass) throws SecurityException,
                                                                      NoSuchMethodException,
                                                                      InstantiationException,
                                                                      IllegalAccessException,
                                                                      InvocationTargetException
    {
        final Constructor<S> constructor = singletonClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        constructor.newInstance();
    }

}
