package wres.engine.statistics.metric.config;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.generated.MetricConfigName;

/**
 * Tests the {@link MetricConfigHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricConfigHelperTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();


    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testFromMetricName() throws MetricConfigException
    {
        //Check for mapping without exception
        for ( MetricConfigName nextConfig : MetricConfigName.values() )
        {
            if ( nextConfig != MetricConfigName.ALL_VALID )
            {
                assertTrue( "No mapping found for '" + nextConfig
                            + "'.",
                            Objects.nonNull( MetricConfigHelper.from( nextConfig ) ) );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( "Expected a null mapping for '" + MetricConfigName.ALL_VALID
                    + "'.",
                    Objects.isNull( MetricConfigHelper.from( MetricConfigName.ALL_VALID ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(MetricConfigName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testExceptionFromMetricNameWithNullInput() throws MetricConfigException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify input configuration with a non-null name to map" );
        MetricConfigHelper.from( (MetricConfigName) null );
    }

}
