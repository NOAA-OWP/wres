package wres.engine.statistics.metric.config;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Objects;

import org.junit.Test;

import wres.config.MetricConfigException;
import wres.config.generated.MetricConfigName;

/**
 * Tests the {@link MetricConfigHelper}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricConfigHelperTest
{

    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void test1From() throws MetricConfigException
    {
        //Check for mapping without exception
        for ( MetricConfigName nextConfig : MetricConfigName.values() )
        {
            MetricConfigHelper.from( nextConfig );
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( "Expected a null mapping for '" + MetricConfigName.ALL_VALID
                    + "'.",
                    Objects.isNull( MetricConfigHelper.from( MetricConfigName.ALL_VALID ) ) );
    }
    
    /**
     * Tests the {@link MetricConfigHelper} for checked exceptions.
     * @throws MetricConfigException if an unexpected exception is encountered
     */
    
    @Test
    public void test3Exceptions() throws MetricConfigException
    {
        //Test the exceptions
        try
        {
            MetricConfigHelper.from( (MetricConfigName) null );
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final NullPointerException e)
        {
        }
    }

}
