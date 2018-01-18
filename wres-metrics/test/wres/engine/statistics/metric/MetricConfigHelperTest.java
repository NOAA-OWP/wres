package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Objects;

import org.junit.Test;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ThresholdOperator;
import wres.datamodel.Threshold.Operator;

/**
 * Tests the {@link ConfigMapper}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricConfigHelperTest
{

    /**
     * Tests the {@link ConfigMapper#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigurationException if a mapping could not be created
     */

    @Test
    public void test1From() throws MetricConfigurationException
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
     * Tests the {@link ConfigMapper#from(ThresholdOperator)}.
     * @throws MetricConfigurationException if a mapping could not be created
     */

    @Test
    public void test2From() throws MetricConfigurationException
    {
        assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN
                    + "'.",
                    MetricConfigHelper.from( ThresholdOperator.GREATER_THAN ) == Operator.GREATER );
        assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN
                    + "'.",
                    MetricConfigHelper.from( ThresholdOperator.LESS_THAN ) == Operator.LESS );
        assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN_OR_EQUAL_TO
                    + "'.",
                    MetricConfigHelper.from( ThresholdOperator.GREATER_THAN_OR_EQUAL_TO ) == Operator.GREATER_EQUAL );
        assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN_OR_EQUAL_TO
                    + "'.",
                    MetricConfigHelper.from( ThresholdOperator.LESS_THAN_OR_EQUAL_TO ) == Operator.LESS_EQUAL );
    }
    
    /**
     * Tests the {@link ConfigMapper} for checked exceptions.
     */
    
    @Test
    public void test3Exceptions()
    {
        //Test the exceptions
        try
        {
            MetricConfigHelper.from( (MetricConfigName) null );
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final MetricConfigurationException e)
        {
        }

        //Test exception cases
        try
        {
            MetricConfigHelper.from( (ThresholdOperator) null );
            fail( "Expected a checked exception on null input." );
        }
        catch ( final MetricConfigurationException e )
        {
        }
    }

}
