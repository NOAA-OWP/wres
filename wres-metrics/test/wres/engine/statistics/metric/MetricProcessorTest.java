package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricOutputForProjectByTimeAndThreshold;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessor}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorTest
{

    /**
     * Tests the {@link MetricProcessor#willStoreMetricOutput()}.
     */

    @Test
    public void test1WillStoreMetricOutput()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test4AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> trueProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.values() );
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> falseProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config );
            //Check for storage
            assertTrue( "Expected a metric processor that stores metric outputs.",
                        trueProcessor.willStoreMetricOutput() == true );
            assertTrue( "Expected a metric processor that does not store metric outputs.",
                        falseProcessor.willStoreMetricOutput() == false );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests all methods related to whether metrics exist in a {@link MetricProcessor}, namely:
     * 
     * <ol>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup, MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasThresholdMetrics()}</li>
     * </ol>
     */

    @Test
    public void test2HasMetrics()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test4AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.values() );
            //Check for existence of metrics
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "' and '"
                        + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ) == true );
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "'.",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED ) == true );
            assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricOutputGroup.SCALAR ) == true );
            assertTrue( "Expected threshold metrics.", processor.hasThresholdMetrics() == true );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }


}
