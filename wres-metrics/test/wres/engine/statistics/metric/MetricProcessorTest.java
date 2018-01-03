package wres.engine.statistics.metric;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
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
        String configPath = "testinput/metricProcessorTest/test1AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> trueProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                            MetricOutputGroup.values() );
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> falseProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config );
            //Check for storage
            assertTrue( "Expected a metric processor that stores metric outputs.",
                        trueProcessor.willStoreMetricOutput() );
            assertFalse( "Expected a metric processor that does not store metric outputs.",
                         falseProcessor.willStoreMetricOutput() );
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
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasThresholdMetrics()}</li>
     * </ol>
     */

    @Test
    public void test2HasMetrics()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorTest/test1AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                            MetricOutputGroup.values() );
            //Check for existence of metrics
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "' and '"
                        + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ) );
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "'.",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED ) );
            assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricOutputGroup.SCALAR ) );
            assertTrue( "Expected threshold metrics.", processor.hasThresholdMetrics() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests that non-score metrics are disallowed when configuring "all valid" metrics in combination with 
     * {@link PairConfig#getPoolingWindow()} that is not null. Uses the configuration in 
     * testinput/metricProcessorTest/test3SingleValued.xml and 
     * testinput/metricProcessorTest/test3Ensemble.xml.
     */

    @Test
    public void test3DisallowNonScores()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/test3SingleValued.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                            MetricOutputGroup.values() );
            //Check that score metrics are defined 
            assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                        + "'.",
                        processor.hasMetrics( MetricOutputGroup.SCALAR ) );
            //Check that no non-score metrics are defined
            for ( MetricOutputGroup next : MetricOutputGroup.values() )
            {
                if ( !next.equals( MetricOutputGroup.SCALAR ) && !next.equals( MetricOutputGroup.VECTOR ) )
                {
                    assertFalse( "Did not expect metrics for '" + next
                                 + "'.",
                                 processor.hasMetrics( next ) );
                }
            }
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPathSingleValued + "'." );
        }
        //Ensemble case
        String configPathEnsemble = "testinput/metricProcessorTest/test3Ensemble.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                            MetricOutputGroup.values() );
            //Check that score metrics are defined 
            assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                        + "'.",
                        processor.hasMetrics( MetricOutputGroup.SCALAR ) );
            //Check that no non-score metrics are defined
            for ( MetricOutputGroup next : MetricOutputGroup.values() )
            {
                if ( !next.equals( MetricOutputGroup.SCALAR ) && !next.equals( MetricOutputGroup.VECTOR ) )
                {
                    assertFalse( "Did not expect metrics for '" + next
                                 + "'.",
                                 processor.hasMetrics( next ) );
                }
            }
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPathSingleValued + "'." );
        }
    }

}
