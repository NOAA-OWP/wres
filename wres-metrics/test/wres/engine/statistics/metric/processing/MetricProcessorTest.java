package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.config.MetricConfigurationException;
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
     * Tests the {@link MetricProcessor#willCacheMetricOutput()}.
     * 
     * @throws MetricConfigurationException if the configuration is incorrect
     * @throws IOException if the input data could not be read
     */

    @Test
    public void test1WillStoreMetricOutput() throws IOException, MetricConfigurationException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorTest/test1AllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> trueProcessor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> falseProcessor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config );
        //Check for storage
        assertTrue( "Expected a metric processor that stores metric outputs.",
                    trueProcessor.willCacheMetricOutput() );
        assertFalse( "Expected a metric processor that does not store metric outputs.",
                     falseProcessor.willCacheMetricOutput() );
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
     * 
     * @throws MetricConfigurationException if the configuration is incorrect
     * @throws IOException if the input data could not be read
     */

    @Test
    public void test2HasMetrics() throws IOException, MetricConfigurationException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorTest/test1AllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
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

    /**
     * Tests that non-score metrics are disallowed when configuring "all valid" metrics in combination with 
     * {@link PairConfig#getPoolingWindow()} that is not null. Uses the configuration in 
     * testinput/metricProcessorTest/test3SingleValued.xml and 
     * testinput/metricProcessorTest/test3Ensemble.xml.
     * 
     * @throws MetricConfigurationException if the configuration is incorrect
     * @throws IOException if the input data could not be read
     */

    @Test
    public void test3DisallowNonScores() throws IOException, MetricConfigurationException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/test3SingleValued.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
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

        //Ensemble case
        String configPathEnsemble = "testinput/metricProcessorTest/test3Ensemble.xml";
        ProjectConfig configEnsemble = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processorEnsemble =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeEnsemblePairs( configEnsemble,
                                                                    MetricOutputGroup.values() );
        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                    + "'.",
                    processorEnsemble.hasMetrics( MetricOutputGroup.SCALAR ) );
        //Check that no non-score metrics are defined
        for ( MetricOutputGroup next : MetricOutputGroup.values() )
        {
            if ( !next.equals( MetricOutputGroup.SCALAR ) && !next.equals( MetricOutputGroup.VECTOR ) )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processorEnsemble.hasMetrics( next ) );
            }
        }
    }

}
