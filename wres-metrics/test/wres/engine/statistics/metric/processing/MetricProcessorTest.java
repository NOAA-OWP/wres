package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigPlus;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdConstants.ThresholdDataType;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigurationException;

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
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigurationException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void test1WillStoreMetricOutput()
            throws IOException, MetricConfigurationException, MetricParameterException, MetricProcessorException
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
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigurationException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void test2HasMetrics()
            throws IOException, MetricConfigurationException, MetricParameterException, MetricProcessorException
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
                    + MetricOutputGroup.DOUBLE_SCORE
                    + ".",
                    processor.hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ) );
        assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                    + "'.",
                    processor.hasMetrics( MetricInputGroup.SINGLE_VALUED ) );
        assertTrue( "Expected metrics for '" + MetricOutputGroup.DOUBLE_SCORE
                    + ".",
                    processor.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );
        assertTrue( "Expected threshold metrics.", processor.hasThresholdMetrics() );
    }

    /**
     * Tests that non-score metrics are disallowed when configuring "all valid" metrics in combination with 
     * {@link PairConfig#getIssuedDatesPoolingWindow()} that is not null. Uses the configuration in 
     * testinput/metricProcessorTest/test3SingleValued.xml and 
     * testinput/metricProcessorTest/test3Ensemble.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigurationException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void test3DisallowNonScores()
            throws IOException, MetricConfigurationException, MetricParameterException, MetricProcessorException
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
        assertTrue( "Expected metrics for '" + MetricOutputGroup.DOUBLE_SCORE
                    + "'.",
                    processor.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );

        //Check that no non-score metrics are defined
        for ( MetricOutputGroup next : MetricOutputGroup.values() )
        {
            if ( next != MetricOutputGroup.DOUBLE_SCORE )
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
        assertTrue( "Expected metrics for '" + MetricOutputGroup.DOUBLE_SCORE
                    + "'.",
                    processorEnsemble.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );
        //Check that no non-score metrics are defined
        for ( MetricOutputGroup next : MetricOutputGroup.values() )
        {
            if ( next != MetricOutputGroup.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processorEnsemble.hasMetrics( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup, wres.datamodel.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/test4SingleValued.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigurationException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void test4DoNotComputeTheseMetricsForThisThreshold()
            throws IOException, MetricConfigurationException, MetricParameterException, MetricProcessorException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final MetadataFactory metFac = metIn.getMetadataFactory();

        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/test4SingleValued.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );

        ThresholdsByMetric thresholds =
                processor.getThresholdsByMetric().filterByGroup( MetricInputGroup.SINGLE_VALUED,
                                                                 MetricOutputGroup.DOUBLE_SCORE );

        Threshold firstTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.5 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + firstTest
                    + "'",
                    firstSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold secondTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.75 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList() ) ) );
        Threshold thirdTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.83 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold fourthTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.9 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + fourthTest
                    + "'",
                    fourthSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                    MetricConstants.ROOT_MEAN_SQUARE_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup, wres.datamodel.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/test5Ensemble.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigurationException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void test5DoNotComputeTheseMetricsForThisThreshold()
            throws IOException, MetricConfigurationException, MetricParameterException, MetricProcessorException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();

        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/test5Ensemble.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.values() );
        Threshold firstTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.1 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );

        ThresholdsByMetric thresholds = processor.getThresholdsByMetric();

        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );

        assertTrue( "Unexpected set of metrics to ignore for threshold '" + firstTest
                    + "'",
                    firstSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                   MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.BRIER_SCORE ) ) ) );

        Threshold secondTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.25 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                                    MetricConstants.BRIER_SCORE ) ) ) );

        Threshold thirdTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.5 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );

        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.BRIER_SKILL_SCORE,
                                                                   MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) ) ) );

        Threshold fourthTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.925 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + fourthTest
                    + "'",
                    fourthSet.equals( new HashSet<>( Arrays.asList() ) ) );
    }


}
