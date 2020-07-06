package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MetricProcessorByTimeEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorByTimeEnsemblePairsTest
{
    /**
     * Test source.
     */

    private static final String TEST_SOURCE =
            "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.RIGHT_MEAN ) ) );
        // Check that average works        
        Pair<Double, Ensemble> pair = Pair.of( 1.0, Ensemble.of( 1.5, 2.0 ) );

        assertTrue( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                condition,
                                                                                                ThresholdDataType.RIGHT_MEAN ) )
                                                      .test( pair ) );
    }

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithoutThresholds.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                 null );
        StatisticsForProject results =
                processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
        DoubleScoreStatistic bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION ).get( 0 );
        DoubleScoreStatistic cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION )
                      .get( 0 );
        DoubleScoreStatistic rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                      .get( 0 );
        DoubleScoreStatistic mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR )
                      .get( 0 );
        DoubleScoreStatistic me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR ).get( 0 );
        DoubleScoreStatistic rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .get( 0 );
        DoubleScoreStatistic crps =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                      .get( 0 );

        //Test contents
        assertTrue( bias.getData().equals( -0.032093836077598345 ) );
        assertTrue( cod.getData().equals( 0.7873367083297588 ) );
        assertTrue( rho.getData().equals( 0.8873199582618204 ) );
        assertTrue( mae.getData().equals( 11.009512537315405 ) );
        assertTrue( me.getData().equals( -1.157869354367079 ) );
        assertTrue( rmse.getData().equals( 41.01563032408479 ) );
        assertTrue( Double.compare( crps.getData(), 9.076475676968208 ) == 0 );
    }

    @Test
    public void testApplyWithValueThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                 StatisticType.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Validate bias
        List<DoubleScoreStatistic> bias = Slicer.filter( processor.getCachedMetricOutput()
                                                                  .getDoubleScoreStatistics(),
                                                         MetricConstants.BIAS_FRACTION );
        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreStatistic> cod = Slicer.filter( processor.getCachedMetricOutput()
                                                                 .getDoubleScoreStatistics(),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION );

        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreStatistic> rho = Slicer.filter( processor.getCachedMetricOutput()
                                                                 .getDoubleScoreStatistics(),
                                                        MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreStatistic> mae = Slicer.filter( processor.getCachedMetricOutput()
                                                                 .getDoubleScoreStatistics(),
                                                        MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreStatistic> me = Slicer.filter( processor.getCachedMetricOutput()
                                                                .getDoubleScoreStatistics(),
                                                       MetricConstants.MEAN_ERROR );
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.2185439080739515 ) );
        //Validate rmse
        List<DoubleScoreStatistic> rmse = Slicer.filter( processor.getCachedMetricOutput()
                                                                  .getDoubleScoreStatistics(),
                                                         MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348335 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    @Test
    public void testApplyWithValueThresholdsAndCategoricalMeasures()
            throws MetricParameterException, IOException, InterruptedException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY_CLASSIFIER,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.5",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig,
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );

        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        // Obtain the results
        List<DoubleScoreStatistic> actual = processor.getCachedMetricOutput()
                                                     .getDoubleScoreStatistics();

        // Check for equality
        BiPredicate<Double, Double> testEqual = FunctionFactory.doubleEquals();

        assertTrue( testEqual.test( Slicer.filter( actual, MetricConstants.THREAT_SCORE ).get( 0 ).getData(),
                                    0.9160756501182034 ) );
        assertTrue( testEqual.test( Slicer.filter( actual, MetricConstants.PEIRCE_SKILL_SCORE )
                                          .get( 0 )
                                          .getData(),
                                    -0.0012886597938144284 ) );
    }

    @Test
    public void testApplyWithProbabilityThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithProbabilityThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Obtain the results
        List<DoubleScoreStatistic> results = processor.getCachedMetricOutput()
                                                      .getDoubleScoreStatistics();

        //Validate a selection of the outputs only

        //Validate bias
        List<DoubleScoreStatistic> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );
        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.05090288343061958 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreStatistic> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7540690263086123 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreStatistic> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.8683714794421868 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreStatistic> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.653785159500924 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreStatistic> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.5134287820490364 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.2185439080739515 ) );
        //Validate rmse
        List<DoubleScoreStatistic> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348335 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.191244125990046 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Expected non-null input to the metric processor." );
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( new ProjectConfig( null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null ),
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( null );
    }

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Cannot configure 'BRIER SCORE' without thresholds to define the events: "
                                 + "add one or more thresholds to the configuration." );

        MetricsConfig metrics =
                new MetricsConfig( null,
                                   Arrays.asList( new MetricConfig( null, null, MetricConfigName.BRIER_SCORE ) ),
                                   null );
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( new ProjectConfig( null,
                                                                                    null,
                                                                                    Arrays.asList( metrics ),
                                                                                    null,
                                                                                    null,
                                                                                    null ),
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
    }

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Unable to determine quantile threshold from probability threshold: no climatological "
                                 + "observations were available in the input" );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BRIER_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig,
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsThree() );
    }

    @Test
    public void testApplyThrowsExceptionWhenBaselineIsMissing()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Specify a non-null baseline from which to generate the 'CONTINUOUS RANKED "
                                 + "PROBABILITY SKILL SCORE'." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig,
                                                                 Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsThree() );
    }

    @Test
    public void testApplyThrowsExceptionForDichotomousMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "In order to configure dichotomous metrics for ensemble inputs, every metric group "
                                 + "that contains dichotomous metrics must also contain thresholds for classifying the forecast "
                                 + "probabilities into occurrences and non-occurrences." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig,
                                                         Collections.singleton( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testApplyThrowsExceptionForMulticategoryMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "In order to configure multicategory metrics for ensemble inputs, every metric "
                                 + "group that contains multicategory metrics must also contain thresholds for classifying the "
                                 + "forecast probabilities into occurrences and non-occurrences." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig,
                                                         Collections.singleton( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testApplyWithAllValidMetrics() throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testAllValid.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                 StatisticType.set() );
        //Check for the expected number of metrics
        //One fewer than total, as sample size appears in both ensemble and single-valued
        assertTrue( processor.metrics.size() == SampleDataGroup.ENSEMBLE.getMetrics().size()
                                                + SampleDataGroup.DISCRETE_PROBABILITY.getMetrics().size()
                                                + SampleDataGroup.SINGLE_VALUED.getMetrics().size()
                                                - 1 );
    }

    @Test
    public void testApplyWithValueThresholdsAndMissings()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                 StatisticType.set() );

        processor.apply( MetricTestDataFactory.getEnsemblePairsOneWithMissings() );

        //Obtain the results
        List<DoubleScoreStatistic> results = processor.getCachedMetricOutput()
                                                      .getDoubleScoreStatistics();
        //Validate bias
        List<DoubleScoreStatistic> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );

        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreStatistic> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreStatistic> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreStatistic> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreStatistic> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.2185439080739515 ) );
        //Validate rmse
        List<DoubleScoreStatistic> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348335 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    @Test
    public void testContingencyTable()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testContingencyTable.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config, StatisticType.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );

        // Expected result
        final TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         Duration.ofHours( 24 ) );

        SampleMetadata expectedSampleMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                               DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                     "SQIN",
                                                                                     "HEFS" ),
                                                               expectedWindow,
                                                               null );

        //Obtain the results
        List<DoubleScoreStatistic> results =
                Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                               meta -> meta.getMetricID().equals( MetricConstants.CONTINGENCY_TABLE )
                                       && meta.getSampleMetadata().getTimeWindow().equals( expectedWindow ) );

        // Exceeds 50.0 with occurrences > 0.05
        Map<MetricConstants, Double> expectedFirstElements = new HashMap<>();
        expectedFirstElements.put( MetricConstants.TRUE_POSITIVES, 40.0 );
        expectedFirstElements.put( MetricConstants.TRUE_NEGATIVES, 91.0 );
        expectedFirstElements.put( MetricConstants.FALSE_POSITIVES, 32.0 );
        expectedFirstElements.put( MetricConstants.FALSE_NEGATIVES, 2.0 );

        OneOrTwoThresholds first = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.05 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaFirst = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, first ),
                                                                    165,
                                                                    MeasurementUnit.of(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    null );

        DoubleScoreStatistic expectedFirst = DoubleScoreStatistic.of( expectedFirstElements, expectedMetaFirst );

        DoubleScoreStatistic actualFirst =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( first ) )
                      .get( 0 );

        assertEquals( expectedFirst, actualFirst );

        // Exceeds 50.0 with occurrences > 0.25
        Map<MetricConstants, Double> expectedSecondElements = new HashMap<>();
        expectedSecondElements.put( MetricConstants.TRUE_POSITIVES, 39.0 );
        expectedSecondElements.put( MetricConstants.TRUE_NEGATIVES, 106.0 );
        expectedSecondElements.put( MetricConstants.FALSE_POSITIVES, 17.0 );
        expectedSecondElements.put( MetricConstants.FALSE_NEGATIVES, 3.0 );

        OneOrTwoThresholds second = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                         Operator.GREATER,
                                                                         ThresholdDataType.LEFT ),
                                                           ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.25 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaSecond = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, second ),
                                                                     165,
                                                                     MeasurementUnit.of(),
                                                                     MetricConstants.CONTINGENCY_TABLE,
                                                                     null );

        DoubleScoreStatistic expectedSecond = DoubleScoreStatistic.of( expectedSecondElements, expectedMetaSecond );

        DoubleScoreStatistic actualSecond =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( second ) )
                      .get( 0 );

        assertEquals( expectedSecond, actualSecond );

        // Exceeds 50.0 with occurrences > 0.5
        Map<MetricConstants, Double> expectedThirdElements = new HashMap<>();
        expectedThirdElements.put( MetricConstants.TRUE_POSITIVES, 39.0 );
        expectedThirdElements.put( MetricConstants.TRUE_NEGATIVES, 108.0 );
        expectedThirdElements.put( MetricConstants.FALSE_POSITIVES, 15.0 );
        expectedThirdElements.put( MetricConstants.FALSE_NEGATIVES, 3.0 );

        OneOrTwoThresholds third = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaThird = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, third ),
                                                                    165,
                                                                    MeasurementUnit.of(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    null );

        DoubleScoreStatistic expectedThird = DoubleScoreStatistic.of( expectedThirdElements, expectedMetaThird );

        DoubleScoreStatistic actualThird =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( third ) )
                      .get( 0 );

        assertEquals( expectedThird, actualThird );

        // Exceeds 50.0 with occurrences > 0.75
        Map<MetricConstants, Double> expectedFourthElements = new HashMap<>();
        expectedFourthElements.put( MetricConstants.TRUE_POSITIVES, 37.0 );
        expectedFourthElements.put( MetricConstants.TRUE_NEGATIVES, 109.0 );
        expectedFourthElements.put( MetricConstants.FALSE_POSITIVES, 14.0 );
        expectedFourthElements.put( MetricConstants.FALSE_NEGATIVES, 5.0 );

        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                         Operator.GREATER,
                                                                         ThresholdDataType.LEFT ),
                                                           ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.75 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaFourth = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, fourth ),
                                                                     165,
                                                                     MeasurementUnit.of(),
                                                                     MetricConstants.CONTINGENCY_TABLE,
                                                                     null );

        DoubleScoreStatistic expectedFourth = DoubleScoreStatistic.of( expectedFourthElements, expectedMetaFourth );
        DoubleScoreStatistic actualFourth =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( fourth ) )
                      .get( 0 );

        assertEquals( expectedFourth, actualFourth );

        // Exceeds 50.0 with occurrences > 0.9
        Map<MetricConstants, Double> expectedFifthElements = new HashMap<>();
        expectedFifthElements.put( MetricConstants.TRUE_POSITIVES, 37.0 );
        expectedFifthElements.put( MetricConstants.TRUE_NEGATIVES, 112.0 );
        expectedFifthElements.put( MetricConstants.FALSE_POSITIVES, 11.0 );
        expectedFifthElements.put( MetricConstants.FALSE_NEGATIVES, 5.0 );

        OneOrTwoThresholds fifth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.9 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaFifth = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, fifth ),
                                                                    165,
                                                                    MeasurementUnit.of(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    null );

        DoubleScoreStatistic expectedFifth = DoubleScoreStatistic.of( expectedFifthElements, expectedMetaFifth );

        DoubleScoreStatistic actualFifth =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( fifth ) )
                      .get( 0 );

        assertEquals( expectedFifth, actualFifth );

        // Exceeds 50.0 with occurrences > 0.95
        Map<MetricConstants, Double> expectedSixthElements = new HashMap<>();
        expectedSixthElements.put( MetricConstants.TRUE_POSITIVES, 36.0 );
        expectedSixthElements.put( MetricConstants.TRUE_NEGATIVES, 113.0 );
        expectedSixthElements.put( MetricConstants.FALSE_POSITIVES, 10.0 );
        expectedSixthElements.put( MetricConstants.FALSE_NEGATIVES, 6.0 );

        OneOrTwoThresholds sixth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMetaSixth = StatisticMetadata.of( SampleMetadata.of( expectedSampleMeta, sixth ),
                                                                    165,
                                                                    MeasurementUnit.of(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    null );

        DoubleScoreStatistic expectedSixth = DoubleScoreStatistic.of( expectedSixthElements, expectedMetaSixth );

        DoubleScoreStatistic actualSixth =
                Slicer.filter( results, meta -> meta.getSampleMetadata().getThresholds().equals( sixth ) )
                      .get( 0 );

        assertEquals( expectedSixth, actualSixth );

    }

    @Test
    public void testApplyWithValueThresholdsAndNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config, StatisticType.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsFour() );

        //Obtain the results
        List<DoubleScoreStatistic> results = processor.getCachedMetricOutput().getDoubleScoreStatistics();

        //Validate the score outputs
        for ( DoubleScoreStatistic nextMetric : results )
        {
            if ( nextMetric.getMetadata().getMetricID() != MetricConstants.SAMPLE_SIZE )
            {
                assertTrue( nextMetric.getData().isNaN() );
            }
        }
    }

    @Test
    public void testThatSampleSizeIsConstructedForEnsembleInput() throws MetricParameterException
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.SAMPLE_SIZE ) );

        ProjectConfig mock =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mock, StatisticType.set() );

        Set<MetricConstants> actualSingleValuedScores =
                Set.of( processor.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedSingleValuedScores = Set.of( MetricConstants.MEAN_ERROR );

        assertEquals( expectedSingleValuedScores, actualSingleValuedScores );

        Set<MetricConstants> actualEnsembleScores =
                Set.of( processor.getMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedEnsembleScores =
                Set.of( MetricConstants.SAMPLE_SIZE,
                        MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );

        assertEquals( expectedEnsembleScores, actualEnsembleScores );

    }

    @Test
    public void testThatSampleSizeIsConstructedForEnsembleInputWhenProbailityScoreExists()
            throws MetricParameterException
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BRIER_SCORE ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.SAMPLE_SIZE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( wres.config.generated.ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              wres.config.generated.ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mock =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mock, StatisticType.set() );

        Set<MetricConstants> actualSingleValuedScores =
                Set.of( processor.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedSingleValuedScores = Set.of( MetricConstants.MEAN_ERROR );

        assertEquals( expectedSingleValuedScores, actualSingleValuedScores );

        Set<MetricConstants> actualEnsembleScores =
                Set.of( processor.getMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedEnsembleScores = Set.of( MetricConstants.SAMPLE_SIZE );

        assertEquals( expectedEnsembleScores, actualEnsembleScores );


        Set<MetricConstants> actualProbabilityScores =
                Set.of( processor.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedProbabilityScores = Set.of( MetricConstants.BRIER_SCORE );

        assertEquals( expectedProbabilityScores, actualProbabilityScores );

    }

}
