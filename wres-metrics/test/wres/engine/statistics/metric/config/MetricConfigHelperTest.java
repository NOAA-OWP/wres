package wres.engine.statistics.metric.config;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdConstants.ThresholdGroup;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.metadata.MetadataFactory;

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
     * Data factory.
     */

    DataFactory dataFac = DefaultDataFactory.getInstance();

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

    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.SummaryStatisticsName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testFromSummaryStatisticsName() throws MetricConfigException
    {
        //Check for mapping without exception
        for ( SummaryStatisticsName nextStat : SummaryStatisticsName.values() )
        {
            if ( nextStat != SummaryStatisticsName.ALL_VALID )
            {
                assertTrue( "No mapping found for '" + nextStat
                            + "'.",
                            Objects.nonNull( MetricConfigHelper.from( nextStat ) ) );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( "Expected a null mapping for '" + SummaryStatisticsName.ALL_VALID
                    + "'.",
                    Objects.isNull( MetricConfigHelper.from( SummaryStatisticsName.ALL_VALID ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(SummaryStatisticsName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testExceptionFromSummaryStatisticsNameWithNullInput() throws MetricConfigException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify input configuration with a non-null name to map" );
        MetricConfigHelper.from( (SummaryStatisticsName) null );
    }

    /**
     * Tests the {@link MetricConfigHelper#getMetricsFromConfig(wres.config.generated.ProjectConfig)} by comparing
     * actual results to expected results.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetMetricsFromConfig() throws MetricConfigException
    {

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();

        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        assertTrue( MetricConfigHelper.getMetricsFromConfig( mockedConfig )
                                      .equals( new HashSet<>( Arrays.asList( MetricConstants.BIAS_FRACTION,
                                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                             MetricConstants.MEAN_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, wres.datamodel.DataFactory, 
     * java.util.Collection)} by comparing actual results to expected results for a scenario where external thresholds
     * are not defined and no threshold dimension is defined.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetThresholdsFromConfigWithoutExternalThresholdsOrDimension() throws MetricConfigException
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = MetricConfigHelper.getThresholdsFromConfig( mockedConfig,
                                                                                        dataFac,
                                                                                        (ThresholdsByMetric) null );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new HashMap<>();
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.1 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.2 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );

        assertTrue( actual.equals( expected ) );

        // Check for the same result when specifying an explicit pair configuration with null dimension
        ProjectConfig mockedConfigWithNullDimension =
                new ProjectConfig( null,
                                   new PairConfig( null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Compute combined thresholds
        ThresholdsByMetric actualByMetricNullDimension =
                MetricConfigHelper.getThresholdsFromConfig( mockedConfigWithNullDimension,
                                                            dataFac,
                                                            (ThresholdsByMetric) null );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actualWithNullDim =
                actualByMetricNullDimension.getOneOrTwoThresholds();

        assertTrue( actualWithNullDim.equals( expected ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, wres.datamodel.DataFactory, 
     * java.util.Collection)} by comparing actual results to expected results for a scenario where external thresholds
     * are defined, together with a dimension for value thresholds.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetThresholdsFromConfigWithExternalThresholdsAndDimension() throws MetricConfigException
    {

        // Obtain the threshold dimension
        MetadataFactory metaFac = dataFac.getMetadataFactory();
        Dimension dimension = metaFac.getDimension( "CMS" );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   new PairConfig( "CMS",
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Mock external thresholds
        Map<MetricConstants, Set<Threshold>> mockExternal = new HashMap<>();
        Set<Threshold> atomicExternal = new HashSet<>();
        atomicExternal.add( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
                                                 Operator.GREATER,
                                                 ThresholdConstants.ThresholdDataType.LEFT,
                                                 dimension ) );
        mockExternal.put( MetricConstants.BIAS_FRACTION, atomicExternal );

        ThresholdsByMetric externalThresholds =
                dataFac.ofThresholdsByMetricBuilder().addThresholds( mockExternal, ThresholdGroup.VALUE ).build();

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = MetricConfigHelper.getThresholdsFromConfig( mockedConfig,
                                                                                        dataFac,
                                                                                        externalThresholds );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new HashMap<>();
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.1 ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.2 ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          dimension ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );

        assertTrue( actual.equals( expected ) );
    }


}
