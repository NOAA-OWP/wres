package wres.datamodel.thresholds;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * Tests the {@link ThresholdsByMetric}.
 * 
 * @author James Brown
 */

public class ThresholdsByMetricTest
{
    /**
     * Tests the {@link ThresholdsByMetric#getThresholds(wres.datamodel.ThresholdsByMetric.ThresholdGroup)}.
     */

    @Test
    public void testGetThresholds()
    {
        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ),
                                                         ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ) ) ) );

        // Value thresholds
        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ) ) ) );

        // Probability classifier thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                                                         Operator.GREATER,
                                                                                                         ThresholdDataType.LEFT ) ) ) );

        // Quantile thresholds
        Map<MetricConstants, Set<ThresholdOuter>> quantiles = new EnumMap<>( MetricConstants.class );
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                                                         OneOrTwoDoubles.of( 0.5 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ) ) ) );

        ThresholdsByMetric container = this.getDefaultContainerOne();

        // Check the probability thresholds
        assertTrue( container.getThresholds( ThresholdGroup.PROBABILITY ).equals( probabilities ) );

        // Check the value thresholds
        assertTrue( container.getThresholds( ThresholdGroup.VALUE ).equals( values ) );

        // Check the probability classifier thresholds
        assertTrue( container.getThresholds( ThresholdGroup.PROBABILITY_CLASSIFIER )
                             .equals( probabilityClassifiers ) );

        // Check the quantile thresholds
        assertTrue( container.getThresholds( ThresholdGroup.QUANTILE ).equals( quantiles ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#hasGroup(ThresholdGroup)}.
     */

    @Test
    public void testHasType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        assertTrue( "Expected probability thresholds in the container.",
                    container.hasGroup( ThresholdGroup.PROBABILITY ) );

        assertTrue( "Expected probability classifier thresholds in the container.",
                    container.hasGroup( ThresholdGroup.PROBABILITY_CLASSIFIER ) );

        assertTrue( "Expected value thresholds in the container.", container.hasGroup( ThresholdGroup.VALUE ) );

        assertTrue( "Expected quantiles thresholds in the container.", container.hasGroup( ThresholdGroup.QUANTILE ) );

        // Check absence
        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        assertFalse( "Expected quantiles thresholds in the container.",
                     containerTwo.hasGroup( ThresholdGroup.QUANTILE ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#union()}.
     */

    @Test
    public void testUnion()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                          OneOrTwoDoubles.of( 0.5 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        assertTrue( expected.equals( container.union() ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#union(MetricConstants, ThresholdGroup...)}.
     */

    @Test
    public void testUnionForThisMetricAndTheseTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                          OneOrTwoDoubles.of( 0.5 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        assertTrue( expected.equals( container.union( MetricConstants.FREQUENCY_BIAS,
                                                                                ThresholdGroup.PROBABILITY,
                                                                                ThresholdGroup.QUANTILE ) ) );

        // Test the empty set
        assertTrue( Collections.emptySet()
                               .equals( container.union( MetricConstants.FREQUENCY_BIAS,
                                                                                   (ThresholdGroup[]) null ) ) );

        assertTrue( Collections.emptySet()
                               .equals( container.union( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#getThresholdTypesForThisMetric(MetricConstants)}.
     */

    @Test
    public void testGetThresholdTypesForThisMetric()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdGroup> expected = new HashSet<>( Arrays.asList( ThresholdGroup.values() ) );

        assertTrue( container.getThresholdTypesForThisMetric( MetricConstants.FREQUENCY_BIAS ).equals( expected ) );

        // Empty set
        assertTrue( container.getThresholdTypesForThisMetric( MetricConstants.MEAN_ERROR )
                             .equals( Collections.emptySet() ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#getMetrics()}.
     */

    @Test
    public void testHasThresholdsForTheseMetrics()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( container.getMetrics().equals( expected ) );
    }
    
    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerOne()
    {

        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ),
                                                         ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ) ) ) );

        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        // Value thresholds
        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( values, ThresholdGroup.VALUE );

        // Probability classifier thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                                                         Operator.GREATER,
                                                                                                         ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilityClassifiers, ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Quantile thresholds
        Map<MetricConstants, Set<ThresholdOuter>> quantiles = new EnumMap<>( MetricConstants.class );
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                                                         OneOrTwoDoubles.of( 0.5 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( quantiles, ThresholdGroup.QUANTILE );

        return builder.build();
    }

    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerTwo()
    {

        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );
        probabilities.put( MetricConstants.BRIER_SCORE,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                                                                Operator.GREATER_EQUAL,
                                                                                                ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        return builder.build();
    }

}
