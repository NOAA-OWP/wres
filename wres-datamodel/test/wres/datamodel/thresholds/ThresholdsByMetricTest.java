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

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * Tests the {@link ThresholdsByMetric}.
 * 
 * @author james.brown@hydrosolved.com
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
     * Tests the {@link ThresholdsByMetric#hasType(ThresholdGroup)}.
     */

    @Test
    public void testHasType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        assertTrue( "Expected probability thresholds in the container.",
                    container.hasType( ThresholdGroup.PROBABILITY ) );

        assertTrue( "Expected probability classifier thresholds in the container.",
                    container.hasType( ThresholdGroup.PROBABILITY_CLASSIFIER ) );

        assertTrue( "Expected value thresholds in the container.", container.hasType( ThresholdGroup.VALUE ) );

        assertTrue( "Expected quantiles thresholds in the container.", container.hasType( ThresholdGroup.QUANTILE ) );

        // Check absence
        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        assertFalse( "Expected quantiles thresholds in the container.",
                     containerTwo.hasType( ThresholdGroup.QUANTILE ) );

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
     * Tests the {@link ThresholdsByMetric#unionForThisMetric(MetricConstants)}.
     */

    @Test
    public void testUnionForThisMetric()
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

        assertTrue( expected.equals( container.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );

        assertTrue( Collections.emptySet().equals( container.unionForThisMetric( MetricConstants.BIAS_FRACTION ) ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#unionForTheseTypes(ThresholdGroup...)}.
     */

    @Test
    public void testUnionForTheseTypes()
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

        assertTrue( expected.equals( container.unionForTheseTypes( ThresholdGroup.PROBABILITY,
                                                                   ThresholdGroup.QUANTILE ) ) );

        // Test the empty set
        assertTrue( Collections.emptySet().equals( container.unionForTheseTypes( (ThresholdGroup[]) null ) ) );

        assertTrue( Collections.emptySet().equals( container.unionForTheseTypes() ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#unionForThisMetricAndTheseTypes(MetricConstants, ThresholdGroup...)}.
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

        assertTrue( expected.equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                ThresholdGroup.PROBABILITY,
                                                                                ThresholdGroup.QUANTILE ) ) );

        // Test the empty set
        assertTrue( Collections.emptySet()
                               .equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                   (ThresholdGroup[]) null ) ) );

        assertTrue( Collections.emptySet()
                               .equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#unionWithThisStore(ThresholdsByMetric)}.
     */

    @Test
    public void testUnionWithThisStore()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                          Operator.GREATER_EQUAL,
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

        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        ThresholdsByMetric union = container.unionWithThisStore( containerTwo );

        assertTrue( expected.equals( union.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );

        // Union with itself
        assertTrue( union.unionWithThisStore( union ) == union );

        // Union for stores with different types        
        ThresholdsByMetric secondUnion = containerTwo.unionWithThisStore( this.getDefaultContainerThree() );

        Set<ThresholdOuter> expectedSecond = new HashSet<>();

        expectedSecond.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                                Operator.GREATER_EQUAL,
                                                                ThresholdDataType.LEFT ) );

        expectedSecond.add( ThresholdOuter.of( OneOrTwoDoubles.of( 12.0 ),
                                                     Operator.LESS,
                                                     ThresholdDataType.LEFT ) );

        assertTrue( expectedSecond.equals( secondUnion.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#unionOfOneOrTwoThresholds()}.
     */

    @Test
    public void testUnionOfOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        ThresholdOuter classifier = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                                 Operator.GREATER,
                                                                                 ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                 Operator.GREATER,
                                                                                 ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                                              OneOrTwoDoubles.of( 0.5 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ),
                                             classifier ) );

        Set<OneOrTwoThresholds> thresholds = container.unionOfOneOrTwoThresholds();

        assertTrue( expected.equals( thresholds ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#getThresholdTypes()}.
     */

    @Test
    public void testGetThresholdTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdGroup> expected = new HashSet<>( Arrays.asList( ThresholdGroup.values() ) );

        assertTrue( container.getThresholdTypes().equals( expected ) );

        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        Set<ThresholdGroup> expectedTwo = new HashSet<>( Arrays.asList( ThresholdGroup.PROBABILITY ) );

        assertTrue( containerTwo.getThresholdTypes().equals( expectedTwo ) );
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
     * Tests the {@link ThresholdsByMetric#hasTheseMetricsForThisThreshold(ThresholdOuter)}.
     */

    @Test
    public void testHasTheseMetricsForThisThreshold()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        ThresholdOuter threshold = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT );

        assertTrue( container.hasTheseMetricsForThisThreshold( threshold ).equals( expected ) );

        // Empty set       
        ThresholdOuter secondThreshold = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.9 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT );

        assertTrue( container.hasTheseMetricsForThisThreshold( secondThreshold ).equals( Collections.emptySet() ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#doesNotHaveTheseMetricsForThisThreshold(ThresholdOuter)}.
     */

    @Test
    public void testDoesNotHaveTheseMetricsForThisThreshold()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();


        ThresholdOuter threshold = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT );

        assertTrue( container.doesNotHaveTheseMetricsForThisThreshold( threshold ).equals( Collections.emptySet() ) );

        // Empty set       
        ThresholdOuter secondThreshold = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.9 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT );

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( container.doesNotHaveTheseMetricsForThisThreshold( secondThreshold ).equals( expected ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#hasThresholdsForTheseMetrics()}.
     */

    @Test
    public void testHasThresholdsForTheseMetrics()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( container.hasThresholdsForTheseMetrics().equals( expected ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#filterByType(ThresholdGroup...)}.
     */

    @Test
    public void testFilterByType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        assertTrue( expected.equals( container.filterByType( ThresholdGroup.PROBABILITY ).union() ) );

        // Test the empty set
        assertTrue( Collections.emptySet().equals( container.filterByType().union() ) );
        assertTrue( Collections.emptySet().equals( container.filterByType( (ThresholdGroup[]) null ).union() ) );

        // Set all types       
        assertTrue( container == container.filterByType( ThresholdGroup.values() ) );

    }

    /**
     * Tests the {@link ThresholdsByMetric#filterByGroup(wres.datamodel.MetricConstants.SampleDataGroup)}.
     */

    @Test
    public void testFilterByGroup()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        assertTrue( expected.equals( container.filterByGroup( SampleDataGroup.DICHOTOMOUS,
                                                              StatisticType.DOUBLE_SCORE )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
        assertTrue( expected.equals( container.filterByGroup( SampleDataGroup.DICHOTOMOUS,
                                                              null )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
        assertTrue( expected.equals( container.filterByGroup( null,
                                                              StatisticType.DOUBLE_SCORE )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );

        // Test the unfiltered set
        assertTrue( expected.equals( container.filterByGroup( null, null )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
    }

    /**
     * Tests the {@link ThresholdsByMetric#getOneOrTwoThresholds().
     */

    @Test
    public void testGetOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerFour();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.85 ),
                                                                                 Operator.GREATER,
                                                                                 ThresholdDataType.LEFT ) ) );
        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                 Operator.GREATER,
                                                                                 ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholds = container.unionOfOneOrTwoThresholds();

        assertTrue( thresholds.equals( expected ) );

        ThresholdsByMetric secondContainer = this.getDefaultContainerTwo();

        Set<OneOrTwoThresholds> expectedTwo = new HashSet<>();

        expectedTwo.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                                                    Operator.GREATER_EQUAL,
                                                                                    ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholdsTwo = secondContainer.unionOfOneOrTwoThresholds();

        assertTrue( thresholdsTwo.equals( expectedTwo ) );
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
        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                                                             Operator.GREATER_EQUAL,
                                                                                             ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        return builder.build();
    }

    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerThree()
    {

        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 12.0 ),
                                                                           Operator.LESS,
                                                                           ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( values, ThresholdGroup.VALUE );

        return builder.build();
    }

    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerFour()
    {

        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );
        probabilities.put( MetricConstants.MEAN_ERROR,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.85 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ),
                                                         ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        return builder.build();
    }

}
