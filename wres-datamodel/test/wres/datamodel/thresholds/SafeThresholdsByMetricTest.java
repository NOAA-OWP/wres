package wres.datamodel.thresholds;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.SafeThresholdsByMetric;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.SafeThresholdsByMetric.SafeThresholdsByMetricBuilder;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * Tests the {@link SafeThresholdsByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SafeThresholdsByMetricTest
{

    /**
     * Instance of a {@link DataFactory}.
     */

    private final DataFactory FACTORY = DefaultDataFactory.getInstance();

    /**
     * Tests the {@link SafeThresholdsByMetric#getThresholds(wres.datamodel.ThresholdsByMetric.ThresholdGroup)}.
     */

    @Test
    public void testGetThresholds()
    {

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> probabilities = new HashMap<>();

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ),
                                                         FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ) ) ) );

        // Value thresholds
        Map<MetricConstants, Set<Threshold>> values = new HashMap<>();
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) ) ) );

        // Probability classifier thresholds
        Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new HashMap<>();
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                                                                  Operator.GREATER,
                                                                                                  ThresholdDataType.LEFT ) ) ) );

        // Quantile thresholds
        Map<MetricConstants, Set<Threshold>> quantiles = new HashMap<>();
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                                  FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                                                  Operator.GREATER,
                                                                                  ThresholdDataType.LEFT ) ) ) );

        ThresholdsByMetric container = this.getDefaultContainerOne();

        // Check the probability thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdGroup.PROBABILITY ).equals( probabilities ) );

        // Check the value thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdGroup.VALUE ).equals( values ) );

        // Check the probability classifier thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdGroup.PROBABILITY_CLASSIFIER )
                             .equals( probabilityClassifiers ) );

        // Check the quantile thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdGroup.QUANTILE ).equals( quantiles ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#hasType(ThresholdGroup)}.
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
     * Tests the {@link SafeThresholdsByMetric#union()}.
     */

    @Test
    public void testUnion()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.union() ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionForThisMetric(MetricConstants)}.
     */

    @Test
    public void testUnionForThisMetric()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForThisMetric( MetricConstants.BIAS_FRACTION ) ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionForTheseTypes(ThresholdGroup...)}.
     */

    @Test
    public void testUnionForTheseTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForTheseTypes( ThresholdGroup.PROBABILITY,
                                                                   ThresholdGroup.QUANTILE ) ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForTheseTypes( (ThresholdGroup[]) null ) ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForTheseTypes() ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionForThisMetricAndTheseTypes(MetricConstants, ThresholdGroup...)}.
     */

    @Test
    public void testUnionForThisMetricAndTheseTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                ThresholdGroup.PROBABILITY,
                                                                                ThresholdGroup.QUANTILE ) ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet()
                               .equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                   (ThresholdGroup[]) null ) ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet()
                               .equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionWithThisStore(ThresholdsByMetric)}.
     */

    @Test
    public void testUnionWithThisStore()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ),
                                                      Operator.GREATER_EQUAL,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER,
                                           ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT ) );

        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        ThresholdsByMetric union = container.unionWithThisStore( containerTwo );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( union.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );

        // Union with itself
        assertTrue( "Unexpected union of thresholds in the container.", union.unionWithThisStore( union ) == union );

        // Union for stores with different types        
        ThresholdsByMetric secondUnion = containerTwo.unionWithThisStore( this.getDefaultContainerThree() );

        Set<Threshold> expectedSecond = new HashSet<>();

        expectedSecond.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ),
                                                            Operator.GREATER_EQUAL,
                                                            ThresholdDataType.LEFT ) );

        expectedSecond.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 12.0 ),
                                                 Operator.LESS,
                                                 ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expectedSecond.equals( secondUnion.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionOfOneOrTwoThresholds()}.
     */

    @Test
    public void testUnionOfOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        Threshold classifier = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                               Operator.GREATER,
                                                               ThresholdDataType.LEFT );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                          FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                                          Operator.GREATER,
                                                                          ThresholdDataType.LEFT ),
                                             classifier ) );

        Set<OneOrTwoThresholds> thresholds = container.unionOfOneOrTwoThresholds();

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( thresholds ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#getThresholdTypes()}.
     */

    @Test
    public void testGetThresholdTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdGroup> expected = new HashSet<>( Arrays.asList( ThresholdGroup.values() ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    container.getThresholdTypes().equals( expected ) );

        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        Set<ThresholdGroup> expectedTwo = new HashSet<>( Arrays.asList( ThresholdGroup.PROBABILITY ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    containerTwo.getThresholdTypes().equals( expectedTwo ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#getThresholdTypesForThisMetric(MetricConstants)}.
     */

    @Test
    public void testGetThresholdTypesForThisMetric()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdGroup> expected = new HashSet<>( Arrays.asList( ThresholdGroup.values() ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    container.getThresholdTypesForThisMetric( MetricConstants.FREQUENCY_BIAS ).equals( expected ) );

        // Empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    container.getThresholdTypesForThisMetric( MetricConstants.MEAN_ERROR )
                             .equals( Collections.EMPTY_SET ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#hasTheseMetricsForThisThreshold(Threshold)}.
     */

    @Test
    public void testHasTheseMetricsForThisThreshold()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        Threshold threshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.hasTheseMetricsForThisThreshold( threshold ).equals( expected ) );

        // Empty set       
        Threshold secondThreshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.9 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.hasTheseMetricsForThisThreshold( secondThreshold ).equals( Collections.emptySet() ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#doesNotHaveTheseMetricsForThisThreshold(Threshold)}.
     */

    @Test
    public void testDoesNotHaveTheseMetricsForThisThreshold()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();


        Threshold threshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.doesNotHaveTheseMetricsForThisThreshold( threshold ).equals( Collections.emptySet() ) );

        // Empty set       
        Threshold secondThreshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.9 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT );

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.doesNotHaveTheseMetricsForThisThreshold( secondThreshold ).equals( expected ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#hasThresholdsForTheseMetrics()}.
     */

    @Test
    public void testHasThresholdsForTheseMetrics()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.hasThresholdsForTheseMetrics().equals( expected ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#filterByType(ThresholdGroup...)}.
     */

    @Test
    public void testFilterByType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByType( ThresholdGroup.PROBABILITY ).union() ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.filterByType().union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.filterByType( (ThresholdGroup[]) null ).union() ) );

        // Set all types       
        assertTrue( "Unexpected union of thresholds in the container.",
                    container == container.filterByType( ThresholdGroup.values() ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#filterByGroup(wres.datamodel.MetricConstants.MetricInputGroup)}.
     */

    @Test
    public void testFilterByGroup()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( MetricInputGroup.DICHOTOMOUS,
                                                              MetricOutputGroup.DOUBLE_SCORE )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( MetricInputGroup.DICHOTOMOUS,
                                                              null )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( null,
                                                              MetricOutputGroup.DOUBLE_SCORE )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );

        // Test the unfiltered set
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( null, null )
                                              .filterByType( ThresholdGroup.PROBABILITY )
                                              .union() ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#getOneOrTwoThresholds().
     */

    @Test
    public void testGetOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerFour();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.85 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ) ) );
        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.95 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholds = container.unionOfOneOrTwoThresholds();

        assertTrue( "Unexpected union of thresholds in the container.",
                    thresholds.equals( expected ) );

        ThresholdsByMetric secondContainer = this.getDefaultContainerTwo();

        Set<OneOrTwoThresholds> expectedTwo = new HashSet<>();

        expectedTwo.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ),
                                                                                Operator.GREATER_EQUAL,
                                                                                ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholdsTwo = secondContainer.unionOfOneOrTwoThresholds();

        assertTrue( "Unexpected union of thresholds in the container.",
                    thresholdsTwo.equals( expectedTwo ) );
    }

    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerOne()
    {

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> probabilities = new HashMap<>();

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ),
                                                         FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ) ) ) );

        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        // Value thresholds
        Map<MetricConstants, Set<Threshold>> values = new HashMap<>();
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( values, ThresholdGroup.VALUE );

        // Probability classifier thresholds
        Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new HashMap<>();
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                                                                  Operator.GREATER,
                                                                                                  ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilityClassifiers, ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Quantile thresholds
        Map<MetricConstants, Set<Threshold>> quantiles = new HashMap<>();
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                                  FACTORY.ofOneOrTwoDoubles( 0.5 ),
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

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> probabilities = new HashMap<>();
        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ),
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

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> values = new HashMap<>();
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 12.0 ),
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

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> probabilities = new HashMap<>();
        probabilities.put( MetricConstants.MEAN_ERROR,
                           new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.85 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ),
                                                         FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.95 ),
                                                                                         Operator.GREATER,
                                                                                         ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        return builder.build();
    }

}
