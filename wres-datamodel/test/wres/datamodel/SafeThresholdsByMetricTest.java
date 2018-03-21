package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.SafeThresholdsByMetric.SafeThresholdsByMetricBuilder;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.ThresholdsByMetric.ApplicationType;
import wres.datamodel.ThresholdsByMetric.ThresholdType;

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
     * Tests the {@link SafeThresholdsByMetric#getThresholds(wres.datamodel.ThresholdsByMetric.ThresholdType)}.
     */

    @Test
    public void testGetThresholds()
    {

        // Probability thresholds
        Map<MetricConstants, Set<Threshold>> probabilities = new HashMap<>();

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                                                         Operator.GREATER ),
                                                         FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                                         Operator.GREATER ) ) ) );

        // Value thresholds
        Map<MetricConstants, Set<Threshold>> values = new HashMap<>();
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                       Operator.GREATER ) ) ) );

        // Probability classifier thresholds
        Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new HashMap<>();
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                                                                  Operator.GREATER ) ) ) );

        // Quantile thresholds
        Map<MetricConstants, Set<Threshold>> quantiles = new HashMap<>();
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                                  FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                                                  Operator.GREATER ) ) ) );

        ThresholdsByMetric container = this.getDefaultContainerOne();

        // Check the probability thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdType.PROBABILITY ).equals( probabilities ) );

        // Check the value thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdType.VALUE ).equals( values ) );

        // Check the probability classifier thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdType.PROBABILITY_CLASSIFIER ).equals( probabilityClassifiers ) );

        // Check the quantile thresholds
        assertTrue( "Unexpected thresholds in container.",
                    container.getThresholds( ThresholdType.QUANTILE ).equals( quantiles ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#hasType(ThresholdType)}.
     */

    @Test
    public void testHasType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        assertTrue( "Expected probability thresholds in the container.",
                    container.hasType( ThresholdType.PROBABILITY ) );

        assertTrue( "Expected probability classifier thresholds in the container.",
                    container.hasType( ThresholdType.PROBABILITY_CLASSIFIER ) );

        assertTrue( "Expected value thresholds in the container.", container.hasType( ThresholdType.VALUE ) );

        assertTrue( "Expected quantiles thresholds in the container.", container.hasType( ThresholdType.QUANTILE ) );

        // Check absence
        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        assertFalse( "Expected quantiles thresholds in the container.",
                     containerTwo.hasType( ThresholdType.QUANTILE ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#union()}.
     */

    @Test
    public void testUnion()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER ) );

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

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForThisMetric( MetricConstants.FREQUENCY_BIAS ) ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForThisMetric( MetricConstants.BIAS_FRACTION ) ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionForTheseTypes(ThresholdType...)}.
     */

    @Test
    public void testUnionForTheseTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForTheseTypes( ThresholdType.PROBABILITY,
                                                                   ThresholdType.QUANTILE ) ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForTheseTypes( (ThresholdType[]) null ) ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.unionForTheseTypes() ) );
    }

    /**
     * Tests the {@link SafeThresholdsByMetric#unionForThisMetricAndTheseTypes(MetricConstants, ThresholdType...)}.
     */

    @Test
    public void testUnionForThisMetricAndTheseTypes()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                ThresholdType.PROBABILITY,
                                                                                ThresholdType.QUANTILE ) ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet()
                               .equals( container.unionForThisMetricAndTheseTypes( MetricConstants.FREQUENCY_BIAS,
                                                                                   (ThresholdType[]) null ) ) );

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

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ), Operator.GREATER_EQUAL ) );

        expected.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                           Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                      Operator.GREATER ) );

        expected.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                   FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                   Operator.GREATER ) );

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
                                                            Operator.GREATER_EQUAL ) );

        expectedSecond.add( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 12.0 ), Operator.LESS ) );

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
                                                               Operator.GREATER );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ),
                                                                             Operator.GREATER ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                             Operator.GREATER ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                  Operator.GREATER ),
                                             classifier ) );

        expected.add( OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                          FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                                          Operator.GREATER ),
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

        Set<ThresholdType> expected = new HashSet<>( Arrays.asList( ThresholdType.values() ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    container.getThresholdTypes().equals( expected ) );

        ThresholdsByMetric containerTwo = this.getDefaultContainerTwo();

        Set<ThresholdType> expectedTwo = new HashSet<>( Arrays.asList( ThresholdType.PROBABILITY ) );

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

        Set<ThresholdType> expected = new HashSet<>( Arrays.asList( ThresholdType.values() ) );

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
                                                              Operator.GREATER );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.hasTheseMetricsForThisThreshold( threshold ).equals( expected ) );

        // Empty set       
        Threshold secondThreshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.9 ),
                                                                    Operator.GREATER );

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
                                                              Operator.GREATER );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.doesNotHaveTheseMetricsForThisThreshold( threshold ).equals( Collections.emptySet() ) );

        // Empty set       
        Threshold secondThreshold = FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.9 ),
                                                                    Operator.GREATER );

        Set<MetricConstants> expected = new HashSet<>( Arrays.asList( MetricConstants.FREQUENCY_BIAS ) );

        assertTrue( "Unexpected metrics for this threshold.",
                    container.doesNotHaveTheseMetricsForThisThreshold( secondThreshold ).equals( expected ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#filterByType(ThresholdType...)}.
     */

    @Test
    public void testFilterByType()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByType( ThresholdType.PROBABILITY ).union() ) );

        // Test the empty set
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.filterByType().union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    Collections.emptySet().equals( container.filterByType( (ThresholdType[]) null ).union() ) );

        // Set all types       
        assertTrue( "Unexpected union of thresholds in the container.",
                    container == container.filterByType( ThresholdType.values() ) );

    }

    /**
     * Tests the {@link SafeThresholdsByMetric#filterByGroup(wres.datamodel.MetricConstants.MetricInputGroup)}.
     */

    @Test
    public void testFilterByGroup()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<Threshold> expected = new HashSet<>();

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.0 ), Operator.GREATER ) );

        expected.add( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ), Operator.GREATER ) );

        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( MetricInputGroup.DICHOTOMOUS,
                                                              MetricOutputGroup.DOUBLE_SCORE )
                                              .filterByType( ThresholdType.PROBABILITY )
                                              .union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( MetricInputGroup.DICHOTOMOUS,
                                                              null )
                                              .filterByType( ThresholdType.PROBABILITY )
                                              .union() ) );
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( null,
                                                              MetricOutputGroup.DOUBLE_SCORE )
                                              .filterByType( ThresholdType.PROBABILITY )
                                              .union() ) );

        // Test the unfiltered set
        assertTrue( "Unexpected union of thresholds in the container.",
                    expected.equals( container.filterByGroup( null, null )
                                              .filterByType( ThresholdType.PROBABILITY )
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
                                                                             Operator.GREATER ) ) );
        expected.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.95 ),
                                                                             Operator.GREATER ) ) );

        Set<OneOrTwoThresholds> thresholds = container.unionOfOneOrTwoThresholds();

        assertTrue( "Unexpected union of thresholds in the container.",
                    thresholds.equals( expected ) );
        
        ThresholdsByMetric secondContainer = this.getDefaultContainerTwo();

        Set<OneOrTwoThresholds> expectedTwo = new HashSet<>();

        expectedTwo.add( OneOrTwoThresholds.of( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.7 ),
                                                                             Operator.GREATER_EQUAL ) ) );

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
                                                                                         Operator.GREATER ),
                                                         FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.1 ),
                                                                                         Operator.GREATER ) ) ) );

        builder.addThresholds( probabilities, ThresholdType.PROBABILITY, ApplicationType.LEFT );

        // Value thresholds
        Map<MetricConstants, Set<Threshold>> values = new HashMap<>();
        values.put( MetricConstants.FREQUENCY_BIAS,
                    new HashSet<>( Arrays.asList( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( 0.2 ),
                                                                       Operator.GREATER ) ) ) );
        builder.addThresholds( values, ThresholdType.VALUE, ApplicationType.LEFT );

        // Probability classifier thresholds
        Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new HashMap<>();
        probabilityClassifiers.put( MetricConstants.FREQUENCY_BIAS,
                                    new HashSet<>( Arrays.asList( FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.3 ),
                                                                                                  Operator.GREATER ) ) ) );
        builder.addThresholds( probabilityClassifiers, ThresholdType.PROBABILITY_CLASSIFIER, ApplicationType.LEFT );

        // Quantile thresholds
        Map<MetricConstants, Set<Threshold>> quantiles = new HashMap<>();
        quantiles.put( MetricConstants.FREQUENCY_BIAS,
                       new HashSet<>( Arrays.asList( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( 0.4 ),
                                                                                  FACTORY.ofOneOrTwoDoubles( 0.5 ),
                                                                                  Operator.GREATER ) ) ) );
        builder.addThresholds( quantiles, ThresholdType.QUANTILE, ApplicationType.LEFT );

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
                                                                                         Operator.GREATER_EQUAL ) ) ) );
        builder.addThresholds( probabilities, ThresholdType.PROBABILITY, ApplicationType.LEFT );

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
                                                                       Operator.LESS ) ) ) );
        builder.addThresholds( values, ThresholdType.VALUE, ApplicationType.LEFT );

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
                                                                                  Operator.GREATER ),
                                                  FACTORY.ofProbabilityThreshold( FACTORY.ofOneOrTwoDoubles( 0.95 ),
                                                                                  Operator.GREATER ) ) ) );
        builder.addThresholds( probabilities, ThresholdType.PROBABILITY, ApplicationType.LEFT );

        return builder.build();
    }

}
