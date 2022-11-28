package wres.datamodel.thresholds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import wres.datamodel.Climatology;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link ThresholdSlicer}.
 * 
 * @author James Brown
 */

class ThresholdSlicerTest
{

    private static final String CMS = "CMS";
    private static final String FLOOD = "FLOOD";

    private FeatureTuple featureTuple;
    private FeatureTuple anotherFeatureTuple;

    @Before
    public void runBeforeEachTest()
    {
        Geometry a = MessageFactory.getGeometry( "a" );
        Geometry b = MessageFactory.getGeometry( "b" );

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( a, b, null );
        this.featureTuple = FeatureTuple.of( geoTuple );

        Geometry c = MessageFactory.getGeometry( "c" );
        Geometry d = MessageFactory.getGeometry( "d" );

        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( c, d, null );
        this.anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );
    }

    @Test
    void testDecompose()
    {
        ThresholdOuter oneThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT,
                                                         "ACTION",
                                                         MeasurementUnit.of( CMS ) );

        ThresholdOuter anotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT,
                                                             FLOOD,
                                                             MeasurementUnit.of( CMS ) );

        Set<ThresholdOuter> someThresholds = Set.of( oneThreshold, anotherThreshold );

        ThresholdOuter oneMoreThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT,
                                                             "ACTION",
                                                             MeasurementUnit.of( CMS ) );

        ThresholdOuter yetAnotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 4.0 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT,
                                                                FLOOD,
                                                                MeasurementUnit.of( CMS ) );

        Set<ThresholdOuter> someMoreThresholds = Set.of( oneMoreThreshold, yetAnotherThreshold );

        Map<FeatureTuple, Set<ThresholdOuter>> mapOfThresholds = new HashMap<>();

        mapOfThresholds.put( this.featureTuple, someThresholds );
        mapOfThresholds.put( this.anotherFeatureTuple, someMoreThresholds );

        List<Map<FeatureTuple, ThresholdOuter>> actual = ThresholdSlicer.decompose( mapOfThresholds );

        List<Map<FeatureTuple, ThresholdOuter>> expected = new ArrayList<>();

        Map<FeatureTuple, ThresholdOuter> aMap = new HashMap<>();
        aMap.put( this.featureTuple, oneThreshold );
        aMap.put( this.anotherFeatureTuple, oneMoreThreshold );

        Map<FeatureTuple, ThresholdOuter> anotherMap = new HashMap<>();
        anotherMap.put( this.featureTuple, anotherThreshold );
        anotherMap.put( this.anotherFeatureTuple, yetAnotherThreshold );

        expected.add( aMap );
        expected.add( anotherMap );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterByGroup()
    {
        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );

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

        probabilities.put( MetricConstants.FREQUENCY_BIAS,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ),
                                                         ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ) ) ) );

        ThresholdsByMetric unfilteredOne = builder.build();
        ThresholdsByMetric unfilteredTwo = new Builder().addThresholds( probabilities, ThresholdGroup.PROBABILITY )
                                                        .addThresholds( probabilityClassifiers,
                                                                        ThresholdGroup.PROBABILITY_CLASSIFIER )
                                                        .build();

        Map<FeatureTuple, ThresholdsByMetric> wrapped = new HashMap<>();
        wrapped.put( this.featureTuple, unfilteredOne );
        wrapped.put( this.anotherFeatureTuple, unfilteredTwo );

        Map<FeatureTuple, ThresholdsByMetric> actual = ThresholdSlicer.filterByGroup( wrapped,
                                                                                      SampleDataGroup.DICHOTOMOUS,
                                                                                      StatisticType.DOUBLE_SCORE,
                                                                                      ThresholdGroup.VALUE,
                                                                                      ThresholdGroup.PROBABILITY );

        Map<FeatureTuple, ThresholdsByMetric> expected = new HashMap<>();
        ThresholdsByMetric expectedOne = new Builder().addThresholds( values, ThresholdGroup.VALUE )
                                                      .build();
        ThresholdsByMetric expectedTwo = new Builder().addThresholds( probabilities, ThresholdGroup.PROBABILITY )
                                                      .build();

        expected.put( this.featureTuple, expectedOne );
        expected.put( this.anotherFeatureTuple, expectedTwo );

        assertEquals( expected, actual );
    }

    @Test
    public void testFilterByThresholdGroup()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<ThresholdOuter> expected = new HashSet<>();

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT ) );

        Set<ThresholdOuter> actual =
                ThresholdSlicer.filterByGroup( container,
                                               ThresholdGroup.PROBABILITY,
                                               ThresholdGroup.PROBABILITY_CLASSIFIER )
                               .union();

        assertEquals( expected, actual );

        // Test the empty set
        assertEquals( Collections.emptySet(), ThresholdSlicer.filterByGroup( container ).union() );
        assertEquals( Collections.emptySet(),
                      ThresholdSlicer.filterByGroup( container, (ThresholdGroup[]) null ).union() );

        // Set all types       
        assertSame( container, ThresholdSlicer.filterByGroup( container, ThresholdGroup.values() ) );
    }

    @Test
    void testAddQuantiles()
    {
        Geometry geometry = Geometry.newBuilder()
                                    .setName( "a" )
                                    .build();
        Feature feature = Feature.of( geometry );

        Climatology climatology =
                new Climatology.Builder().addClimatology( feature,
                                                          new double[] { 1.5, 4.9, 6.3, 27, 43.3, 433.9, 1012.6, 2009.8,
                                                                         7001.4, 12038.5, 17897.2 } )
                                         .build();

        wres.statistics.generated.Pool.Builder builder = wres.statistics.generated.Pool.newBuilder();
        GeometryTuple geoTupleOne = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder().setName( "a" ) )
                                                 .setRight( Geometry.newBuilder().setName( "b" ) )
                                                 .build();
        builder.setGeometryGroup( GeometryGroup.newBuilder().addGeometryTuples( geoTupleOne ) );

        FeatureTuple oneTuple = FeatureTuple.of( geoTupleOne );

        Map<FeatureTuple, Set<ThresholdOuter>> thresholds =
                Map.of( oneTuple,
                        Set.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                       Operator.GREATER_EQUAL,
                                                                       ThresholdDataType.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 7.0 / 11.0 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) ) );

        Map<FeatureTuple, Set<ThresholdOuter>> actual =
                ThresholdSlicer.addQuantiles( thresholds, climatology );

        Map<FeatureTuple, Set<ThresholdOuter>> expected =
                Map.of( oneTuple,
                        Set.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1.5 ),
                                                                    OneOrTwoDoubles.of( 0.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 17897.2 ),
                                                                    OneOrTwoDoubles.of( 1.0 ),
                                                                    Operator.GREATER_EQUAL,
                                                                    ThresholdDataType.LEFT ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1647.18182 ),
                                                                    OneOrTwoDoubles.of( 7.0 / 11.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 433.9 ),
                                                                    OneOrTwoDoubles.of( 0.5 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCompose()
    {
        ThresholdOuter first = ThresholdOuter.of( OneOrTwoDoubles.of( 100.0 ),
                                                  Operator.GREATER,
                                                  ThresholdDataType.RIGHT,
                                                  FLOOD,
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter second = ThresholdOuter.of( OneOrTwoDoubles.of( 1000.0 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.RIGHT,
                                                   FLOOD,
                                                   MeasurementUnit.of( CMS ) );

        ThresholdOuter actual = ThresholdSlicer.compose( Set.of( first, second ) );

        ThresholdOuter expected = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NaN ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.RIGHT,
                                                     FLOOD,
                                                     MeasurementUnit.of( CMS ) );

        assertEquals( expected, actual );

        ThresholdOuter third = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.RIGHT,
                                                                      "aThreshold" );

        ThresholdOuter fourth = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.RIGHT,
                                                                       "anotherThreshold" );

        ThresholdOuter actualTwo = ThresholdSlicer.compose( Set.of( third, fourth ) );

        ThresholdOuter expectedTwo = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.RIGHT );

        assertEquals( expectedTwo, actualTwo );

        ThresholdOuter fifth = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                  Operator.LESS,
                                                  ThresholdDataType.RIGHT,
                                                  "aThreshold",
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter sixth = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                  Operator.LESS,
                                                  ThresholdDataType.RIGHT,
                                                  "anotherThreshold",
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter actualThree = ThresholdSlicer.compose( Set.of( fifth, sixth ) );

        ThresholdOuter expectedThree = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                          Operator.LESS,
                                                          ThresholdDataType.RIGHT,
                                                          MeasurementUnit.of( CMS ) );

        assertEquals( expectedThree, actualThree );

        ThresholdOuter seventh = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                     OneOrTwoDoubles.of( 0.83 ),
                                                                     Operator.LESS,
                                                                     ThresholdDataType.RIGHT,
                                                                     "aThreshold",
                                                                     MeasurementUnit.of( CMS ) );

        ThresholdOuter eighth = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                    OneOrTwoDoubles.of( 0.83 ),
                                                                    Operator.LESS,
                                                                    ThresholdDataType.RIGHT,
                                                                    "anotherThreshold",
                                                                    MeasurementUnit.of( CMS ) );

        ThresholdOuter actualFour = ThresholdSlicer.compose( Set.of( seventh, eighth ) );

        ThresholdOuter expectedFour = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                          OneOrTwoDoubles.of( 0.83 ),
                                                                          Operator.LESS,
                                                                          ThresholdDataType.RIGHT,
                                                                          null,
                                                                          MeasurementUnit.of( CMS ) );

        assertEquals( expectedFour, actualFour );
    }

    @Test
    public void testFilterByThresholdValuesAndNames()
    {
        // Same values, different probabilities
        Set<ThresholdOuter> input = new HashSet<>();
        ThresholdOuter first = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                           .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                           .setOperator( Operator.GREATER_EQUAL )
                                                           .setDataType( ThresholdDataType.LEFT )
                                                           .build();
        ThresholdOuter second = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                            .setOperator( Operator.GREATER_EQUAL )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .build();

        input.add( first );
        input.add( second );

        Set<ThresholdOuter> actual = ThresholdSlicer.filter( Collections.unmodifiableSet( input ) );
        Set<ThresholdOuter> expected = Set.of( second );

        assertEquals( expected, actual );

        // Same values with different units and different probabilities
        Set<ThresholdOuter> anotherInput = new HashSet<>();
        ThresholdOuter anotherFirst = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                  .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                                  .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                  .setOperator( Operator.GREATER_EQUAL )
                                                                  .setDataType( ThresholdDataType.LEFT )
                                                                  .build();
        ThresholdOuter anotherSecond = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                   .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                   .setUnits( MeasurementUnit.of( "OTHER_UNIT" ) )
                                                                   .setOperator( Operator.GREATER_EQUAL )
                                                                   .setDataType( ThresholdDataType.LEFT )
                                                                   .build();

        anotherInput.add( anotherFirst );
        anotherInput.add( anotherSecond );

        Set<ThresholdOuter> anotherActual = ThresholdSlicer.filter( Collections.unmodifiableSet( anotherInput ) );
        Set<ThresholdOuter> anotherExpected = Set.of( anotherFirst, anotherSecond );

        assertEquals( anotherExpected, anotherActual );

        // Same values with same units and different probabilities and names
        Set<ThresholdOuter> yetAnotherInput = new HashSet<>();
        ThresholdOuter yetAnotherFirst = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                     .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                                     .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                     .setOperator( Operator.GREATER_EQUAL )
                                                                     .setDataType( ThresholdDataType.LEFT )
                                                                     .setLabel( "name" )
                                                                     .build();
        ThresholdOuter yetAnotherSecond = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                      .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                      .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                      .setOperator( Operator.GREATER_EQUAL )
                                                                      .setDataType( ThresholdDataType.LEFT )
                                                                      .setLabel( "anotherName" )
                                                                      .build();

        yetAnotherInput.add( yetAnotherFirst );
        yetAnotherInput.add( yetAnotherSecond );

        Set<ThresholdOuter> yetAnotherActual = ThresholdSlicer.filter( Collections.unmodifiableSet( yetAnotherInput ) );
        Set<ThresholdOuter> yetAnotherExpected = Set.of( yetAnotherFirst, yetAnotherSecond );

        assertEquals( yetAnotherExpected, yetAnotherActual );

        // No values with different probabilities
        Set<ThresholdOuter> oneMoreInput = new HashSet<>();
        ThresholdOuter oneMoreFirst = new ThresholdOuter.Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                                  .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                  .setOperator( Operator.GREATER_EQUAL )
                                                                  .setDataType( ThresholdDataType.LEFT )
                                                                  .build();
        ThresholdOuter oneMoreSecond = new ThresholdOuter.Builder().setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                   .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                   .setOperator( Operator.GREATER_EQUAL )
                                                                   .setDataType( ThresholdDataType.LEFT )
                                                                   .build();

        oneMoreInput.add( oneMoreFirst );
        oneMoreInput.add( oneMoreSecond );

        Set<ThresholdOuter> oneMoreActual = ThresholdSlicer.filter( Collections.unmodifiableSet( oneMoreInput ) );
        Set<ThresholdOuter> oneMoreExpected = Set.of( oneMoreFirst, oneMoreSecond );

        assertEquals( oneMoreExpected, oneMoreActual );
    }

    @Test
    public void testUnionOfOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerFour();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.85 ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) ) );
        expected.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholds = ThresholdSlicer.unionOfOneOrTwoThresholds( container );

        assertEquals( expected, thresholds );

        ThresholdsByMetric secondContainer = this.getDefaultContainerTwo();

        Set<OneOrTwoThresholds> expectedTwo = new HashSet<>();

        expectedTwo.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.7 ),
                                                                                       Operator.GREATER_EQUAL,
                                                                                       ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> thresholdsTwo = ThresholdSlicer.unionOfOneOrTwoThresholds( secondContainer );

        assertEquals( expectedTwo, thresholdsTwo );
    }

    @Test
    public void testUnionOfOneOrTwoThresholdsWithDichotomousScore()
    {
        ThresholdsByMetric container = this.getDefaultContainerOne();

        Set<OneOrTwoThresholds> expected = new HashSet<>();

        ThresholdOuter classifier = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                           Operator.GREATER,
                                                                           ThresholdDataType.LEFT );

        ThresholdOuter one = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT );
        expected.add( OneOrTwoThresholds.of( one, classifier ) );

        ThresholdOuter two = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT );

        expected.add( OneOrTwoThresholds.of( two, classifier ) );

        ThresholdOuter three = ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                  Operator.GREATER,
                                                  ThresholdDataType.LEFT );

        expected.add( OneOrTwoThresholds.of( three, classifier ) );

        ThresholdOuter four = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 0.4 ),
                                                                  OneOrTwoDoubles.of( 0.5 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT );

        expected.add( OneOrTwoThresholds.of( four, classifier ) );

        // Add the event thresholds without decision thresholds
        expected.add( OneOrTwoThresholds.of( one ) );
        expected.add( OneOrTwoThresholds.of( two ) );
        expected.add( OneOrTwoThresholds.of( three ) );
        expected.add( OneOrTwoThresholds.of( four ) );

        Set<OneOrTwoThresholds> thresholds = ThresholdSlicer.unionOfOneOrTwoThresholds( container );

        assertEquals( expected, thresholds );
    }

    @Test
    public void testGetOneOrTwoThresholds()
    {
        ThresholdsByMetric container = this.getDefaultContainerThree();

        SortedSet<OneOrTwoThresholds> expectedThresholds = new TreeSet<>();

        ThresholdOuter thresholdOne = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT );

        ThresholdOuter thresholdTwo = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT );

        ThresholdOuter thresholdThree = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT );

        expectedThresholds.add( OneOrTwoThresholds.of( thresholdOne ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdTwo ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdOne, thresholdThree ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdTwo, thresholdThree ) );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual = ThresholdSlicer.getOneOrTwoThresholds( container );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> expected =
                Map.of( MetricConstants.PROBABILITY_OF_DETECTION, expectedThresholds );

        assertEquals( expected, actual );
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

    /**
     * Returns a default container for testing.
     * 
     * @return a default container
     */

    private ThresholdsByMetric getDefaultContainerThree()
    {
        Builder builder = new Builder();

        // Probability thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );

        probabilities.put( MetricConstants.PROBABILITY_OF_DETECTION,
                           new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ),
                                                         ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                                Operator.GREATER,
                                                                                                ThresholdDataType.LEFT ) ) ) );

        builder.addThresholds( probabilities, ThresholdGroup.PROBABILITY );

        // Probability classifier thresholds
        Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );
        probabilityClassifiers.put( MetricConstants.PROBABILITY_OF_DETECTION,
                                    new HashSet<>( Arrays.asList( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                                                         Operator.GREATER,
                                                                                                         ThresholdDataType.LEFT ) ) ) );
        builder.addThresholds( probabilityClassifiers, ThresholdGroup.PROBABILITY_CLASSIFIER );

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
