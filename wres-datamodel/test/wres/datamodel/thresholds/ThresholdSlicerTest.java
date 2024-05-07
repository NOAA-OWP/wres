package wres.datamodel.thresholds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.types.Climatology;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link ThresholdSlicer}.
 *
 * @author James Brown
 */

class ThresholdSlicerTest
{

    private static final String CMS = "CMS";
    private static final String FLOOD = "FLOOD";
    private static final String UNIT = "bar";

    private FeatureTuple featureTuple;
    private FeatureTuple anotherFeatureTuple;

    @BeforeEach
    public void runBeforeEachTest()
    {
        Geometry a = wres.statistics.MessageFactory.getGeometry( "a" );
        Geometry b = wres.statistics.MessageFactory.getGeometry( "b" );

        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( a, b, null );
        this.featureTuple = FeatureTuple.of( geoTuple );

        Geometry c = wres.statistics.MessageFactory.getGeometry( "c" );
        Geometry d = MessageFactory.getGeometry( "d" );

        GeometryTuple anotherGeoTuple = wres.statistics.MessageFactory.getGeometryTuple( c, d, null );
        this.anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );
    }

    @Test
    void testDecompose()
    {
        ThresholdOuter oneThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                         ThresholdOperator.GREATER,
                                                         ThresholdOrientation.LEFT,
                                                         "ACTION",
                                                         MeasurementUnit.of( CMS ) );

        ThresholdOuter anotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                             ThresholdOperator.GREATER,
                                                             ThresholdOrientation.LEFT,
                                                             FLOOD,
                                                             MeasurementUnit.of( CMS ) );

        Set<ThresholdOuter> someThresholds = Set.of( oneThreshold, anotherThreshold );

        ThresholdOuter oneMoreThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                             ThresholdOperator.GREATER,
                                                             ThresholdOrientation.LEFT,
                                                             "ACTION",
                                                             MeasurementUnit.of( CMS ) );

        ThresholdOuter yetAnotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 4.0 ),
                                                                ThresholdOperator.GREATER,
                                                                ThresholdOrientation.LEFT,
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
    void testDecomposeWithDuplicates()
    {
        ThresholdOuter oneThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                         ThresholdOperator.GREATER,
                                                         ThresholdOrientation.LEFT,
                                                         "ACTION",
                                                         MeasurementUnit.of( CMS ) );

        ThresholdOuter anotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                             ThresholdOperator.GREATER,
                                                             ThresholdOrientation.LEFT,
                                                             FLOOD,
                                                             MeasurementUnit.of( CMS ) );

        Set<ThresholdOuter> someThresholds = Set.of( oneThreshold, anotherThreshold );

        ThresholdOuter oneMoreThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                             ThresholdOperator.GREATER,
                                                             ThresholdOrientation.LEFT,
                                                             "ACTION",
                                                             MeasurementUnit.of( CMS ) );

        ThresholdOuter yetAnotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 4.0 ),
                                                                ThresholdOperator.GREATER,
                                                                ThresholdOrientation.LEFT,
                                                                FLOOD,
                                                                MeasurementUnit.of( CMS ) );

        ThresholdOuter duplicateOfYetAnotherThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 5.0 ),
                                                                           ThresholdOperator.GREATER,
                                                                           ThresholdOrientation.LEFT,
                                                                           FLOOD,
                                                                           MeasurementUnit.of( CMS ) );

        Set<ThresholdOuter> someMoreThresholds = Set.of( oneMoreThreshold,
                                                         yetAnotherThreshold,
                                                         duplicateOfYetAnotherThreshold );

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

        Map<FeatureTuple, ThresholdOuter> yetAnotherMap = new HashMap<>();
        yetAnotherMap.put( this.anotherFeatureTuple, duplicateOfYetAnotherThreshold );

        expected.add( aMap );
        expected.add( anotherMap );
        expected.add( yetAnotherMap );

        assertEquals( expected, actual );
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
                                                                  7001.4, 12038.5, 17897.2 },
                                                          UNIT )
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
                                                                       ThresholdOperator.GREATER,
                                                                       ThresholdOrientation.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                       ThresholdOperator.GREATER_EQUAL,
                                                                       ThresholdOrientation.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 7.0 / 11.0 ),
                                                                       ThresholdOperator.GREATER,
                                                                       ThresholdOrientation.LEFT ),
                                ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                       ThresholdOperator.GREATER,
                                                                       ThresholdOrientation.LEFT ) ) );

        Map<FeatureTuple, Set<ThresholdOuter>> actual =
                ThresholdSlicer.addQuantiles( thresholds, climatology );

        Map<FeatureTuple, Set<ThresholdOuter>> expected =
                Map.of( oneTuple,
                        Set.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1.5 ),
                                                                    OneOrTwoDoubles.of( 0.0 ),
                                                                    ThresholdOperator.GREATER,
                                                                    ThresholdOrientation.LEFT,
                                                                    null,
                                                                    MeasurementUnit.of( UNIT ) ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 17897.2 ),
                                                                    OneOrTwoDoubles.of( 1.0 ),
                                                                    ThresholdOperator.GREATER_EQUAL,
                                                                    ThresholdOrientation.LEFT,
                                                                    null,
                                                                    MeasurementUnit.of( UNIT ) ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1647.18182 ),
                                                                    OneOrTwoDoubles.of( 7.0 / 11.0 ),
                                                                    ThresholdOperator.GREATER,
                                                                    ThresholdOrientation.LEFT,
                                                                    null,
                                                                    MeasurementUnit.of( UNIT ) ),
                                ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 433.9 ),
                                                                    OneOrTwoDoubles.of( 0.5 ),
                                                                    ThresholdOperator.GREATER,
                                                                    ThresholdOrientation.LEFT,
                                                                    null,
                                                                    MeasurementUnit.of( UNIT ) ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testCompose()
    {
        ThresholdOuter first = ThresholdOuter.of( OneOrTwoDoubles.of( 100.0 ),
                                                  ThresholdOperator.GREATER,
                                                  ThresholdOrientation.RIGHT,
                                                  FLOOD,
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter second = ThresholdOuter.of( OneOrTwoDoubles.of( 1000.0 ),
                                                   ThresholdOperator.GREATER,
                                                   ThresholdOrientation.RIGHT,
                                                   FLOOD,
                                                   MeasurementUnit.of( CMS ) );

        ThresholdOuter actual = ThresholdSlicer.compose( Set.of( first, second ) );

        ThresholdOuter expected = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NaN ),
                                                     ThresholdOperator.GREATER,
                                                     ThresholdOrientation.RIGHT,
                                                     FLOOD,
                                                     MeasurementUnit.of( CMS ) );

        assertEquals( expected, actual );

        ThresholdOuter third = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                      ThresholdOperator.GREATER,
                                                                      ThresholdOrientation.RIGHT,
                                                                      "aThreshold" );

        ThresholdOuter fourth = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                       ThresholdOperator.GREATER,
                                                                       ThresholdOrientation.RIGHT,
                                                                       "anotherThreshold" );

        ThresholdOuter actualTwo = ThresholdSlicer.compose( Set.of( third, fourth ) );

        ThresholdOuter expectedTwo = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                            ThresholdOperator.GREATER,
                                                                            ThresholdOrientation.RIGHT );

        assertEquals( expectedTwo, actualTwo );

        ThresholdOuter fifth = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                  ThresholdOperator.LESS,
                                                  ThresholdOrientation.RIGHT,
                                                  "aThreshold",
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter sixth = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                  ThresholdOperator.LESS,
                                                  ThresholdOrientation.RIGHT,
                                                  "anotherThreshold",
                                                  MeasurementUnit.of( CMS ) );

        ThresholdOuter actualThree = ThresholdSlicer.compose( Set.of( fifth, sixth ) );

        ThresholdOuter expectedThree = ThresholdOuter.of( OneOrTwoDoubles.of( 1269.0 ),
                                                          ThresholdOperator.LESS,
                                                          ThresholdOrientation.RIGHT,
                                                          MeasurementUnit.of( CMS ) );

        assertEquals( expectedThree, actualThree );

        ThresholdOuter seventh = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                     OneOrTwoDoubles.of( 0.83 ),
                                                                     ThresholdOperator.LESS,
                                                                     ThresholdOrientation.RIGHT,
                                                                     "aThreshold",
                                                                     MeasurementUnit.of( CMS ) );

        ThresholdOuter eighth = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                    OneOrTwoDoubles.of( 0.83 ),
                                                                    ThresholdOperator.LESS,
                                                                    ThresholdOrientation.RIGHT,
                                                                    "anotherThreshold",
                                                                    MeasurementUnit.of( CMS ) );

        ThresholdOuter actualFour = ThresholdSlicer.compose( Set.of( seventh, eighth ) );

        ThresholdOuter expectedFour = ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1269.0 ),
                                                                          OneOrTwoDoubles.of( 0.83 ),
                                                                          ThresholdOperator.LESS,
                                                                          ThresholdOrientation.RIGHT,
                                                                          null,
                                                                          MeasurementUnit.of( CMS ) );

        assertEquals( expectedFour, actualFour );
    }

    @Test
    void testFilterByThresholdValuesAndNames()
    {
        // Same values, different probabilities
        Set<ThresholdOuter> input = new HashSet<>();
        ThresholdOuter first = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                           .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                           .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                           .setOrientation( ThresholdOrientation.LEFT )
                                                           .build();
        ThresholdOuter second = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                            .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                            .setOrientation( ThresholdOrientation.LEFT )
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
                                                                  .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                  .setOrientation( ThresholdOrientation.LEFT )
                                                                  .build();
        ThresholdOuter anotherSecond = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                   .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                   .setUnits( MeasurementUnit.of( "OTHER_UNIT" ) )
                                                                   .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                   .setOrientation( ThresholdOrientation.LEFT )
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
                                                                     .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                     .setOrientation( ThresholdOrientation.LEFT )
                                                                     .setLabel( "name" )
                                                                     .build();
        ThresholdOuter yetAnotherSecond = new ThresholdOuter.Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                                      .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                      .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                      .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                      .setOrientation( ThresholdOrientation.LEFT )
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
                                                                  .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                  .setOrientation( ThresholdOrientation.LEFT )
                                                                  .build();
        ThresholdOuter oneMoreSecond = new ThresholdOuter.Builder().setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                                                   .setUnits( MeasurementUnit.of( "UNIT" ) )
                                                                   .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                                   .setOrientation( ThresholdOrientation.LEFT )
                                                                   .build();

        oneMoreInput.add( oneMoreFirst );
        oneMoreInput.add( oneMoreSecond );

        Set<ThresholdOuter> oneMoreActual = ThresholdSlicer.filter( Collections.unmodifiableSet( oneMoreInput ) );
        Set<ThresholdOuter> oneMoreExpected = Set.of( oneMoreFirst, oneMoreSecond );

        assertEquals( oneMoreExpected, oneMoreActual );
    }

    @Test
    void testGetOneOrTwoThresholds()
    {
        Pair<Set<MetricConstants>, Set<ThresholdOuter>> container = this.getMetricsAndThresholds();

        SortedSet<OneOrTwoThresholds> expectedThresholds = new TreeSet<>();

        ThresholdOuter thresholdOne = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                             ThresholdOperator.GREATER,
                                                                             ThresholdOrientation.LEFT );

        ThresholdOuter thresholdTwo = ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                             ThresholdOperator.GREATER,
                                                                             ThresholdOrientation.LEFT );

        ThresholdOuter thresholdThree =
                new ThresholdOuter.Builder().setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                            .setOperator( ThresholdOperator.GREATER )
                                            .setOrientation( ThresholdOrientation.LEFT )
                                            .setThresholdType( ThresholdType.PROBABILITY_CLASSIFIER )
                                            .build();

        expectedThresholds.add( OneOrTwoThresholds.of( thresholdOne ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdTwo ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdOne, thresholdThree ) );
        expectedThresholds.add( OneOrTwoThresholds.of( thresholdTwo, thresholdThree ) );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual =
                ThresholdSlicer.getOneOrTwoThresholds( container.getLeft(), container.getRight() );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> expected =
                Map.of( MetricConstants.PROBABILITY_OF_DETECTION, expectedThresholds );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMetricsAndThresholdsForProcessing()
    {
        Geometry oneLeft = Geometry.newBuilder()
                                   .setName( "foo" )
                                   .build();
        Geometry twoLeft = Geometry.newBuilder()
                                   .setName( UNIT )
                                   .build();
        Geometry oneRight = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();
        Geometry twoRight = Geometry.newBuilder()
                                    .setName( "qux" )
                                    .build();

        wres.config.yaml.components.Threshold thresholdOne
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 23.0 )
                                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                                       .build() )
                                  .type( ThresholdType.VALUE )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( oneLeft )
                                  .build();
        wres.config.yaml.components.Threshold thresholdTwo
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 25.0 )
                                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                                       .build() )
                                  .type( ThresholdType.VALUE )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( twoLeft )
                                  .build();

        wres.config.yaml.components.Threshold thresholdThree
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 0.3 )
                                                       .setOperator( Threshold.ThresholdOperator.LESS )
                                                       .build() )
                                  .type( ThresholdType.PROBABILITY )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( oneLeft )
                                  .build();
        wres.config.yaml.components.Threshold thresholdFour
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 0.5 )
                                                       .setOperator( Threshold.ThresholdOperator.LESS )
                                                       .build() )
                                  .type( ThresholdType.PROBABILITY )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( twoLeft )
                                  .build();

        Metric one = MetricBuilder.Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( Set.of( thresholdOne ) )
                                                                  .probabilityThresholds( Set.of( thresholdThree ) )
                                                                  .build() );
        Metric two = MetricBuilder.Metric( MetricConstants.MEAN_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( Set.of( thresholdOne ) )
                                                                  .probabilityThresholds( Set.of( thresholdThree ) )
                                                                  .build() );

        Metric three = MetricBuilder.Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                             MetricParametersBuilder.builder()
                                                                    .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                    .thresholds( Set.of( thresholdTwo ) )
                                                                    .probabilityThresholds( Set.of( thresholdFour ) )
                                                                    .build() );
        Metric four = MetricBuilder.Metric( MetricConstants.MEAN_SQUARE_ERROR,
                                            MetricParametersBuilder.builder()
                                                                   .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                   .thresholds( Set.of( thresholdTwo ) )
                                                                   .probabilityThresholds( Set.of( thresholdFour ) )
                                                                   .build() );

        Set<Metric> metrics = Set.of( one, two, three, four );

        Dataset dataset = DatasetBuilder.builder()
                                        .build();

        GeometryTuple singletonOne = GeometryTuple.newBuilder()
                                                  .setLeft( oneLeft )
                                                  .setRight( oneRight )
                                                  .build();
        GeometryTuple singletonTwo = GeometryTuple.newBuilder()
                                                  .setLeft( twoLeft )
                                                  .setRight( twoRight )
                                                  .build();

        Features features = new Features( Set.of( singletonOne, singletonTwo ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( dataset )
                                                                       .right( dataset )
                                                                       .metrics( metrics )
                                                                       .features( features )
                                                                       .build();

        Set<MetricsAndThresholds> actual = ThresholdSlicer.getMetricsAndThresholdsForProcessing( evaluation );

        FeatureTuple tupleOne = FeatureTuple.of( singletonOne );
        FeatureTuple tupleTwo = FeatureTuple.of( singletonTwo );

        ThresholdOuter expectedThresholdOne = ThresholdOuter.of( thresholdOne.threshold(), ThresholdType.VALUE );
        ThresholdOuter expectedThresholdTwo = ThresholdOuter.of( thresholdTwo.threshold(), ThresholdType.VALUE );
        ThresholdOuter expectedThresholdThree = ThresholdOuter.of( thresholdThree.threshold(),
                                                                   ThresholdType.PROBABILITY );
        ThresholdOuter expectedThresholdFour = ThresholdOuter.of( thresholdFour.threshold(),
                                                                  ThresholdType.PROBABILITY );

        Map<FeatureTuple, Set<ThresholdOuter>> expectedThresholdsOne = Map.of( tupleOne,
                                                                               Set.of( expectedThresholdOne,
                                                                                       expectedThresholdThree ) );
        Map<FeatureTuple, Set<ThresholdOuter>> expectedThresholdsTwo = Map.of( tupleTwo,
                                                                               Set.of( expectedThresholdTwo,
                                                                                       expectedThresholdFour ) );


        MetricsAndThresholds firstExpected = new MetricsAndThresholds( Set.of( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                               MetricConstants.MEAN_ERROR ),
                                                                       expectedThresholdsOne,
                                                                       0,
                                                                       Pool.EnsembleAverageType.MEDIAN );
        MetricsAndThresholds secondExpected =
                new MetricsAndThresholds( Set.of( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                  MetricConstants.MEAN_SQUARE_ERROR ),
                                          expectedThresholdsTwo,
                                          0,
                                          Pool.EnsembleAverageType.MEAN );

        Set<MetricsAndThresholds> expected = Set.of( firstExpected, secondExpected );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMetricsAndThresholdsForProcessingWithMultipleFeaturesPerLeftFeature()
    {
        Geometry oneLeft = Geometry.newBuilder()
                                   .setName( "foo" )
                                   .build();
        Geometry twoLeft = Geometry.newBuilder()
                                   .setName( "foo" )
                                   .build();
        Geometry oneRight = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();
        Geometry twoRight = Geometry.newBuilder()
                                    .setName( "qux" )
                                    .build();

        wres.config.yaml.components.Threshold thresholdOne
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 23.0 )
                                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                                       .build() )
                                  .type( ThresholdType.VALUE )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( oneLeft )
                                  .build();
        wres.config.yaml.components.Threshold thresholdTwo
                = ThresholdBuilder.builder()
                                  .threshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( 0.2 )
                                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                                       .build() )
                                  .type( ThresholdType.PROBABILITY )
                                  .featureNameFrom( DatasetOrientation.LEFT )
                                  .feature( twoLeft )
                                  .build();

        Metric one = MetricBuilder.Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( Set.of( thresholdOne ) )
                                                                  .probabilityThresholds( Set.of( thresholdTwo ) )
                                                                  .build() );

        Set<Metric> metrics = Set.of( one );

        Dataset dataset = DatasetBuilder.builder()
                                        .build();

        GeometryTuple singletonOne = GeometryTuple.newBuilder()
                                                  .setLeft( oneLeft )
                                                  .setRight( oneRight )
                                                  .build();
        GeometryTuple singletonTwo = GeometryTuple.newBuilder()
                                                  .setLeft( twoLeft )
                                                  .setRight( twoRight )
                                                  .build();

        Features features = new Features( Set.of( singletonOne, singletonTwo ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( dataset )
                                                                       .right( dataset )
                                                                       .metrics( metrics )
                                                                       .features( features )
                                                                       .build();

        Set<MetricsAndThresholds> actual = ThresholdSlicer.getMetricsAndThresholdsForProcessing( evaluation );

        FeatureTuple tupleOne = FeatureTuple.of( singletonOne );
        FeatureTuple tupleTwo = FeatureTuple.of( singletonTwo );

        ThresholdOuter expectedThresholdOne = ThresholdOuter.of( thresholdOne.threshold(), ThresholdType.VALUE );
        ThresholdOuter expectedThresholdTwo = ThresholdOuter.of( thresholdTwo.threshold(), ThresholdType.PROBABILITY );

        Map<FeatureTuple, Set<ThresholdOuter>> expectedThresholds = Map.of( tupleOne,
                                                                            Set.of( expectedThresholdOne,
                                                                                    expectedThresholdTwo ),
                                                                            tupleTwo,
                                                                            Set.of( expectedThresholdOne,
                                                                                    expectedThresholdTwo ) );


        MetricsAndThresholds firstExpected = new MetricsAndThresholds( Set.of( MetricConstants.MEAN_ABSOLUTE_ERROR ),
                                                                       expectedThresholds,
                                                                       0,
                                                                       Pool.EnsembleAverageType.MEDIAN );

        Set<MetricsAndThresholds> expected = Set.of( firstExpected );

        assertEquals( expected, actual );
    }

    /**
     * Returns a default container for testing.
     *
     * @return a default container
     */

    private Pair<Set<MetricConstants>, Set<ThresholdOuter>> getMetricsAndThresholds()
    {
        // Probability thresholds
        Set<ThresholdOuter> thresholds = new TreeSet<>();

        thresholds.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                               ThresholdOperator.GREATER,
                                                               ThresholdOrientation.LEFT ) );
        thresholds.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                               ThresholdOperator.GREATER,
                                                               ThresholdOrientation.LEFT ) );

        // Probability classifier thresholds
        ThresholdOuter classifier = new ThresholdOuter.Builder()
                .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                .setOperator( ThresholdOperator.GREATER )
                .setOrientation( ThresholdOrientation.LEFT )
                .setThresholdType( ThresholdType.PROBABILITY_CLASSIFIER )
                .build();

        thresholds.add( classifier );

        return Pair.of( Set.of( MetricConstants.PROBABILITY_OF_DETECTION ), Collections.unmodifiableSet( thresholds ) );
    }

}
