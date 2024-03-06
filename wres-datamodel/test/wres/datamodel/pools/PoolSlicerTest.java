package wres.datamodel.pools;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.types.Ensemble;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.types.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link PoolSlicer}.
 *
 * @author James Brown
 */

class PoolSlicerTest
{

    private static final String HEFS = "HEFS";
    private static final String SQIN = "SQIN";
    private static final String DRRC3 = "DRRC3";
    private static final String DRRC2 = "DRRC2";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";
    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";

    @Test
    void testFilterSingleValuedPairsByLeft()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        double[] expected = new double[] { 1, 1, 1 };
        ThresholdOuter threshold = ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.LEFT );
        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Double>> pairs = Pool.of( values, meta, values, PoolMetadata.of( true ), null );
        Pool<Pair<Double, Double>> sliced =
                PoolSlicer.filter( pairs, Slicer.left( threshold ), threshold );
        //Test with baseline
        assertArrayEquals( Slicer.getLeftSide( sliced.getBaselineData() ), expected );
        //Test without baseline
        Pool<Pair<Double, Double>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Double>> slicedNoBase =
                PoolSlicer.filter( pairsNoBase, Slicer.left( threshold ), threshold );
        assertArrayEquals( Slicer.getLeftSide( slicedNoBase ), expected );
    }

    @Test
    void testFilterEnsemblePairsByLeft()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        double[] expected = new double[] { 1, 1, 1 };
        ThresholdOuter threshold = ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.LEFT );
        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Ensemble>> pairs = Pool.of( values, meta, values, PoolMetadata.of( true ), null );
        Pool<Pair<Double, Ensemble>> sliced =
                PoolSlicer.filter( pairs, Slicer.leftVector( threshold ), threshold );

        //Test with baseline
        assertArrayEquals( Slicer.getLeftSide( sliced.getBaselineData() ), expected );

        //Test without baseline
        Pool<Pair<Double, Ensemble>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Ensemble>> slicedNoBase =
                PoolSlicer.filter( pairsNoBase, Slicer.leftVector( threshold ), threshold );

        assertArrayEquals( Slicer.getLeftSide( slicedNoBase ), expected );
    }

    @Test
    void testTransformEnsemblePairsToSingleValuedPairs()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 6, 7, 8, 9, 10 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 11, 12, 13, 14, 15 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 16, 17, 18, 19, 20 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 21, 22, 23, 24, 25 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 26, 27, 28, 29, 30 ) ) );
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( values, PoolMetadata.of(), values, PoolMetadata.of( true ), null );
        Function<Pair<Double, Ensemble>, Pair<Double, Double>> mapper =
                in -> Pair.of( in.getLeft(),
                               Arrays.stream( in.getRight().getMembers() )
                                     .average()
                                     .orElseThrow() );
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                Slicer.getRightSide( PoolSlicer.transform( Pool.of( values, PoolMetadata.of() ), mapper ) );
        assertArrayEquals( actualNoBase, expected );
        //Test baseline
        double[] actualBase = Slicer.getRightSide( PoolSlicer.transform( input, mapper ).getBaselineData() );
        assertArrayEquals( actualBase, expected );
    }

    @Test
    void testTransformSingleValuedPairsToDichotomousPairs()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        PoolMetadata meta = PoolMetadata.of();
        Function<Pair<Double, Double>, Pair<Boolean, Boolean>> mapper =
                in -> Pair.of( in.getLeft() > 0, in.getRight() > 0 );
        final List<Pair<Boolean, Boolean>> expectedValues = new ArrayList<>();
        expectedValues.add( Pair.of( false, true ) );
        expectedValues.add( Pair.of( false, true ) );
        expectedValues.add( Pair.of( true, true ) );
        expectedValues.add( Pair.of( true, true ) );
        expectedValues.add( Pair.of( false, false ) );
        expectedValues.add( Pair.of( true, true ) );

        Pool<Pair<Boolean, Boolean>> expectedNoBase = Pool.of( expectedValues, meta );
        Pool<Pair<Boolean, Boolean>> expectedBase = Pool.of( expectedValues,
                                                             meta,
                                                             expectedValues,
                                                             PoolMetadata.of( true ),
                                                             null );

        //Test without baseline
        Pool<Pair<Boolean, Boolean>> actualNoBase =
                PoolSlicer.transform( Pool.of( values, meta ), mapper );
        assertEquals( expectedNoBase.get(), actualNoBase.get() );

        //Test baseline
        Pool<Pair<Boolean, Boolean>> actualBase =
                PoolSlicer.transform( Pool.of( values, meta, values, PoolMetadata.of( true ), null ),
                                      mapper );
        assertEquals( expectedBase.getBaselineData().get(), actualBase.getBaselineData().get() );
    }

    @Test
    void testTransformEnsemblePairsToDiscreteProbabilityPairs()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 2, 3, 3 ) ) );
        values.add( Pair.of( 3.0, Ensemble.of( 3, 3, 3, 3, 3 ) ) );
        values.add( Pair.of( 4.0, Ensemble.of( 4, 4, 4, 4, 4 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 5.0, Ensemble.of( 1, 1, 6, 6, 50 ) ) );
        PoolMetadata meta = PoolMetadata.of();
        ThresholdOuter threshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.LEFT );

        List<Pair<Probability, Probability>> expectedPairs = new ArrayList<>();
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) );

        //Test without baseline
        Pool<Pair<Double, Ensemble>> pairs = Pool.of( values, meta );

        Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> mapper =
                pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        Pool<Pair<Probability, Probability>> sliced =
                PoolSlicer.transform( pairs, mapper );

        assertEquals( expectedPairs, sliced.get() );

        //Test baseline
        Pool<Pair<Probability, Probability>> slicedWithBaseline =
                PoolSlicer.transform( Pool.of( values, meta, values, PoolMetadata.of( true ), null ), mapper );
        assertEquals( expectedPairs, slicedWithBaseline.get() );
        assertEquals( expectedPairs, slicedWithBaseline.getBaselineData().get() );
    }

    @Test
    void testGetPairCount()
    {
        Pool<TimeSeries<Boolean>> pool = Pool.of( List.of(), PoolMetadata.of() );

        assertEquals( 0, PoolSlicer.getEventCount( pool ) );


        SortedSet<Event<Boolean>> eventsOne = new TreeSet<>();
        eventsOne.add( Event.of( Instant.MIN, Boolean.FALSE ) );
        SortedSet<Event<Boolean>> eventsTwo = new TreeSet<>();
        eventsTwo.add( Event.of( Instant.MIN, Boolean.FALSE ) );
        eventsTwo.add( Event.of( Instant.MAX, Boolean.TRUE ) );

        Pool<TimeSeries<Boolean>> anotherPool =
                Pool.of( List.of( TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "foo",
                                                                        Feature.of(
                                                                                wres.statistics.MessageFactory.getGeometry(
                                                                                        "bar" ) ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        Feature.of(
                                                                                wres.statistics.MessageFactory.getGeometry(
                                                                                        "smeg" ) ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        assertEquals( 3, PoolSlicer.getEventCount( anotherPool ) );
    }

    @Test
    void testUnpack()
    {
        SortedSet<Event<String>> eventsOne = new TreeSet<>();
        eventsOne.add( Event.of( Instant.MIN, "Un" ) );
        SortedSet<Event<String>> eventsTwo = new TreeSet<>();
        eventsTwo.add( Event.of( Instant.MIN, "pack" ) );
        eventsTwo.add( Event.of( Instant.MAX, "ed!" ) );

        Pool<TimeSeries<String>> pool =
                Pool.of( List.of( TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "foo",
                                                                        Feature.of( wres.statistics.MessageFactory.getGeometry(
                                                                                "bar" ) ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        Feature.of( wres.statistics.MessageFactory.getGeometry(
                                                                                "smeg" ) ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        Pool<String> expected = Pool.of( List.of( "Un", "pack", "ed!" ), PoolMetadata.of() );

        assertEquals( expected, PoolSlicer.unpack( pool ) );
    }

    @Test
    void testDecompose()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( "unit" )
                                          .build();

        wres.statistics.generated.Pool.Builder builder = wres.statistics.generated.Pool.newBuilder();

        builder.setGeometryGroup( GeometryGroup.newBuilder()
                                               .addGeometryTuples( GeometryTuple.newBuilder()
                                                                                .setLeft( Geometry.newBuilder()
                                                                                                  .setName( "foo" ) )
                                                                                .setRight( Geometry.newBuilder()
                                                                                                   .setName( "bar" ) ) ) );

        Pool<String> aPool = new Builder<String>().addData( List.of( "a", "b", "c" ) )
                                                  .setMetadata( PoolMetadata.of( evaluation,
                                                                                 builder.build() ) )
                                                  .build();

        wres.statistics.generated.Pool.Builder anotherBuilder = wres.statistics.generated.Pool.newBuilder();

        anotherBuilder.setGeometryGroup( GeometryGroup.newBuilder()
                                                      .addGeometryTuples( GeometryTuple.newBuilder()
                                                                                       .setLeft( Geometry.newBuilder()
                                                                                                         .setName( "baz" ) )
                                                                                       .setRight( Geometry.newBuilder()
                                                                                                          .setName(
                                                                                                                  "qux" ) ) ) );

        Pool<String> anotherPool = new Builder<String>().addData( List.of( "d", "e", "f" ) )
                                                        .setMetadata( PoolMetadata.of( evaluation,
                                                                                       anotherBuilder.build() ) )
                                                        .build();

        Pool<String> merged = new Builder<String>().addPool( aPool )
                                                   .addPool( anotherPool )
                                                   .build();

        Map<FeatureTuple, Pool<String>> actual =
                PoolSlicer.decompose( merged, next -> next.getFeatureTuples().iterator().next() );

        assertEquals( 2, actual.size() );

        Map<FeatureTuple, Pool<String>> expected = new HashMap<>();

        Geometry one = wres.statistics.MessageFactory.getGeometry( "foo" );
        Geometry two = wres.statistics.MessageFactory.getGeometry( "bar" );
        Geometry three = wres.statistics.MessageFactory.getGeometry( "baz" );
        Geometry four = wres.statistics.MessageFactory.getGeometry( "qux" );
        GeometryTuple geoTupleOne = wres.statistics.MessageFactory.getGeometryTuple( one, two, null );
        GeometryTuple geoTupleTwo = wres.statistics.MessageFactory.getGeometryTuple( three, four, null );
        FeatureTuple featureTupleOne = FeatureTuple.of( geoTupleOne );
        FeatureTuple featureTupleTwo = FeatureTuple.of( geoTupleTwo );

        expected.put( featureTupleOne, aPool );
        expected.put( featureTupleTwo, anotherPool );

        assertEquals( expected, actual );
    }

    @Test
    void testUnionOf()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Geometry geo = wres.statistics.MessageFactory.getGeometry( DRRC2 );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geo, geo, geo );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool poolOne =
                MessageFactory.getPool( featureGroup,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  FIRST_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  "1985-12-31T23:59:59Z" ) ) ),
                                        null,
                                        null,
                                        false,
                                        1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, poolOne );

        wres.statistics.generated.Pool poolTwo =
                MessageFactory.getPool( featureGroup,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  SECOND_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  "1986-12-31T23:59:59Z" ) ) ),
                                        null,
                                        null,
                                        false,
                                        1 );

        PoolMetadata m2 = PoolMetadata.of( evaluation, poolTwo );

        wres.statistics.generated.Pool poolThree =
                MessageFactory.getPool( featureGroup,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  "1987-01-01T00:00:00Z" ),
                                                                                                          Instant.parse(
                                                                                                                  "1988-01-01T00:00:00Z" ) ) ),
                                        null,
                                        null,
                                        false,
                                        1 );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolThree );

        wres.statistics.generated.Pool poolFour =
                MessageFactory.getPool( featureGroup,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  FIRST_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  "1988-01-01T00:00:00Z" ) ) ),
                                        null,
                                        null,
                                        false,
                                        1 );


        PoolMetadata benchmark = PoolMetadata.of( evaluation, poolFour );

        assertEquals( benchmark, PoolSlicer.unionOf( Arrays.asList( m1, m2, m3 ) ) );
    }

    @Test
    void testUnionOfThrowsExceptionWithNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> PoolSlicer.unionOf( null ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    @Test
    void testUnionOfThrowsExceptionWithEmptyInput()
    {
        List<PoolMetadata> input = List.of();
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> PoolSlicer.unionOf( input ) );

        assertEquals( "Cannot find the union of empty input.", actual.getMessage() );
    }

    @Test
    void testUnionOfThrowsExceptionWithOneNullInput()
    {
        List<PoolMetadata> nullInput = Collections.singletonList( null );
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> PoolSlicer.unionOf( nullInput ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    @Test
    void testUnionOfThrowsExceptionWithOneNullAndOneValidInput()
    {
        List<PoolMetadata> oneNull = Arrays.asList( PoolMetadata.of(), null );

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> PoolSlicer.unionOf( oneNull ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    @Test
    void testUnionOfThrowsExceptionWithUnequalInputs()
    {
        Geometry geo = wres.statistics.MessageFactory.getGeometry( DRRC3 );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geo, geo, geo );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        wres.statistics.generated.Pool poolOne =
                MessageFactory.getPool( featureGroup,
                                        null,
                                        TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                        null,
                                        false,
                                        1 );

        PoolMetadata failOne = PoolMetadata.of( evaluation, poolOne );

        wres.statistics.generated.Pool poolTwo = MessageFactory.getPool( featureGroup,
                                                                         null,
                                                                         TimeScaleOuter.of(),
                                                                         null,
                                                                         false,
                                                                         1 );

        PoolMetadata failTwo = PoolMetadata.of( evaluation, poolTwo );

        List<PoolMetadata> list = List.of( failOne, failTwo );
        PoolMetadataException actual = assertThrows( PoolMetadataException.class,
                                                     () -> PoolSlicer.unionOf( list ) );

        assertTrue( Objects.nonNull( actual.getMessage() ) && actual.getMessage()
                                                                    .startsWith( "Only the time window and thresholds "
                                                                                 + "and features can differ when "
                                                                                 + "finding the union of metadata." ) );
    }

    @Test
    void testUnionOfThrowsExceptionWithUnequalEvaluations()
    {
        // Different evaluations
        Evaluation evaluationOne = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        PoolMetadata poolMetaOne =
                PoolMetadata.of( evaluationOne, wres.statistics.generated.Pool.getDefaultInstance() );
        PoolMetadata poolMetaTwo =
                PoolMetadata.of( evaluationTwo, wres.statistics.generated.Pool.getDefaultInstance() );

        List<PoolMetadata> pools = List.of( poolMetaOne, poolMetaTwo );

        PoolMetadataException actual = assertThrows( PoolMetadataException.class,
                                                     () -> PoolSlicer.unionOf( pools ) );

        assertTrue( actual.getMessage().contains( "Only the time window and thresholds and features can differ" ) );
    }

    @Test
    void testUnionOfWithDifferentThresholdsTimeWindowsAndFeatures()
    {
        // Differ on all of thresholds, time window and features, but nothing else
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Geometry one = wres.statistics.MessageFactory.getGeometry( "a" );
        Geometry two = wres.statistics.MessageFactory.getGeometry( "b" );
        Geometry three = wres.statistics.MessageFactory.getGeometry( "c" );
        Geometry four = wres.statistics.MessageFactory.getGeometry( "d" );
        Geometry five = wres.statistics.MessageFactory.getGeometry( "e" );
        Geometry six = wres.statistics.MessageFactory.getGeometry( "f" );

        GeometryTuple geoTupleOne = wres.statistics.MessageFactory.getGeometryTuple( one, two, three );
        GeometryTuple geoTupleTwo = wres.statistics.MessageFactory.getGeometryTuple( four, five, six );
        FeatureTuple featureTupleOne = FeatureTuple.of( geoTupleOne );
        FeatureTuple featureTupleTwo = FeatureTuple.of( geoTupleTwo );
        FeatureGroup featureGroupOne = FeatureGroup.of( MessageFactory.getGeometryGroup( null, featureTupleOne ) );
        FeatureGroup featureGroupTwo = FeatureGroup.of( MessageFactory.getGeometryGroup( null, featureTupleTwo ) );

        wres.statistics.generated.Pool poolOne =
                MessageFactory.getPool( featureGroupOne,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  FIRST_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  SECOND_TIME ) ) ),
                                        TimeScaleOuter.of(),
                                        OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  ThresholdOperator.GREATER,
                                                                                  ThresholdOrientation.LEFT ) ),
                                        false,
                                        1 );

        wres.statistics.generated.Pool poolTwo =
                MessageFactory.getPool( featureGroupTwo,
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  FIRST_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  FIRST_TIME ) ) ),
                                        TimeScaleOuter.of(),
                                        OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                                                  ThresholdOperator.GREATER,
                                                                                  ThresholdOrientation.LEFT ) ),
                                        false,
                                        1 );

        PoolMetadata poolMetaOne = PoolMetadata.of( evaluation, poolOne );
        PoolMetadata poolMetaTwo = PoolMetadata.of( evaluation, poolTwo );

        List<PoolMetadata> pools = List.of( poolMetaOne, poolMetaTwo );

        PoolMetadata actual = PoolSlicer.unionOf( pools );

        wres.statistics.generated.Pool expectedPool =
                MessageFactory.getPool( FeatureGroup.of( MessageFactory.getGeometryGroup( Set.of( featureTupleOne,
                                                                                                  featureTupleTwo ) ) ),
                                        TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( Instant.parse(
                                                                                                                  FIRST_TIME ),
                                                                                                          Instant.parse(
                                                                                                                  SECOND_TIME ) ) ),
                                        TimeScaleOuter.of(),
                                        null,
                                        false,
                                        1 );

        PoolMetadata expected = PoolMetadata.of( evaluation, expectedPool );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterMultipleFeaturesByThreshold()
    {

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( "unit" )
                                          .build();

        wres.statistics.generated.Pool.Builder builder = wres.statistics.generated.Pool.newBuilder();
        GeometryTuple geoTupleOne = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder().setName( "baz" ) )
                                                 .setRight( Geometry.newBuilder().setName( "qux" ) )
                                                 .build();
        builder.setGeometryGroup( GeometryGroup.newBuilder().addGeometryTuples( geoTupleOne ) );

        Pool<Pair<Double, Double>> aPool =
                new Builder<Pair<Double, Double>>().addData( Pair.of( 1.0, 2.0 ) )
                                                   .addData( Pair.of( 3.0, 4.0 ) )
                                                   .addData( Pair.of( 5.0, 6.0 ) )
                                                   .setMetadata( PoolMetadata.of( evaluation,
                                                                                  builder.build() ) )
                                                   .build();

        wres.statistics.generated.Pool.Builder anotherBuilder = wres.statistics.generated.Pool.newBuilder();
        GeometryTuple geoTupleTwo = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder().setName( "foo" ) )
                                                 .setRight( Geometry.newBuilder().setName( "bar" ) )
                                                 .build();
        anotherBuilder.setGeometryGroup( GeometryGroup.newBuilder().addGeometryTuples( geoTupleTwo ) );

        Pool<Pair<Double, Double>> anotherPool =
                new Builder<Pair<Double, Double>>().addData( Pair.of( 7.0, 8.0 ) )
                                                   .addData( Pair.of( 9.0, 10.0 ) )
                                                   .addData( Pair.of( 11.0, 12.0 ) )
                                                   .setMetadata( PoolMetadata.of( evaluation,
                                                                                  anotherBuilder.build() ) )
                                                   .build();

        Pool<Pair<Double, Double>> merged = new Builder<Pair<Double, Double>>().addPool( aPool )
                                                                               .addPool( anotherPool )
                                                                               .build();

        Map<FeatureTuple, Predicate<Pair<Double, Double>>> predicates = new HashMap<>();
        ThresholdOuter thresholdOne = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                         ThresholdOperator.GREATER_EQUAL,
                                                         ThresholdOrientation.LEFT );
        ThresholdOuter thresholdTwo = ThresholdOuter.of( OneOrTwoDoubles.of( 9.0 ),
                                                         ThresholdOperator.GREATER_EQUAL,
                                                         ThresholdOrientation.LEFT );
        Predicate<Pair<Double, Double>> predicateOne = Slicer.left( thresholdOne );
        Predicate<Pair<Double, Double>> predicateTwo = Slicer.left( thresholdTwo );


        predicates.put( MessageFactory.parse( geoTupleOne ), predicateOne );
        predicates.put( MessageFactory.parse( geoTupleTwo ), predicateTwo );

        Map<FeatureTuple, Pool<Pair<Double, Double>>> pools = PoolSlicer.decompose( merged,
                                                                                    PoolSlicer.getFeatureMapper() );

        Pool<Pair<Double, Double>> actual = PoolSlicer.filter( pools,
                                                               predicates,
                                                               merged.getMetadata(),
                                                               null,
                                                               meta -> meta );

        wres.statistics.generated.Pool expectedPool =
                wres.statistics.generated.Pool.newBuilder()
                                              .setGeometryGroup( GeometryGroup.newBuilder()
                                                                              .addGeometryTuples( geoTupleOne )
                                                                              .addGeometryTuples( geoTupleTwo ) )
                                              .addGeometryTuples( geoTupleOne )
                                              .addGeometryTuples( geoTupleTwo )
                                              .build();

        // Note the order, which is the same order as the comparable key used to decompose the pool, in this case a 
        // FeatureTuple
        Pool<Pair<Double, Double>> expected =
                new Builder<Pair<Double, Double>>().addData( Pair.of( 3.0, 4.0 ) )
                                                   .addData( Pair.of( 5.0, 6.0 ) )
                                                   .addData( Pair.of( 9.0, 10.0 ) )
                                                   .addData( Pair.of( 11.0, 12.0 ) )
                                                   .setMetadata( PoolMetadata.of( evaluation,
                                                                                  expectedPool ) )
                                                   .build();

        assertEquals( expected, actual );
    }

    @Test
    void testFilterEmptyPool()
    {
        Map<String, Pool<Pair<Double, Double>>> pools = Map.of();
        Pool<Pair<Double, Double>> actual = PoolSlicer.filter( pools,
                                                               Map.of(),
                                                               PoolMetadata.of(),
                                                               null,
                                                               meta -> meta );

        Pool<Pair<Double, Double>> expected = Pool.of( List.of(), PoolMetadata.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testTransformMultipleFeaturesByThreshold()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( "unit" )
                                          .build();

        wres.statistics.generated.Pool.Builder builder = wres.statistics.generated.Pool.newBuilder();
        GeometryTuple geoTupleOne = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder().setName( "baz" ) )
                                                 .setRight( Geometry.newBuilder().setName( "qux" ) )
                                                 .build();
        builder.setGeometryGroup( GeometryGroup.newBuilder().addGeometryTuples( geoTupleOne ) );

        Pool<Pair<Double, Double>> aPool =
                new Builder<Pair<Double, Double>>().addData( Pair.of( 1.0, 2.0 ) )
                                                   .addData( Pair.of( 3.0, 4.0 ) )
                                                   .addData( Pair.of( 5.0, 6.0 ) )
                                                   .setMetadata( PoolMetadata.of( evaluation,
                                                                                  builder.build() ) )
                                                   .build();

        wres.statistics.generated.Pool.Builder anotherBuilder = wres.statistics.generated.Pool.newBuilder();
        GeometryTuple geoTupleTwo = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder().setName( "foo" ) )
                                                 .setRight( Geometry.newBuilder().setName( "bar" ) )
                                                 .build();
        anotherBuilder.setGeometryGroup( GeometryGroup.newBuilder().addGeometryTuples( geoTupleTwo ) );

        Pool<Pair<Double, Double>> anotherPool =
                new Builder<Pair<Double, Double>>().addData( Pair.of( 7.0, 8.0 ) )
                                                   .addData( Pair.of( 9.0, 10.0 ) )
                                                   .addData( Pair.of( 11.0, 12.0 ) )
                                                   .setMetadata( PoolMetadata.of( evaluation,
                                                                                  anotherBuilder.build() ) )
                                                   .build();

        Pool<Pair<Double, Double>> merged = new Builder<Pair<Double, Double>>().addPool( aPool )
                                                                               .addPool( anotherPool )
                                                                               .build();

        Map<FeatureTuple, Function<Pair<Double, Double>, Pair<Boolean, Boolean>>> transformers = new HashMap<>();

        Function<Pair<Double, Double>, Pair<Boolean, Boolean>> transformerOne =
                pair -> Pair.of( pair.getLeft() > 3.0, pair.getRight() > 3.0 );

        Function<Pair<Double, Double>, Pair<Boolean, Boolean>> transformerTwo =
                pair -> Pair.of( pair.getLeft() > 9.0, pair.getRight() > 9.0 );

        transformers.put( MessageFactory.parse( geoTupleOne ), transformerOne );
        transformers.put( MessageFactory.parse( geoTupleTwo ), transformerTwo );

        Map<FeatureTuple, Pool<Pair<Double, Double>>> pools = PoolSlicer.decompose( merged,
                                                                                    PoolSlicer.getFeatureMapper() );
        Pool<Pair<Boolean, Boolean>> actual =
                PoolSlicer.transform( pools, transformers, merged.getMetadata(), null, meta -> meta );

        wres.statistics.generated.Pool expectedPool = wres.statistics.generated.Pool.newBuilder()
                                                                                    .setGeometryGroup( GeometryGroup.newBuilder()
                                                                                                                    .addGeometryTuples(
                                                                                                                            geoTupleOne )
                                                                                                                    .addGeometryTuples(
                                                                                                                            geoTupleTwo ) )
                                                                                    .addGeometryTuples( geoTupleOne )
                                                                                    .addGeometryTuples( geoTupleTwo )
                                                                                    .build();

        // Note the order, which is the same order as the comparable key used to decompose the pool, in this case a 
        // FeatureTuple
        Pool<Pair<Boolean, Boolean>> expected =
                new Builder<Pair<Boolean, Boolean>>().addData( Pair.of( false, false ) )
                                                     .addData( Pair.of( false, true ) )
                                                     .addData( Pair.of( true, true ) )
                                                     .addData( Pair.of( false, false ) )
                                                     .addData( Pair.of( false, true ) )
                                                     .addData( Pair.of( true, true ) )
                                                     .setMetadata( PoolMetadata.of( evaluation,
                                                                                    expectedPool ) )
                                                     .build();

        assertEquals( expected, actual );
    }

}
