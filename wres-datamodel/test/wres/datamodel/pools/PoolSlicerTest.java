package wres.datamodel.pools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;

/**
 * Tests the {@link PoolSlicer}.
 * 
 * @author James Brown
 */

class PoolSlicerTest
{

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
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT );
        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Double>> pairs = Pool.of( values, meta, values, meta, null );
        Pool<Pair<Double, Double>> sliced =
                PoolSlicer.filter( pairs, Slicer.left( threshold::test ), threshold::test );
        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        Pool<Pair<Double, Double>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Double>> slicedNoBase =
                PoolSlicer.filter( pairsNoBase, Slicer.left( threshold::test ), threshold::test );
        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
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
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT );
        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Ensemble>> pairs = Pool.of( values, meta, values, meta, null );
        Pool<Pair<Double, Ensemble>> sliced =
                PoolSlicer.filter( pairs, Slicer.leftVector( threshold::test ), threshold::test );

        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );

        //Test without baseline
        Pool<Pair<Double, Ensemble>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Ensemble>> slicedNoBase =
                PoolSlicer.filter( pairsNoBase, Slicer.leftVector( threshold::test ), threshold::test );

        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
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
        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Ensemble>> input = Pool.of( values, meta, values, meta, null );
        Function<Pair<Double, Ensemble>, Pair<Double, Double>> mapper =
                in -> Pair.of( in.getLeft(),
                               Arrays.stream( in.getRight().getMembers() ).average().getAsDouble() );
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                Slicer.getRightSide( PoolSlicer.transform( Pool.of( values, meta ), mapper ) );
        assertTrue( Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = Slicer.getRightSide( PoolSlicer.transform( input, mapper ).getBaselineData() );
        assertTrue( Arrays.equals( actualBase, expected ) );
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
                                                             meta,
                                                             null );

        //Test without baseline
        Pool<Pair<Boolean, Boolean>> actualNoBase =
                PoolSlicer.transform( Pool.of( values, meta ), mapper );
        assertEquals( expectedNoBase.get(), actualNoBase.get() );

        //Test baseline
        Pool<Pair<Boolean, Boolean>> actualBase =
                PoolSlicer.transform( Pool.of( values, meta, values, meta, null ),
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
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT );

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
                PoolSlicer.transform( Pool.of( values, meta, values, meta, null ), mapper );
        assertEquals( expectedPairs, slicedWithBaseline.get() );
        assertEquals( expectedPairs, slicedWithBaseline.getBaselineData().get() );
    }    
    
    @Test
    void testGetPairCount()
    {
        Pool<TimeSeries<Boolean>> pool = Pool.of( List.of(), PoolMetadata.of() );

        assertEquals( 0, PoolSlicer.getPairCount( pool ) );


        SortedSet<Event<Boolean>> eventsOne = new TreeSet<>();
        eventsOne.add( Event.of( Instant.MIN, Boolean.valueOf( false ) ) );
        SortedSet<Event<Boolean>> eventsTwo = new TreeSet<>();
        eventsTwo.add( Event.of( Instant.MIN, Boolean.valueOf( false ) ) );
        eventsTwo.add( Event.of( Instant.MAX, Boolean.valueOf( true ) ) );

        Pool<TimeSeries<Boolean>> anotherPool =
                Pool.of( List.of( TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "foo",
                                                                        FeatureKey.of( "bar" ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        FeatureKey.of( "smeg" ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        assertEquals( 3, PoolSlicer.getPairCount( anotherPool ) );
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
                                                                        FeatureKey.of( "bar" ),
                                                                        "baz" ),
                                                 eventsOne ),
                                  TimeSeries.of( TimeSeriesMetadata.of( Collections.emptyMap(),
                                                                        TimeScaleOuter.of(),
                                                                        "bla",
                                                                        FeatureKey.of( "smeg" ),
                                                                        "faz" ),
                                                 eventsTwo ) ),
                         PoolMetadata.of() );

        Pool<String> expected = Pool.of( List.of( "Un", "pack", "ed!" ), PoolMetadata.of() );
        
        assertEquals( expected, PoolSlicer.unpack( pool ) );
    }

}
