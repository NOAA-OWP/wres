package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.inputs.pairs.DichotomousPair;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPair;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link DataFactory}.
 * 
 * TODO: refactor the tests of containers (as opposed to factory methods) into their own test classes.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 */
public final class DataFactoryTest
{

    public static final double THRESHOLD = 0.00001;

    /**
     * Tests for the successful construction of pairs via the {@link DataFactory}.
     */

    @Test
    public void constructionOfPairsTest()
    {

        final Location l = Location.of( "DRRC2" );
        final Metadata m1 = Metadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( l, "SQIN", "HEFS" ) );
        final List<DichotomousPair> input = new ArrayList<>();
        input.add( DichotomousPair.of( true, false ) );
        assertNotNull( DichotomousPairs.ofDichotomousPairs( input, m1 ) );

        final List<DiscreteProbabilityPair> dInput = new ArrayList<>();
        dInput.add( DiscreteProbabilityPair.of( 0.0, 1.0 ) );
        final Location l2 = Location.of( "DRRC2" );
        final Metadata m2 = Metadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( l2, "SQIN", "HEFS" ) );
        final Location l3 = Location.of( "DRRC2" );
        final Metadata m3 = Metadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( l3, "SQIN", "ESP" ) );
        assertNotNull( DiscreteProbabilityPairs.of( dInput, m2 ) );
        assertNotNull( DiscreteProbabilityPairs.of( dInput, dInput, m2, m3, null ) );

        final List<SingleValuedPair> dInputSingleValued = new ArrayList<>();
        dInputSingleValued.add( SingleValuedPair.of( 0.0, 1.0 ) );

        assertNotNull( SingleValuedPairs.of( dInputSingleValued, m3 ) );
        assertNotNull( SingleValuedPairs.of( dInputSingleValued, dInputSingleValued, m2, m3, null ) );

        final List<EnsemblePair> eInput = new ArrayList<>();
        eInput.add( EnsemblePair.of( 0.0, new double[] { 1.0, 2.0 } ) );
        assertNotNull( EnsemblePairs.of( eInput, m3 ) );
        assertNotNull( EnsemblePairs.of( eInput, eInput, m2, m3, null ) );
    }

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final SingleValuedPair tuple = SingleValuedPair.of( 1.0, 2.0 );
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getLeft(), THRESHOLD );
        assertEquals( 2.0, tuple.getRight(), THRESHOLD );
    }

    @Test
    public void vectorOfDoublesTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], THRESHOLD );
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], THRESHOLD );
    }

    @Test
    public void pairOfVectorsTest()
    {
        final double[] arrOne = { 1.0, 2.0, 3.0 };
        final double[] arrTwo = { 4.0, 5.0 };
        final Pair<VectorOfDoubles, VectorOfDoubles> pair = DataFactory.pairOf( arrOne, arrTwo );
        assertNotNull( pair );
        assertEquals( 1.0, pair.getLeft().getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, pair.getLeft().getDoubles()[1], THRESHOLD );
        assertEquals( 3.0, pair.getLeft().getDoubles()[2], THRESHOLD );
        assertEquals( 4.0, pair.getRight().getDoubles()[0], THRESHOLD );
        assertEquals( 5.0, pair.getRight().getDoubles()[1], THRESHOLD );
    }

    @Test
    public void ensemblePairTest()
    {
        final double[] arrOne = { 2.0, 3.0 };
        final EnsemblePair tuple = EnsemblePair.of( 1.0, arrOne );
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getLeft(), THRESHOLD );
        assertEquals( 2.0, tuple.getRight()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getRight()[1], THRESHOLD );
        // check that toString() does not throw exception and is not null
        assertNotNull( tuple.toString() );
    }

    @Test
    public void ensemblePairMutationTest()
    {
        final double[] arrOne = { 2.0, 3.0 };
        final EnsemblePair tuple = EnsemblePair.of( 1.0, arrOne );
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getLeft(), THRESHOLD );
        assertEquals( 2.0, tuple.getRight()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getRight()[1], THRESHOLD );
    }

    @Test
    public void ensemblePairBoxedMutationTest()
    {
        final Double[] arrOne = { 2.0, 3.0 };
        final EnsemblePair tuple = EnsemblePair.of( 1.0, arrOne );
        assertNotNull( tuple );

        // mutate the original array
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;

        assertEquals( 1.0, tuple.getLeft(), THRESHOLD );
        assertEquals( 2.0, tuple.getRight()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getRight()[1], THRESHOLD );
        // check that toString() does not throw exception and is not null
        assertNotNull( tuple.toString() );
    }

    @Test
    public void dichotomousPairTest()
    {
        final boolean one = true;
        final boolean two = false;
        final DichotomousPair bools = DichotomousPair.of( one, two );
        assertEquals( true, bools.getLeft() );
        assertEquals( false, bools.getRight() );
    }

    @Test
    public void dichotomousPairMutationTest()
    {
        boolean one = true;
        boolean two = false;
        final DichotomousPair bools = DichotomousPair.of( one, two );
        one = false;
        two = true;
        assertEquals( true, bools.getLeft() );
        assertEquals( false, bools.getRight() );
    }

    @Test
    public void ensemblePairToStringTest()
    {
        double[] arr = { 123456.0, 78910.0, 111213.0 };
        EnsemblePair p = EnsemblePair.of( 141516.0, arr );
        String result = p.toString();
        assertTrue( "12345 expected to show up in toString: " + result,
                    result.contains( "12345" ) );
        assertTrue( "7891 expected to show up in toString: " + result,
                    result.contains( "7891" ) );
        assertTrue( "11121 expected to show up in toString: " + result,
                    result.contains( "11121" ) );
        assertTrue( "14151 expected to show up in toString: " + result,
                    result.contains( "14151" ) );
    }

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link Pair}.
     */

    @Test
    public void compareDefaultMapBiKeyTest()
    {
        //Test equality
        Pair<TimeWindow, Threshold> first = Pair.of( TimeWindow.of( Instant.MIN,
                                                                    Instant.MAX,
                                                                    ReferenceTime.ISSUE_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( TimeWindow.of( Instant.MIN,
                                                                     Instant.MAX,
                                                                     ReferenceTime.ISSUE_TIME ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected equality.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindow, Threshold> third = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.MAX,
                                                                    ReferenceTime.ISSUE_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", third.compareTo( first ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( first.compareTo( third ) ) == Math.abs( third.compareTo( first ) ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                     Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                     ReferenceTime.ISSUE_TIME ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", third.compareTo( fourth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( third.compareTo( fourth ) ) == Math.abs( fourth.compareTo( third ) ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                    ReferenceTime.VALID_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", fourth.compareTo( fifth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( fourth.compareTo( fifth ) ) == Math.abs( fifth.compareTo( fourth ) ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                    ReferenceTime.VALID_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", fifth.compareTo( sixth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( fifth.compareTo( sixth ) ) == Math.abs( sixth.compareTo( fifth ) ) );
        //Check nullity contract
        try
        {
            first.compareTo( null );
            fail( "Expected null pointer on comparing." );
        }
        catch ( NullPointerException e )
        {
        }
    }

    /**
     * Tests the {@link Pair#equals(Object)} and {@link Pair#hashCode()}.
     */

    @Test
    public void equalsHashCodePairTest()
    {
        //Equality
        Pair<TimeWindow, Threshold> zeroeth = Pair.of( TimeWindow.of( Instant.MIN,
                                                                      Instant.MAX,
                                                                      ReferenceTime.ISSUE_TIME ),
                                                       Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> first = Pair.of( TimeWindow.of( Instant.MIN,
                                                                    Instant.MAX,
                                                                    ReferenceTime.ISSUE_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( TimeWindow.of( Instant.MIN,
                                                                     Instant.MAX,
                                                                     ReferenceTime.ISSUE_TIME ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        //Reflexive
        assertEquals( "Expected reflexive equality.", first, first );
        //Symmetric 
        assertTrue( "Expected symmetric equality.", first.equals( second ) && second.equals( first ) );
        //Transitive 
        assertTrue( "Expected transitive equality.",
                    zeroeth.equals( first ) && first.equals( second ) && zeroeth.equals( second ) );
        //Nullity
        assertTrue( "Expected inequality on null.", !first.equals( null ) );
        //Check hashcode
        //assertEquals( "Expected equal hashcodes.", first.hashCode(), second.hashCode() );

        //Test inequalities
        //Earliest date
        Pair<TimeWindow, Threshold> third = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.MAX,
                                                                    ReferenceTime.ISSUE_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !third.equals( first ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                     Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                     ReferenceTime.ISSUE_TIME ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !third.equals( fourth ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                    ReferenceTime.VALID_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !fourth.equals( fifth ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                    Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                    ReferenceTime.VALID_TIME ),
                                                     Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !fifth.equals( sixth ) );
    }

}
