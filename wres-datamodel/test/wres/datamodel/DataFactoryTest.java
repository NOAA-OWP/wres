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

import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link DataFactory}.
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

        final Location l = MetadataFactory.getLocation( "DRRC2" );
        final Metadata m1 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                         MetadataFactory.getDatasetIdentifier( l, "SQIN", "HEFS" ) );
        final List<VectorOfBooleans> input = new ArrayList<>();
        input.add( DataFactory.vectorOf( new boolean[] { true, false } ) );
        assertNotNull( DataFactory.ofDichotomousPairs( input, m1 ) );
        assertNotNull( DataFactory.ofMulticategoryPairs( input, m1 ) );

        final List<PairOfDoubles> dInput = new ArrayList<>();
        dInput.add( DataFactory.pairOf( 0.0, 1.0 ) );
        final Location l2 = MetadataFactory.getLocation( "DRRC2" );
        final Metadata m2 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                         MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" ) );
        final Location l3 = MetadataFactory.getLocation( "DRRC2" );
        final Metadata m3 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                         MetadataFactory.getDatasetIdentifier( l3, "SQIN", "ESP" ) );
        assertNotNull( DataFactory.ofDiscreteProbabilityPairs( dInput, m2 ) );
        assertNotNull( DataFactory.ofDiscreteProbabilityPairs( dInput, dInput, m2, m3, null ) );
        assertNotNull( DataFactory.ofSingleValuedPairs( dInput, m3 ) );
        assertNotNull( DataFactory.ofSingleValuedPairs( dInput, dInput, m2, m3, null ) );

        final List<PairOfDoubleAndVectorOfDoubles> eInput = new ArrayList<>();
        eInput.add( DataFactory.pairOf( 0.0, new double[] { 1.0, 2.0 } ) );
        assertNotNull( DataFactory.ofEnsemblePairs( eInput, m3 ) );
        assertNotNull( DataFactory.ofEnsemblePairs( eInput, eInput, m2, m3, null ) );
    }

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final PairOfDoubles tuple = DataFactory.pairOf( 1.0, 2.0 );
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getItemOne(), THRESHOLD );
        assertEquals( 2.0, tuple.getItemTwo(), THRESHOLD );
    }

    @Test
    public void vectorOfDoublesTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = DataFactory.vectorOf( arrOne );
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], THRESHOLD );
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = DataFactory.vectorOf( arrOne );
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
    public void pairOfDoubleAndVectorOfDoublesTest()
    {
        final double[] arrOne = { 2.0, 3.0 };
        final PairOfDoubleAndVectorOfDoubles tuple = DataFactory.pairOf( 1.0, arrOne );
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getItemOne(), THRESHOLD );
        assertEquals( 2.0, tuple.getItemTwo()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getItemTwo()[1], THRESHOLD );
        // check that toString() does not throw exception and is not null
        assertNotNull( tuple.toString() );
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 2.0, 3.0 };
        final PairOfDoubleAndVectorOfDoubles tuple = DataFactory.pairOf( 1.0, arrOne );
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;
        assertNotNull( tuple );
        assertEquals( 1.0, tuple.getItemOne(), THRESHOLD );
        assertEquals( 2.0, tuple.getItemTwo()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getItemTwo()[1], THRESHOLD );
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesUsingBoxedMutationTest()
    {
        final Double[] arrOne = { 2.0, 3.0 };
        final PairOfDoubleAndVectorOfDoubles tuple = DataFactory.pairOf( 1.0, arrOne );
        assertNotNull( tuple );

        // mutate the original array
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;

        assertEquals( 1.0, tuple.getItemOne(), THRESHOLD );
        assertEquals( 2.0, tuple.getItemTwo()[0], THRESHOLD );
        assertEquals( 3.0, tuple.getItemTwo()[1], THRESHOLD );
        // check that toString() does not throw exception and is not null
        assertNotNull( tuple.toString() );
    }

    @Test
    public void vectorOfBooleanTest()
    {
        final boolean[] arrOne = { false, true };
        final VectorOfBooleans vec = DataFactory.vectorOf( arrOne );
        assertEquals( false, vec.getBooleans()[0] );
        assertEquals( true, vec.getBooleans()[1] );
    }

    @Test
    public void vectorOfBooleanMutationTest()
    {
        final boolean[] arrOne = { false, true };
        final VectorOfBooleans vec = DataFactory.vectorOf( arrOne );
        // mutate the values in the original array
        arrOne[0] = true;
        arrOne[1] = false;
        // despite mutation, we should get the same result back
        assertEquals( false, vec.getBooleans()[0] );
        assertEquals( true, vec.getBooleans()[1] );
    }

    @Test
    public void pairOfBooleansTest()
    {
        final boolean one = true;
        final boolean two = false;
        final PairOfBooleans bools = DataFactory.pairOf( one, two );
        assertEquals( true, bools.getItemOne() );
        assertEquals( false, bools.getItemTwo() );
    }

    @Test
    public void pairOfBooleansMutationTest()
    {
        boolean one = true;
        boolean two = false;
        final PairOfBooleans bools = DataFactory.pairOf( one, two );
        one = false;
        two = true;
        assertEquals( true, bools.getItemOne() );
        assertEquals( false, bools.getItemTwo() );
    }

    @Test
    public void pairOfDoubleAndVectorOfDoubleToStringTest()
    {
        double[] arr = { 123456.0, 78910.0, 111213.0 };
        PairOfDoubleAndVectorOfDoubles p = DataFactory.pairOf( 141516.0, arr );
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
        Pair<TimeWindow, Threshold> first = Pair.of( DataFactory.ofTimeWindow( Instant.MIN,
                                                                               Instant.MAX,
                                                                               ReferenceTime.ISSUE_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                Instant.MAX,
                                                                                ReferenceTime.ISSUE_TIME ),
                                                      DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected equality.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindow, Threshold> third = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.MAX,
                                                                               ReferenceTime.ISSUE_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", third.compareTo( first ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( first.compareTo( third ) ) == Math.abs( third.compareTo( first ) ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                ReferenceTime.ISSUE_TIME ),
                                                      DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", third.compareTo( fourth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( third.compareTo( fourth ) ) == Math.abs( fourth.compareTo( third ) ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                               ReferenceTime.VALID_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected greater than.", fourth.compareTo( fifth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( fourth.compareTo( fifth ) ) == Math.abs( fifth.compareTo( fourth ) ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                               ReferenceTime.VALID_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 0.0 ),
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
        Pair<TimeWindow, Threshold> zeroeth = Pair.of( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ),
                                                       DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> first = Pair.of( DataFactory.ofTimeWindow( Instant.MIN,
                                                                               Instant.MAX,
                                                                               ReferenceTime.ISSUE_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                Instant.MAX,
                                                                                ReferenceTime.ISSUE_TIME ),
                                                      DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
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
        Pair<TimeWindow, Threshold> third = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.MAX,
                                                                               ReferenceTime.ISSUE_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !third.equals( first ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                ReferenceTime.ISSUE_TIME ),
                                                      DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !third.equals( fourth ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                               ReferenceTime.VALID_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !fourth.equals( fifth ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                               Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                               ReferenceTime.VALID_TIME ),
                                                     DataFactory.ofThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
        assertTrue( "Expected inequality.", !fifth.equals( sixth ) );
    }

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link DefaultMapKey}.
     */

    @Test
    public void compareDefaultMapKeyTest()
    {
        //Test equality
        MapKey<TimeWindow> first = DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                    Instant.MAX,
                                                                                    ReferenceTime.ISSUE_TIME ) );
        MapKey<TimeWindow> second = DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                     Instant.MAX,
                                                                                     ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected equality.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        MapKey<TimeWindow> third =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.MAX,
                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected greater than.", third.compareTo( first ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( first.compareTo( third ) ) == Math.abs( third.compareTo( first ) ) );
        //Latest date
        MapKey<TimeWindow> fourth =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected greater than.", third.compareTo( fourth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( third.compareTo( fourth ) ) == Math.abs( fourth.compareTo( third ) ) );
        //Reference time
        MapKey<TimeWindow> fifth =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 ReferenceTime.VALID_TIME ) );
        assertTrue( "Expected greater than.", fourth.compareTo( fifth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( fourth.compareTo( fifth ) ) == Math.abs( fifth.compareTo( fourth ) ) );
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
     * Tests the {@link DefaultMapKey#equals(Object)} and {@link DefaultMapKey#hashCode()}.
     */

    @Test
    public void equalsHashCodeDefaultMapKeyTest()
    {
        //Equality
        MapKey<TimeWindow> zeroeth = DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      ReferenceTime.ISSUE_TIME ) );
        MapKey<TimeWindow> first = DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                    Instant.MAX,
                                                                                    ReferenceTime.ISSUE_TIME ) );
        MapKey<TimeWindow> second = DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.MIN,
                                                                                     Instant.MAX,
                                                                                     ReferenceTime.ISSUE_TIME ) );
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
        assertEquals( "Expected equal hashcodes.", first.hashCode(), second.hashCode() );

        //Test inequalities
        //Earliest date
        MapKey<TimeWindow> third =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.MAX,
                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected inequality.", !third.equals( first ) );
        //Latest date
        MapKey<TimeWindow> fourth =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected inequality.", !third.equals( fourth ) );
        //Reference time
        MapKey<TimeWindow> fifth =
                DataFactory.getMapKey( DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 ReferenceTime.VALID_TIME ) );
        assertTrue( "Expected inequality.", !fourth.equals( fifth ) );
    }


}
