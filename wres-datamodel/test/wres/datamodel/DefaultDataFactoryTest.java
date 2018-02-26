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

import wres.datamodel.DefaultDataFactory.DefaultMapKey;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;

/**
 * Tests the {@link DefaultDataFactory}.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */
public final class DefaultDataFactoryTest
{

    public static final double THRESHOLD = 0.00001;

    private final DataFactory metIn = DefaultDataFactory.getInstance();

    /**
     * Tests for the successful construction of pairs via the {@link DefaultDataFactory}.
     */

    @Test
    public void constructionOfPairsTest()
    {
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                                 metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        final List<VectorOfBooleans> input = new ArrayList<>();
        input.add( metIn.vectorOf( new boolean[] { true, false } ) );
        assertNotNull( metIn.ofDichotomousPairs( input, m1 ) );
        assertNotNull( metIn.ofMulticategoryPairs( input, m1 ) );

        final List<PairOfDoubles> dInput = new ArrayList<>();
        dInput.add( metIn.pairOf( 0.0, 1.0 ) );
        final Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                                 metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        final Metadata m3 = metaFac.getMetadata( metaFac.getDimension(),
                                                 metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "ESP" ) );
        assertNotNull( metIn.ofDiscreteProbabilityPairs( dInput, m2 ) );
        assertNotNull( metIn.ofDiscreteProbabilityPairs( dInput, dInput, m2, m3, null ) );
        assertNotNull( metIn.ofSingleValuedPairs( dInput, m3 ) );
        assertNotNull( metIn.ofSingleValuedPairs( dInput, dInput, m2, m3, null ) );

        final List<PairOfDoubleAndVectorOfDoubles> eInput = new ArrayList<>();
        eInput.add( metIn.pairOf( 0.0, new double[] { 1.0, 2.0 } ) );
        assertNotNull( metIn.ofEnsemblePairs( eInput, m3 ) );
        assertNotNull( metIn.ofEnsemblePairs( eInput, eInput, m2, m3, null ) );
    }

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final PairOfDoubles tuple = metIn.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo(), 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesTest()
    {
        final double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = metIn.vectorOf(arrOne);
        assertNotNull( doubleVecOne );
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = metIn.vectorOf( arrOne );
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull(doubleVecOne);
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void pairOfVectorsTest()
    {
        final double[] arrOne = {1.0, 2.0, 3.0};
        final double[] arrTwo = {4.0, 5.0};
        final Pair<VectorOfDoubles, VectorOfDoubles> pair = metIn.pairOf( arrOne, arrTwo);
        assertNotNull(pair);
        assertEquals(pair.getLeft().getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(pair.getLeft().getDoubles()[1], 2.0, THRESHOLD);
        assertEquals(pair.getLeft().getDoubles()[2], 3.0, THRESHOLD);
        assertEquals(pair.getRight().getDoubles()[0], 4.0, THRESHOLD);
        assertEquals(pair.getRight().getDoubles()[1], 5.0, THRESHOLD);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesTest()
    {
        final double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
        // check that toString() does not throw exception and is not null
        assertNotNull(tuple.toString());
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesMutationTest()
    {
        final double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;
        assertNotNull( tuple );
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesUsingBoxedMutationTest()
    {
        final Double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        assertNotNull(tuple);

        // mutate the original array
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;

        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
        // check that toString() does not throw exception and is not null
        assertNotNull(tuple.toString());
    }

    @Test
    public void vectorOfBooleanTest()
    {
        final boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = metIn.vectorOf(arrOne);
        assertEquals(vec.getBooleans()[0], false);
        assertEquals(vec.getBooleans()[1], true);
    }

    @Test
    public void vectorOfBooleanMutationTest()
    {
        final boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = metIn.vectorOf(arrOne);
        // mutate the values in the original array
        arrOne[0] = true;
        arrOne[1] = false;
        // despite mutation, we should get the same result back
        assertEquals(vec.getBooleans()[0], false);
        assertEquals(vec.getBooleans()[1], true);
    }

    @Test
    public void pairOfBooleansTest()
    {
        final boolean one = true;
        final boolean two = false;
        final PairOfBooleans bools = metIn.pairOf(one, two);
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }

    @Test
    public void pairOfBooleansMutationTest()
    {
        boolean one = true;
        boolean two = false;
        final PairOfBooleans bools = metIn.pairOf(one, two);
        one = false;
        two = true;
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }

    @Test
    public void pairOfDoubleAndVectorOfDoubleToStringTest()
    {
        double[] arr = {123456.0, 78910.0, 111213.0};
        PairOfDoubleAndVectorOfDoubles p = metIn.pairOf(141516.0, arr);
        String result = p.toString();
        assertTrue("12345 expected to show up in toString: " + result,
                    result.contains("12345"));
        assertTrue("7891 expected to show up in toString: " + result,
                    result.contains("7891"));
        assertTrue("11121 expected to show up in toString: " + result,
                    result.contains("11121"));
        assertTrue("14151 expected to show up in toString: " + result,
                    result.contains("14151"));
    }

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link Pair}.
     */

    @Test
    public void compareDefaultMapBiKeyTest()
    {
        //Test equality
        Pair<TimeWindow, Threshold> first = Pair.of( metIn.ofTimeWindow( Instant.MIN,
                                                                         Instant.MAX,
                                                                         ReferenceTime.ISSUE_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        Pair<TimeWindow, Threshold> second = Pair.of( metIn.ofTimeWindow( Instant.MIN,
                                                                          Instant.MAX,
                                                                          ReferenceTime.ISSUE_TIME ),
                                                      metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected equality.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindow, Threshold> third = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.MAX,
                                                                         ReferenceTime.ISSUE_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected greater than.", third.compareTo( first ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( first.compareTo( third ) ) == Math.abs( third.compareTo( first ) ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                          ReferenceTime.ISSUE_TIME ),
                                                      metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected greater than.", third.compareTo( fourth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( third.compareTo( fourth ) ) == Math.abs( fourth.compareTo( third ) ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                         ReferenceTime.VALID_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected greater than.", fourth.compareTo( fifth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( fourth.compareTo( fifth ) ) == Math.abs( fifth.compareTo( fourth ) ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                         ReferenceTime.VALID_TIME ),
                                                     metIn.ofThreshold( 0.0, Operator.GREATER ) );
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
        Pair<TimeWindow, Threshold> zeroeth = Pair.of( metIn.ofTimeWindow( Instant.MIN,
                                                                           Instant.MAX,
                                                                           ReferenceTime.ISSUE_TIME ),
                                                       metIn.ofThreshold( 1.0, Operator.GREATER ) );
        Pair<TimeWindow, Threshold> first = Pair.of( metIn.ofTimeWindow( Instant.MIN,
                                                                         Instant.MAX,
                                                                         ReferenceTime.ISSUE_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        Pair<TimeWindow, Threshold> second = Pair.of( metIn.ofTimeWindow( Instant.MIN,
                                                                          Instant.MAX,
                                                                          ReferenceTime.ISSUE_TIME ),
                                                      metIn.ofThreshold( 1.0, Operator.GREATER ) );
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
        Pair<TimeWindow, Threshold> third = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.MAX,
                                                                         ReferenceTime.ISSUE_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected inequality.", !third.equals( first ) );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                          ReferenceTime.ISSUE_TIME ),
                                                      metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected inequality.", !third.equals( fourth ) );
        //Reference time
        Pair<TimeWindow, Threshold> fifth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                         ReferenceTime.VALID_TIME ),
                                                     metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Expected inequality.", !fourth.equals( fifth ) );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                         ReferenceTime.VALID_TIME ),
                                                     metIn.ofThreshold( 0.0, Operator.GREATER ) );
        assertTrue( "Expected inequality.", !fifth.equals( sixth ) );
    }

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link DefaultMapKey}.
     */

    @Test
    public void compareDefaultMapKeyTest()
    {
        //Test equality
        DefaultMapKey<TimeWindow> first =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        DefaultMapKey<TimeWindow> second =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected equality.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        DefaultMapKey<TimeWindow> third =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected greater than.", third.compareTo( first ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( first.compareTo( third ) ) == Math.abs( third.compareTo( first ) ) );
        //Latest date
        DefaultMapKey<TimeWindow> fourth =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected greater than.", third.compareTo( fourth ) > 0 );
        assertTrue( "Expected anticommutativity.",
                    Math.abs( third.compareTo( fourth ) ) == Math.abs( fourth.compareTo( third ) ) );
        //Reference time
        DefaultMapKey<TimeWindow> fifth =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
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
        DefaultMapKey<TimeWindow> zeroeth =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        DefaultMapKey<TimeWindow> first =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        DefaultMapKey<TimeWindow> second =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.MIN,
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
        DefaultMapKey<TimeWindow> third =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                 Instant.MAX,
                                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected inequality.", !third.equals( first ) );
        //Latest date
        DefaultMapKey<TimeWindow> fourth =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                 ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Expected inequality.", !third.equals( fourth ) );
        //Reference time
        DefaultMapKey<TimeWindow> fifth =
                (DefaultMapKey<TimeWindow>) metIn.getMapKey( metIn.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                 ReferenceTime.VALID_TIME ) );
        assertTrue( "Expected inequality.", !fourth.equals( fifth ) );
    }


}
