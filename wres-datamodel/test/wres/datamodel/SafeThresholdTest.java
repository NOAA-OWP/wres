package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.SafeThreshold.ThresholdBuilder;
import wres.datamodel.Threshold.Operator;

/**
 * Tests the {@link SafeThreshold}. 
 * 
 * TODO: this is a placeholder to be implemented properly in terms of depth and coverage
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeThresholdTest
{

    /**
     * Tests {@link SafeThreshold#hashCode()}.
     */

    @Test
    public void test1HashCode()
    {
        Threshold first = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER_EQUAL ).build();
        Threshold second = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER_EQUAL ).build();
        Threshold third =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .build();
        Threshold fourth =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .build();
        Threshold fifth =
                new ThresholdBuilder().setThreshold( Double.NEGATIVE_INFINITY )
                                      .setThresholdProbability( Double.NEGATIVE_INFINITY )
                                      .setCondition( Operator.GREATER )
                                      .build();
        Threshold sixth =
                new ThresholdBuilder().setThreshold( Double.NEGATIVE_INFINITY )
                                      .setThresholdProbability( Double.NEGATIVE_INFINITY )
                                      .setCondition( Operator.GREATER )
                                      .build();
        assertEquals( "Expected equal hash codes.", first.hashCode(), second.hashCode() );
        assertEquals( "Expected equal hash codes.", third.hashCode(), fourth.hashCode() );
        assertEquals( "Expected equal hash codes.", fifth.hashCode(), sixth.hashCode() );        
        assertTrue( "Expected unequal hash codes.", first.hashCode() != third.hashCode() );
    }

    /**
     * Tests {@link SafeThreshold#compareTo(Threshold)}.
     */

    @Test
    public void test1CompareTo()
    {
        Threshold first = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold second = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold third =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        Threshold fourth =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 1.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        //Equal
        assertTrue( "Expected equal values.", first.compareTo( second ) == 0 );
        //Unequal
        assertTrue( "Expected different values.", third.compareTo( first ) != 0 );
        //Anticommutative
        assertTrue( "Expected anticommutative behaviour.",
                    Math.abs( third.compareTo( first ) ) == Math.abs( first.compareTo( third ) ) );
        //Reflexive
        assertTrue( "Expected reflexive equality.", first.compareTo( first ) == 0 );
        //Symmetric 
        assertTrue( "Expected symmetric equality.", first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );
        //Transitive 
        assertTrue( "Expected transitive behaviour.",
                    fourth.compareTo( third ) > 0 && third.compareTo( first ) > 0 && fourth.compareTo( first ) > 0 );
        //Nullity
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
     * Tests {@link SafeThreshold#equals(Object)}.
     */

    @Test
    public void test1Equals()
    {
        Threshold left = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold otherLeft = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold right = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold otherRight =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        //Equal
        assertTrue( "Expected equal values.", left.equals( right ) );
        //Unequal
        assertTrue( "Expected different values.", !otherRight.equals( left ) );
        //Reflexive
        assertEquals( "Expected reflexive equality.", left, left );
        //Symmetric 
        assertTrue( "Expected symmetric equality.", left.equals( right ) && right.equals( left ) );
        //Transitive 
        assertTrue( "Expected transitive equality.",
                    left.equals( right ) && left.equals( otherLeft ) && otherLeft.equals( right ) );
        //Nullity
        assertTrue( "Expected inequality on null.", !left.equals( null ) );
    }


}
