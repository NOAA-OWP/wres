package wres.datamodel;

import static org.junit.Assert.assertTrue;

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
    public void test1hashCode()
    {
        Threshold left = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold right = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold otherRight =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        assertTrue( "Expected equal hash codes.", left.hashCode() == right.hashCode() );
        assertTrue( "Expected unequal hash codes.", left.hashCode() != otherRight.hashCode() );
    }

    /**
     * Tests {@link SafeThreshold#compareTo(Threshold)}.
     */

    @Test
    public void test1compareTo()
    {
        Threshold left = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold right = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold otherRight =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        assertTrue( "Expected comparative values.", left.compareTo( right ) == 0 );
        assertTrue( "Expected different values.", otherRight.compareTo( left ) != 0 );
    }

    /**
     * Tests {@link SafeThreshold#equals(Object)}.
     */

    @Test
    public void test1equals()
    {
        Threshold left = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold right = new ThresholdBuilder().setThreshold( 0.0 ).setCondition( Operator.GREATER ).build();
        Threshold otherRight =
                new ThresholdBuilder().setThreshold( 0.0 )
                                      .setThresholdProbability( 0.0 )
                                      .setCondition( Operator.GREATER )
                                      .build();
        assertTrue( "Expected comparative values.", left.equals( right ) );
        assertTrue( "Expected different values.", !otherRight.equals( left ) );
    }


}
