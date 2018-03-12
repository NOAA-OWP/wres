package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.Threshold.Operator;

/**
 * Tests the default implementation of an {@link Thresholds} returned by {@link Thresholds#of(Threshold)} and 
 * {@link Thresholds#of(Threshold, Threshold)}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DefaultThresholdsTest
{

    /**
     * Instance of a {@link DataFactory} for testing.
     */

    private static final DataFactory FACTORY = DefaultDataFactory.getInstance();

    /**
     * Constructs a {@link Thresholds} and confirms that the {@link Thresholds#first()} returns the expected result.
     */

    @Test
    public void testGetFirst()
    {
        Thresholds thresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    FACTORY.ofThreshold( 0.1, Operator.GREATER ).equals( thresholds.first() ) );
    }

    /**
     * Constructs a {@link Thresholds} and confirms that the {@link Thresholds#second()} returns the expected result.
     */

    @Test
    public void testGetSecond()
    {
        Thresholds thresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ),
                                               FACTORY.ofThreshold( 0.2, Operator.GREATER ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    FACTORY.ofThreshold( 0.2, Operator.GREATER ).equals( thresholds.second() ) );
    }

    /**
     * Constructs a {@link Thresholds} and tests {@link Thresholds#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        Thresholds thresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertTrue( "The thresholds instance does not meet the equals contract for reflexivity.",
                    thresholds.equals( thresholds ) );
        // Symmetric
        Thresholds otherThresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertTrue( "The thresholds instances do not meet the equals contract for symmetry.",
                    thresholds.equals( otherThresholds ) && otherThresholds.equals( thresholds ) );
        // Transitive
        Thresholds oneMoreThresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertTrue( "The thresholds instances do not meet the equals contract for transitivity.",
                    thresholds.equals( otherThresholds ) && otherThresholds.equals( oneMoreThresholds )
                                                                                                  && thresholds.equals( oneMoreThresholds ) );
        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The event does not meet the equals contract for consistency.",
                        thresholds.equals( otherThresholds ) );
        }
        // Nullity
        assertFalse( "The event does not meet the equals contract for nullity.", thresholds.equals( null ) );

        // Check unequal cases
        Thresholds unequalOnFirst = Thresholds.of( FACTORY.ofThreshold( 0.2, Operator.GREATER ),
                                                   FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertFalse( "Expected the thresholds to differ.", thresholds.equals( unequalOnFirst ) );
        // Differences on nullity of second
        Thresholds unequalOnNullity = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ),
                                                     FACTORY.ofThreshold( 0.3, Operator.GREATER ) );
        assertFalse( "Expected the event to differ on value.", thresholds.equals( unequalOnNullity ) );
        Thresholds unequalOnSecond = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ),
                                                    FACTORY.ofThreshold( 0.4, Operator.GREATER ) );
        assertFalse( "Expected the event to differ on value.", unequalOnNullity.equals( unequalOnSecond ) );
    }

    /**
     * Constructs a {@link Thresholds} and tests {@link Thresholds#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        Thresholds thresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        Thresholds otherThresholds = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        assertTrue( "The hashcode of the thresholds is inconsistent with equals.",
                    thresholds.equals( otherThresholds ) && thresholds.hashCode() == otherThresholds.hashCode() );
        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The thresholds instances do not meet the hashcode contract for consistency.",
                        thresholds.hashCode() == otherThresholds.hashCode() );
        }
    }
    
    /**
     * Constructs a {@link Thresholds} and compares to other instances using {@link Thresholds#compareTo(Thresholds)}.
     */

    @Test
    public void testCompareTo()
    {
        // Consistent with equals
        Thresholds first = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        Thresholds second = Thresholds.of( FACTORY.ofThreshold( 0.1, Operator.GREATER ) );
        
        assertTrue( "The thresholds are not comparable.",
                    first.compareTo( second ) == 0 );
        
        Thresholds third = Thresholds.of( FACTORY.ofThreshold( 0.2, Operator.GREATER ) );
        
        // Anticommutative
        assertTrue( "Expected anticommutative behaviour.",
                    Math.abs( third.compareTo( first ) ) == Math.abs( first.compareTo( third ) ) );
        // Reflexive
        assertTrue( "Expected reflexive comparability.", first.compareTo( first ) == 0 );
        // Symmetric 
        assertTrue( "Expected symmetric comparability.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );

        // Transitive 
        Thresholds fourth = Thresholds.of( FACTORY.ofThreshold( 0.3, Operator.GREATER ) );

        assertTrue( "Expected transitive behaviour.",
                    fourth.compareTo( third ) > 0 && third.compareTo( first ) > 0
                                                      && fourth.compareTo( first ) > 0 );
        
    }    

    /**
     * Constructs a {@link Thresholds} and tests {@link Thresholds#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        Thresholds testString = Thresholds.of( FACTORY.ofQuantileThreshold( 27.0, 0.5, Operator.GREATER_EQUAL ) );
        
        assertTrue( ">= 27.0 [Pr = 0.5]".equals( testString.toString() ) );
        
        Thresholds secondTestString = Thresholds.of( FACTORY.ofQuantileThreshold( 23.0, 0.2, Operator.GREATER ),
                                               FACTORY.ofProbabilityThreshold( 0.1, Operator.GREATER ) );

        assertTrue( "> 23.0 [Pr = 0.2] & Pr > 0.1".equals( secondTestString.toString() ) );
    }
    
    /**
     * Constructs a {@link Thresholds} and tests {@link Thresholds#toStringSafe()} against other instances.
     */

    @Test
    public void testToStringSafe()
    {       
        Thresholds testString = Thresholds.of( FACTORY.ofQuantileThreshold( 27.0, 0.5, Operator.GREATER_EQUAL ) );

        assertTrue( "GTE_27.0_Pr=0.5".equals( testString.toStringSafe() ) );
        
        Thresholds secondTestString = Thresholds.of( FACTORY.ofQuantileThreshold( 23.0, 0.2, Operator.GREATER ),
                                               FACTORY.ofProbabilityThreshold( 0.1, Operator.GREATER ) );

        assertTrue( "GT_23.0_Pr=0.2_&_Pr_GT_0.1".equals( secondTestString.toStringSafe() ) );
    }

    /**
     * Constructs a {@link Thresholds} and tests for exceptions.
     */

    @Test
    public void testExceptions()
    {
        try
        {
            Thresholds.of( null, null );
            fail( "Expected an exception on building a thresholds with a null first threshold." );
        }
        catch ( NullPointerException e )
        {
        }
    }

}
