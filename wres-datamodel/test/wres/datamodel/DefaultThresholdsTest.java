package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;


/**
 * Tests the default implementation of an {@link OneOrTwoThresholds} returned by {@link OneOrTwoThresholds#of(Threshold)} and 
 * {@link OneOrTwoThresholds#of(Threshold, Threshold)}.
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
     * Constructs a {@link OneOrTwoThresholds} and confirms that the {@link OneOrTwoThresholds#first()} returns the expected result.
     */

    @Test
    public void testGetFirst()
    {
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                         ThresholdDataType.LEFT )
                           .equals( thresholds.first() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and confirms that the {@link OneOrTwoThresholds#second()} returns the expected result.
     */

    @Test
    public void testGetSecond()
    {
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ),
                                               FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.2 ), Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.2 ), Operator.GREATER,
                                         ThresholdDataType.LEFT )
                           .equals( thresholds.second() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );
        assertTrue( "The thresholds instance does not meet the equals contract for reflexivity.",
                    thresholds.equals( thresholds ) );
        // Symmetric
        OneOrTwoThresholds otherThresholds =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ) );
        assertTrue( "The thresholds instances do not meet the equals contract for symmetry.",
                    thresholds.equals( otherThresholds ) && otherThresholds.equals( thresholds ) );
        // Transitive
        OneOrTwoThresholds oneMoreThresholds =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ) );
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
        OneOrTwoThresholds unequalOnFirst =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.2 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ),
                               FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );
        assertFalse( "Expected the thresholds to differ.", thresholds.equals( unequalOnFirst ) );
        // Differences on nullity of second
        OneOrTwoThresholds unequalOnNullity =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ),
                               FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.3 ), Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );
        assertFalse( "Expected the event to differ on value.", thresholds.equals( unequalOnNullity ) );
        OneOrTwoThresholds unequalOnSecond =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ),
                               FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.4 ), Operator.GREATER,
                                                    ThresholdDataType.LEFT ) );
        assertFalse( "Expected the event to differ on value.", unequalOnNullity.equals( unequalOnSecond ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );
        OneOrTwoThresholds otherThresholds =
                OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                            ThresholdDataType.LEFT ) );
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
     * Constructs a {@link OneOrTwoThresholds} and compares to other instances using {@link OneOrTwoThresholds#compareTo(OneOrTwoThresholds)}.
     */

    @Test
    public void testCompareTo()
    {
        // Consistent with equals
        OneOrTwoThresholds first = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
        OneOrTwoThresholds second = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) );

        assertTrue( "The thresholds are not comparable.",
                    first.compareTo( second ) == 0 );

        OneOrTwoThresholds third = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.2 ), Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );

        // Anticommutative
        assertTrue( "Expected anticommutative behaviour.",
                    Math.abs( third.compareTo( first ) ) == Math.abs( first.compareTo( third ) ) );
        // Reflexive
        assertTrue( "Expected reflexive comparability.", first.compareTo( first ) == 0 );
        // Symmetric 
        assertTrue( "Expected symmetric comparability.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );

        // Transitive 
        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( FACTORY.ofThreshold( SafeOneOrTwoDoubles.of( 0.3 ), Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) );

        assertTrue( "Expected transitive behaviour.",
                    fourth.compareTo( third ) > 0 && third.compareTo( first ) > 0
                                                      && fourth.compareTo( first ) > 0 );

    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        OneOrTwoThresholds testString =
                OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 27.0 ),
                                                            SafeOneOrTwoDoubles.of( 0.5 ),
                                                            Operator.GREATER_EQUAL,
                                                            ThresholdDataType.LEFT ) );

        assertTrue( ">= 27.0 [Pr = 0.5]".equals( testString.toString() ) );

        OneOrTwoThresholds secondTestString =
                OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 23.0 ),
                                                            SafeOneOrTwoDoubles.of( 0.2 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT ),
                               FACTORY.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                               ThresholdDataType.LEFT ) );

        assertTrue( "> 23.0 [Pr = 0.2] AND Pr > 0.1".equals( secondTestString.toString() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#toStringSafe()} against other instances.
     */

    @Test
    public void testToStringSafe()
    {
        OneOrTwoThresholds testString =
                OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 27.0 ),
                                                            SafeOneOrTwoDoubles.of( 0.5 ),
                                                            Operator.GREATER_EQUAL,
                                                            ThresholdDataType.LEFT ) );

        assertTrue( "GTE_27.0_Pr_EQ_0.5".equals( testString.toStringSafe() ) );

        OneOrTwoThresholds secondTestString =
                OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 23.0 ),
                                                            SafeOneOrTwoDoubles.of( 0.2 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT ),
                               FACTORY.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( 0.1 ), Operator.GREATER,
                                                               ThresholdDataType.LEFT ) );

        assertTrue( "GT_23.0_Pr_EQ_0.2_AND_Pr_GT_0.1".equals( secondTestString.toStringSafe() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests for exceptions.
     */

    @Test
    public void testExceptions()
    {
        try
        {
            OneOrTwoThresholds.of( null, null );
            fail( "Expected an exception on building a thresholds with a null first threshold." );
        }
        catch ( NullPointerException e )
        {
        }
    }

}
