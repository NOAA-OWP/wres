package wres.datamodel.thresholds;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link OneOrTwoThresholds}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class OneOrTwoThresholdsTest
{

    /**
     * Constructs a {@link OneOrTwoThresholds} and confirms that the {@link OneOrTwoThresholds#first()} returns the 
     * expected result.
     */

    @Test
    public void testGetFirst()
    {
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                        Operator.GREATER,
                                                                                        ThresholdDataType.LEFT ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                             Operator.GREATER,
                                             ThresholdDataType.LEFT )
                               .equals( thresholds.first() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and confirms that the {@link OneOrTwoThresholds#second()} returns the 
     * expected result.
     */

    @Test
    public void testGetSecond()
    {
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                        Operator.GREATER,
                                                                                        ThresholdDataType.LEFT ),
                                                               ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                                        Operator.GREATER,
                                                                                        ThresholdDataType.LEFT ) );
        assertTrue( "The threshold has an unexpected first threshold.",
                    ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                             Operator.GREATER,
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
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                        Operator.GREATER,
                                                                                        ThresholdDataType.LEFT ) );
        assertTrue( "The thresholds instance does not meet the equals contract for reflexivity.",
                    thresholds.equals( thresholds ) );
        // Symmetric
        OneOrTwoThresholds otherThresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        assertTrue( "The thresholds instances do not meet the equals contract for symmetry.",
                    thresholds.equals( otherThresholds ) && otherThresholds.equals( thresholds ) );
        // Transitive
        OneOrTwoThresholds oneMoreThresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        assertTrue( "The thresholds instances do not meet the equals contract for transitivity.",
                    thresholds.equals( otherThresholds ) && otherThresholds.equals( oneMoreThresholds )
                                                                                                  && thresholds.equals( oneMoreThresholds ) );
        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The threshold does not meet the equals contract for consistency.",
                        thresholds.equals( otherThresholds ) );
        }
        // Nullity
        assertNotEquals( null, thresholds );

        // Check unequal cases
        OneOrTwoThresholds unequalOnFirst =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ),
                                       ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        assertFalse( "Expected the thresholds to differ.", thresholds.equals( unequalOnFirst ) );
        // Differences on nullity of second
        OneOrTwoThresholds unequalOnNullity =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ),
                                       ThresholdOuter.of( OneOrTwoDoubles.of( 0.3 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        assertFalse( "Expected the threshold to differ on value.", thresholds.equals( unequalOnNullity ) );
        OneOrTwoThresholds unequalOnSecond =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ),
                                       ThresholdOuter.of( OneOrTwoDoubles.of( 0.4 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        assertFalse( "Expected the threshold to differ on value.", unequalOnNullity.equals( unequalOnSecond ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#hashCode()} against the hashes of 
     * other instances.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                        Operator.GREATER,
                                                                                        ThresholdDataType.LEFT ) );
        OneOrTwoThresholds otherThresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
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
     * Constructs a {@link OneOrTwoThresholds} and compares to other instances using 
     * {@link OneOrTwoThresholds#compareTo(OneOrTwoThresholds)}.
     */

    @Test
    public void testCompareTo()
    {
        // Consistent with equals
        OneOrTwoThresholds first = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                   Operator.GREATER,
                                                                                   ThresholdDataType.LEFT ) );
        OneOrTwoThresholds second = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );

        assertTrue( "The thresholds are not comparable.",
                    first.compareTo( second ) == 0 );

        OneOrTwoThresholds third = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                                   Operator.GREATER,
                                                                                   ThresholdDataType.LEFT ) );

        // Anticommutative
        assertTrue( third.compareTo( first ) + first.compareTo( third ) == 0 );
        // Reflexive
        assertTrue( "Expected reflexive comparability.", first.compareTo( first ) == 0 );
        // Symmetric 
        assertTrue( "Expected symmetric comparability.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );

        // Transitive 
        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.3 ),
                                                                                    Operator.GREATER,
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
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 27.0 ),
                                                                        OneOrTwoDoubles.of( 0.5 ),
                                                                        Operator.GREATER_EQUAL,
                                                                        ThresholdDataType.LEFT ) );

        assertTrue( ">= 27.0 [Pr = 0.5]".equals( testString.toString() ) );

        OneOrTwoThresholds secondTestString =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 23.0 ),
                                                                        OneOrTwoDoubles.of( 0.2 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                       ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                           Operator.GREATER,
                                                                           ThresholdDataType.LEFT ) );

        assertTrue( "> 23.0 [Pr = 0.2] AND Pr > 0.1".equals( secondTestString.toString() ) );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests for exceptions.
     */

    @Test
    public void testExceptionOnConstructionWithNullInput()
    {
        assertThrows( NullPointerException.class, () -> OneOrTwoThresholds.of( null, null ) );
    }

}
