package wres.datamodel.thresholds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter.Builder;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link ThresholdOuter}. 
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ThresholdOuterTest
{

    private static final String THRESHOLD_LABEL = "a threshold";

    /**
     * Tests {@link ThresholdOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // One threshold
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.GREATER_EQUAL )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter second = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                             .setOperator( Operator.GREATER_EQUAL )
                                             .setDataType( ThresholdDataType.LEFT )
                                             .build();

        assertEquals( first.hashCode(), second.hashCode() );

        // One probability threshold
        ThresholdOuter third =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER_EQUAL )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter fourth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER_EQUAL )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertEquals( third.hashCode(), fourth.hashCode() );

        // One threshold with probability
        ThresholdOuter fifth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER_EQUAL )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter sixth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER_EQUAL )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertEquals( fifth.hashCode(), sixth.hashCode() );

        // One threshold with probability and all data
        ThresholdOuter seventh =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter eighth =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertEquals( seventh.hashCode(), eighth.hashCode() );

        // Two thresholds
        ThresholdOuter ninth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter tenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertEquals( ninth.hashCode(), tenth.hashCode() );

        // Two thresholds and probabilities
        ThresholdOuter eleventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter twelfth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertEquals( eleventh.hashCode(), twelfth.hashCode() );

        ThresholdOuter thirteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "a label" )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();
        ThresholdOuter fourteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "a label" )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertEquals( thirteenth.hashCode(), fourteenth.hashCode() );

    }

    /**
     * Tests {@link ThresholdOuter#compareTo(ThresholdOuter)}.
     */

    @Test
    public void testCompareTo()
    {
        // Same conditions
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.GREATER )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter second = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                             .setOperator( Operator.GREATER )
                                             .setDataType( ThresholdDataType.LEFT )
                                             .build();

        assertTrue( first.compareTo( second ) == 0 );

        // Different conditions
        ThresholdOuter third = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.LESS )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertFalse( first.compareTo( third ) == 0 );

        // One has real values, the other probability values
        ThresholdOuter fourth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter fifth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( fourth.compareTo( fifth ) == 0 );
        assertFalse( fifth.compareTo( fourth ) == 0 );

        // Both have real values, one has probability values
        ThresholdOuter sixth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( fourth.compareTo( sixth ) == 0 );
        assertFalse( sixth.compareTo( fourth ) == 0 );

        // Different ordinary values
        ThresholdOuter seventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( fourth.compareTo( seventh ) == 0 );

        // Equal probability values
        assertTrue( fifth.compareTo( fifth ) == 0 );

        // Unequal probability values
        ThresholdOuter eighth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( fifth.compareTo( eighth ) == 0 );

        // One has a label, the other not
        ThresholdOuter ninth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "A" )
                             .build();

        assertFalse( eighth.compareTo( ninth ) == 0 );
        assertFalse( ninth.compareTo( eighth ) == 0 );

        // Unequal labels
        ThresholdOuter tenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "B" )
                             .build();

        assertFalse( ninth.compareTo( tenth ) == 0 );
        assertFalse( tenth.compareTo( ninth ) == 0 );

        // Equal labels
        assertTrue( tenth.compareTo( tenth ) == 0 );

        // Anticommutative
        assertTrue( third.compareTo( first ) + first.compareTo( third ) == 0 );
        // Reflexive
        assertTrue( first.compareTo( first ) == 0 );
        // Symmetric 
        assertTrue( first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );

        // Transitive 
        ThresholdOuter eleventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter twelfth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.9 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertTrue( twelfth.compareTo( eleventh ) > 0 && eleventh.compareTo( first ) > 0
                    && twelfth.compareTo( first ) > 0 );

        // Equal ordinary values for a between condition
        ThresholdOuter thirteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter fourteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertTrue( thirteenth.compareTo( fourteenth ) == 0 );

        // Unequal ordinary values for between condition
        ThresholdOuter fifteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( thirteenth.compareTo( fifteenth ) == 0 );

        // Equal probability values for a between condition
        ThresholdOuter sixteenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        ThresholdOuter seventeenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertTrue( sixteenth.compareTo( seventeenth ) == 0 );

        // Unequal ordinary values for between condition
        ThresholdOuter eighteenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( sixteenth.compareTo( eighteenth ) == 0 );

        ThresholdOuter nineteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        ThresholdOuter twentieth =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertFalse( sixteenth.compareTo( nineteenth ) == 0 );

        assertTrue( nineteenth.compareTo( twentieth ) == 0 );

        // One has units, the other not
        ThresholdOuter twentyFirst =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertFalse( eighth.compareTo( twentyFirst ) == 0 );
        assertFalse( twentyFirst.compareTo( eighth ) == 0 );

        // Different units
        ThresholdOuter twentySecond =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertFalse( twentyFirst.compareTo( twentySecond ) == 0 );

        // Different data types
        ThresholdOuter twentyThird =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.RIGHT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertFalse( twentySecond.compareTo( twentyThird ) == 0 );
    }

    /**
     * Tests {@link ThresholdOuter#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        ThresholdOuter left = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                           .setOperator( Operator.GREATER )
                                           .setDataType( ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter otherLeft = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setOperator( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        ThresholdOuter right = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.GREATER )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter full = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                           .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                           .setOperator( Operator.BETWEEN )
                                           .setDataType( ThresholdDataType.LEFT )
                                           .setLabel( "A" )
                                           .build();
        //Equal
        assertTrue( "Expected equal values.", left.equals( right ) );
        assertTrue( "Expected equal values.", full.equals( full ) );

        //Reflexive
        assertEquals( "Expected reflexive equality.", left, left );
        //Symmetric 
        assertTrue( "Expected symmetric equality.", left.equals( right ) && right.equals( left ) );
        //Transitive 
        assertTrue( "Expected transitive equality.",
                    left.equals( right ) && left.equals( otherLeft ) && otherLeft.equals( right ) );
        //Nullity
        assertNotEquals( null, left );

        //Check equiality with a label
        ThresholdOuter leftPlusLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "A" )
                             .build();
        ThresholdOuter rightPlusLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "A" )
                             .build();
        assertTrue( "Expected equal thresholds.", leftPlusLabel.equals( rightPlusLabel ) );

        // Unequal combinations
        // Combinations of the full threshold that are unequal
        // Unequal lower threshold
        ThresholdOuter fullDiffLower = new Builder().setValues( OneOrTwoDoubles.of( 0.05, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "A" )
                                                    .build();
        // Unequal lower probability
        ThresholdOuter fullDiffLowerProb = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.15,
                                                                                               0.7 ) )
                                                        .setOperator( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal upper threshold
        ThresholdOuter fullDiffUpper = new Builder().setValues( OneOrTwoDoubles.of( 0.00, 0.55 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "A" )
                                                    .build();
        // Unequal upper probability
        ThresholdOuter fullDiffUpperProb = new Builder().setValues( OneOrTwoDoubles.of( 0.00, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                               0.77 ) )
                                                        .setOperator( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal condition
        ThresholdOuter fullDiffCondition = new Builder().setValues( OneOrTwoDoubles.of( 0.00 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                        .setOperator( Operator.GREATER )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();

        // Unequal label
        ThresholdOuter fullDiffLabel = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.55 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "B" )
                                                    .build();
        assertFalse( full.equals( fullDiffLower ) );
        assertFalse( full.equals( fullDiffUpper ) );
        assertFalse( full.equals( fullDiffLowerProb ) );
        assertFalse( full.equals( fullDiffUpperProb ) );
        assertFalse( full.equals( fullDiffCondition ) );
        assertFalse( full.equals( fullDiffLabel ) );

        // Differences based on real vs. probability thresholds
        ThresholdOuter noProbs = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( Operator.GREATER )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();
        ThresholdOuter withProbs =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();
        assertFalse( noProbs.equals( withProbs ) );
        assertFalse( withProbs.equals( noProbs ) );

        ThresholdOuter bothRealNoProbs = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                      .setOperator( Operator.BETWEEN )
                                                      .setDataType( ThresholdDataType.LEFT )
                                                      .setLabel( "A" )
                                                      .build();

        ThresholdOuter bothRealBothProbs = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                               0.7 ) )
                                                        .setOperator( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();

        assertFalse( bothRealNoProbs.equals( bothRealBothProbs ) );

        // Differences on labels
        ThresholdOuter withLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "B" )
                             .build();
        assertFalse( noProbs.equals( withProbs ) );
        assertFalse( noProbs.equals( withLabel ) );

        // Differences on units
        ThresholdOuter cfs =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        ThresholdOuter cms =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        ThresholdOuter noUnits =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( cfs.equals( cms ) );
        assertTrue( cfs.equals( cfs ) );
        assertFalse( cfs.equals( noUnits ) );

        // Different data types
        ThresholdOuter noUnitsRightData =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.RIGHT )
                             .build();
        assertFalse( noUnits.equals( noUnitsRightData ) );

    }

    /**
     * Tests the accessors to {@link ThresholdOuter}.
     */

    @Test
    public void testAccessors()
    {
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .build();

        // Test accessors
        assertTrue( threshold.getValues().first().equals( 0.0 ) );
        assertTrue( threshold.getValues().second().equals( 0.5 ) );
        assertTrue( threshold.getProbabilities().first().equals( 0.0 ) );
        assertTrue( threshold.getProbabilities().second().equals( 0.7 ) );
        assertTrue( threshold.getOperator() == Operator.BETWEEN );
        assertTrue( threshold.getLabel().equals( THRESHOLD_LABEL ) );
    }

    /**
     * Tests the {@link ThresholdOuter#toString()}.
     */

    @Test
    public void testToString()
    {

        // All data
        ThresholdOuter allData = ThresholdOuter.ALL_DATA;

        assertEquals( "All data", allData.toString() );

        // One value threshold, no label
        ThresholdOuter oneValPlusLabel = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                      .setOperator( Operator.GREATER )
                                                      .setDataType( ThresholdDataType.LEFT )
                                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                                      .build();

        assertTrue( oneValPlusLabel.toString().equals( "> 0.0 CMS" ) );

        // One probability and value threshold
        ThresholdOuter oneValOneProb = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setOperator( Operator.GREATER )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertTrue( oneValOneProb.toString().equals( "> 0.0 [Pr = 0.0]" ) );

        // One probability threshold
        ThresholdOuter oneProb = new Builder()
                                              .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( Operator.GREATER )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();

        assertTrue( oneProb.toString().equals( "Pr > 0.0" ) );

        // Pair of probability thresholds
        ThresholdOuter twoProb = new Builder()
                                              .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                              .setOperator( Operator.BETWEEN )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();

        assertTrue( twoProb.toString().equals( "Pr >= 0.0 AND < 0.5" ) );

        // Pair of value thresholds
        ThresholdOuter twoVal = new Builder()
                                             .setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                             .setOperator( Operator.BETWEEN )
                                             .setDataType( ThresholdDataType.LEFT )
                                             .build();

        assertTrue( twoVal.toString().equals( ">= 0.0 AND < 0.5" ) );

        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertTrue( threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        // Test additional conditions
        ThresholdOuter less = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                           .setOperator( Operator.LESS )
                                           .setDataType( ThresholdDataType.LEFT )
                                           .build();

        assertTrue( less.toString().equals( "Pr < 0.5" ) );

        ThresholdOuter lessEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                .setOperator( Operator.LESS_EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( lessEqual.toString().equals( "Pr <= 0.5" ) );

        ThresholdOuter greaterEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                   .setOperator( Operator.GREATER_EQUAL )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( greaterEqual.toString().equals( "Pr >= 0.5" ) );

        ThresholdOuter equal = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                            .setOperator( Operator.EQUAL )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertTrue( equal.toString().equals( "Pr = 0.5" ) );

    }

    /**
     * Tests the {@link ThresholdOuter#toStringSafe()}.
     */

    @Test
    public void testToStringSafe()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .build();

        assertEquals( "GTE_0.0_Pr_EQ_0.0_AND_LT_0.5_Pr_EQ_0.7_a_threshold", threshold.toStringSafe() );

    }

    /**
     * See #79746
     */

    @Test
    public void testToStringSafeEliminatesReservedCharactersInUnits()
    {
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 23.0 ) )
                                                .setOperator( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setUnits( MeasurementUnit.of( "ft3/s" ) )
                                                .build();

        assertEquals( "GT_23.0_ft3s", threshold.toStringSafe() );
    }

    /**
     * Tests the {@link ThresholdOuter#toStringWithoutUnits()}.
     */

    @Test
    public void testToStringWithoutUnits()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertTrue( threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        assertTrue( threshold.toStringWithoutUnits().equals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)" ) );
    }

    /**
     * Tests the {@link ThresholdOuter#test(Double)}.
     */

    @Test
    public void testTest()
    {
        // Operator.BETWEEN real values
        ThresholdOuter realVals = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                               .setOperator( Operator.BETWEEN )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertTrue( realVals.test( 0.25 ) );
        assertFalse( realVals.test( 0.55 ) );
        assertFalse( realVals.test( -0.1 ) );

        // Operator.BETWEEN probabilities
        ThresholdOuter probs = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                            .setOperator( Operator.BETWEEN )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertTrue( probs.test( 0.25 ) );
        assertFalse( probs.test( 0.55 ) );
        assertFalse( probs.test( -0.1 ) );

        // Operator.GREATER
        ThresholdOuter greater = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( Operator.GREATER )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();

        assertTrue( greater.test( 0.25 ) );
        assertFalse( greater.test( -0.1 ) );

        // Operator.LESS
        ThresholdOuter less = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                           .setOperator( Operator.LESS )
                                           .setDataType( ThresholdDataType.LEFT )
                                           .build();

        assertFalse( less.test( 0.25 ) );
        assertTrue( less.test( -0.1 ) );

        // Operator.LESS_EQUAL
        ThresholdOuter lessEqual = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setOperator( Operator.LESS_EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertFalse( lessEqual.test( 0.25 ) );
        assertTrue( lessEqual.test( -0.0 ) );

        // Operator.GREATER_EQUAL
        ThresholdOuter greaterEqual = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                   .setOperator( Operator.GREATER_EQUAL )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( greaterEqual.test( -0.0 ) );
        assertFalse( greaterEqual.test( -0.1 ) );

        // Operator.EQUAL
        ThresholdOuter equal = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.EQUAL )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertTrue( equal.test( -0.0 ) );
        assertFalse( equal.test( -0.1 ) );

    }

    /**
     * Tests the {@link ThresholdOuter#isFinite()}.
     */

    @Test
    public void testIsFinite()
    {
        // Finite threshold
        ThresholdOuter realVals = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                               .setOperator( Operator.BETWEEN )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertTrue( realVals.isFinite() );

        // Infinite threshold lower bound
        ThresholdOuter infiniteLower =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY, 0.5 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( infiniteLower.isFinite() );

        // Infinite threshold upper bound
        ThresholdOuter infiniteUpper =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, Double.POSITIVE_INFINITY ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( infiniteUpper.isFinite() );

        // Infinite threshold lower bound probability
        ThresholdOuter infiniteLowerprob =
                new Builder().setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY,
                                                                    0.5 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( infiniteLowerprob.isFinite() );

        // Infinite threshold upper bound probability
        ThresholdOuter infiniteUpperProb =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0,
                                                                    Double.POSITIVE_INFINITY ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertFalse( infiniteUpperProb.isFinite() );

    }

    @Test
    public void testBuildFromExistingThreshold()
    {
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                            .setOperator( Operator.GREATER_EQUAL )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        ThresholdOuter actual = new Builder( first.getThreshold() ).setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                                   .build();

        ThresholdOuter expected = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                               .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                               .setOperator( Operator.GREATER_EQUAL )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testExceptionOnConstructionWithoutCondition()
    {
        assertThrows( NullPointerException.class, () -> new Builder().build() );
    }

    @Test
    public void testExceptionOnConstructionWithNullThresholds()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionWithNegativeProbability()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( -1.0 ) )
                                         .setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionWithProbabilityGreaterThanOne()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 2.0 ) )
                                         .setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAThresholdThatChecksForProbabilitiesGreaterThanOne()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                         .setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfATwoSidedThresholdWithoutABetweenCondition()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                         .setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAMissingUpperBound()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                         .setOperator( Operator.BETWEEN )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithAMissingUpperBound()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                         .setOperator( Operator.BETWEEN )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithALowerBoundAboveTheUpperBound()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setValues( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                         .setOperator( Operator.BETWEEN )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundAboveTheUpperBound()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                         .setOperator( Operator.BETWEEN )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAnUpperBoundAboveOne()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 2.0 ) )
                                         .setOperator( Operator.BETWEEN )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundBelowZero()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                         .setOperator( Operator.LESS )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithASingleBoundAboveOne()
    {
        assertThrows( ThresholdException.class,
                      () -> new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0 ) )
                                         .setOperator( Operator.GREATER )
                                         .setDataType( ThresholdDataType.LEFT )
                                         .build() );
    }

    @Test
    public void testExceptionOnComparingWithNullInput()
    {
        ThresholdOuter threshold =
                ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ), Operator.GREATER, ThresholdDataType.LEFT );

        assertThrows( NullPointerException.class,
                      () -> threshold.compareTo( null ) );
    }

}
