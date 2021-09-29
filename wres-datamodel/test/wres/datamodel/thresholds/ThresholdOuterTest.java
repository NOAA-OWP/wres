package wres.datamodel.thresholds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
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

        assertEquals( 0, first.compareTo( second ) );

        // Different conditions
        ThresholdOuter third = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( Operator.LESS )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertNotEquals( 0, first.compareTo( third ) );

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

        assertNotEquals( 0, fourth.compareTo( fifth ) );
        assertNotEquals( 0, fifth.compareTo( fourth ) );

        // Both have real values, one has probability values
        ThresholdOuter sixth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertNotEquals( 0, fourth.compareTo( sixth ) );
        assertNotEquals( 0, sixth.compareTo( fourth ) );

        // Different ordinary values
        ThresholdOuter seventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertNotEquals( 0, fourth.compareTo( seventh ) );

        // Equal probability values
        assertEquals( 0, fifth.compareTo( fifth ) );

        // Unequal probability values
        ThresholdOuter eighth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertNotEquals( 0, fifth.compareTo( eighth ) );

        // One has a label, the other not
        ThresholdOuter ninth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "A" )
                             .build();

        assertNotEquals( 0, eighth.compareTo( ninth ) );
        assertNotEquals( 0, ninth.compareTo( eighth ) );

        // Unequal labels
        ThresholdOuter tenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "B" )
                             .build();

        assertNotEquals( 0, ninth.compareTo( tenth ) );
        assertNotEquals( 0, tenth.compareTo( ninth ) );

        // Equal labels
        assertEquals( 0, tenth.compareTo( tenth ) );

        // Anticommutative
        assertEquals( 0, third.compareTo( first ) + first.compareTo( third ) );
        // Reflexive
        assertEquals( 0, first.compareTo( first ) );
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

        assertEquals( 0, thirteenth.compareTo( fourteenth ) );

        // Unequal ordinary values for between condition
        ThresholdOuter fifteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertNotEquals( 0, thirteenth.compareTo( fifteenth ) );

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

        assertEquals( 0, sixteenth.compareTo( seventeenth ) );

        // Unequal ordinary values for between condition
        ThresholdOuter eighteenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.LEFT )
                             .build();

        assertNotEquals( 0, sixteenth.compareTo( eighteenth ) );

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

        assertNotEquals( 0, sixteenth.compareTo( nineteenth ) );

        assertEquals( 0, nineteenth.compareTo( twentieth ) );

        // One has units, the other not
        ThresholdOuter twentyFirst =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertNotEquals( 0, eighth.compareTo( twentyFirst ) );
        assertNotEquals( 0, twentyFirst.compareTo( eighth ) );

        // Different units
        ThresholdOuter twentySecond =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertNotEquals( 0, twentyFirst.compareTo( twentySecond ) );

        // Different data types
        ThresholdOuter twentyThird =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.RIGHT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertNotEquals( 0, twentySecond.compareTo( twentyThird ) );
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
        assertEquals( left, right );
        assertEquals( full, full );

        //Reflexive
        assertEquals( left, left );
        //Symmetric 
        assertTrue( left.equals( right ) && right.equals( left ) );
        //Transitive 
        assertTrue( left.equals( right ) && left.equals( otherLeft ) && otherLeft.equals( right ) );
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
        assertEquals( leftPlusLabel, rightPlusLabel );

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
        assertNotEquals( full, fullDiffLower );
        assertNotEquals( full, fullDiffUpper );
        assertNotEquals( full, fullDiffLowerProb );
        assertNotEquals( full, fullDiffUpperProb );
        assertNotEquals( full, fullDiffCondition );
        assertNotEquals( full, fullDiffLabel );

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
        assertNotEquals( noProbs, withProbs );
        assertNotEquals( withProbs, noProbs );

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

        assertNotEquals( bothRealNoProbs, bothRealBothProbs );

        // Differences on labels
        ThresholdOuter withLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( Operator.GREATER )
                             .setDataType( ThresholdDataType.LEFT )
                             .setLabel( "B" )
                             .build();
        assertNotEquals( noProbs, withProbs );
        assertNotEquals( noProbs, withLabel );

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

        assertNotEquals( cfs, cms );
        assertEquals( cfs, cfs );
        assertNotEquals( cfs, noUnits );

        // Different data types
        ThresholdOuter noUnitsRightData =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( Operator.BETWEEN )
                             .setDataType( ThresholdDataType.RIGHT )
                             .build();
        assertNotEquals( noUnits, noUnitsRightData );

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
        assertEquals( 0.0, threshold.getValues().first(), 0.01 );
        assertEquals( 0.5, threshold.getValues().second(), 0.01 );
        assertEquals( 0.0, threshold.getProbabilities().first(), 0.01 );
        assertEquals( 0.7, threshold.getProbabilities().second(), 0.01 );
        assertSame( Operator.BETWEEN, threshold.getOperator() );
        assertEquals( THRESHOLD_LABEL, threshold.getLabel() );
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

        assertEquals( "> 0.0 CMS", oneValPlusLabel.toString() );

        // One probability and value threshold
        ThresholdOuter oneValOneProb = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setOperator( Operator.GREATER )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertEquals( "> 0.0 [Pr = 0.0]", oneValOneProb.toString() );

        // One probability threshold
        ThresholdOuter oneProb = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( Operator.GREATER )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();

        assertEquals( "Pr > 0.0", oneProb.toString() );

        // Pair of probability thresholds
        ThresholdOuter twoProb = new Builder()
                                              .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                              .setOperator( Operator.BETWEEN )
                                              .setDataType( ThresholdDataType.LEFT )
                                              .build();

        assertEquals( "Pr >= 0.0 AND < 0.5", twoProb.toString() );

        // Pair of value thresholds
        ThresholdOuter twoVal = new Builder()
                                             .setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                             .setOperator( Operator.BETWEEN )
                                             .setDataType( ThresholdDataType.LEFT )
                                             .build();

        assertEquals( ">= 0.0 AND < 0.5", twoVal.toString() );

        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertEquals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)", threshold.toString() );

        // Test additional conditions
        ThresholdOuter less = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                           .setOperator( Operator.LESS )
                                           .setDataType( ThresholdDataType.LEFT )
                                           .build();

        assertEquals( "Pr < 0.5", less.toString() );

        ThresholdOuter lessEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                .setOperator( Operator.LESS_EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertEquals( "Pr <= 0.5", lessEqual.toString() );

        ThresholdOuter greaterEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                   .setOperator( Operator.GREATER_EQUAL )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertEquals( "Pr >= 0.5", greaterEqual.toString() );

        ThresholdOuter equal = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                            .setOperator( Operator.EQUAL )
                                            .setDataType( ThresholdDataType.LEFT )
                                            .build();

        assertEquals( "Pr = 0.5", equal.toString() );

        ThresholdOuter greaterEqualNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN ) )
                                                      .setOperator( Operator.GREATER_EQUAL )
                                                      .setDataType( ThresholdDataType.LEFT )
                                                      .setLabel( "FOO" )
                                                      .build();

        assertEquals( ">= FOO", greaterEqualNaN.toString() );

        ThresholdOuter betweenNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN, 1.0 ) )
                                                 .setOperator( Operator.BETWEEN )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .setLabel( "FOO" )
                                                 .build();

        assertEquals( "FOO", betweenNaN.toString() );

        ThresholdOuter quantileBetweenNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN, 1.0 ) )
                                                         .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.3 ) )
                                                         .setOperator( Operator.BETWEEN )
                                                         .setDataType( ThresholdDataType.LEFT )
                                                         .setLabel( "FOO" )
                                                         .build();

        assertEquals( ">= Pr = 0.1 AND < Pr = 0.3 (FOO)", quantileBetweenNaN.toString() );

        ThresholdOuter quantileNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN ) )
                                                  .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                  .setOperator( Operator.LESS )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .setLabel( "FOO" )
                                                  .build();

        assertEquals( "< Pr = 0.1 (FOO)", quantileNaN.toString() );
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

        assertEquals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)", threshold.toString() );

        assertEquals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)", threshold.toStringWithoutUnits() );
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
        Builder builder = new Builder();
        assertThrows( NullPointerException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionWithNullThresholds()
    {
        Builder builder = new Builder().setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionWithNegativeProbability()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( -1.0 ) )
                                       .setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionWithProbabilityGreaterThanOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 2.0 ) )
                                       .setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAThresholdThatChecksForProbabilitiesGreaterThanOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                       .setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfATwoSidedThresholdWithoutABetweenCondition()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                       .setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class,
                      () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAMissingUpperBound()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( Operator.BETWEEN )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithAMissingUpperBound()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( Operator.BETWEEN )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithALowerBoundAboveTheUpperBound()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                       .setOperator( Operator.BETWEEN )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundAboveTheUpperBound()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                       .setOperator( Operator.BETWEEN )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAnUpperBoundAboveOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 2.0 ) )
                                       .setOperator( Operator.BETWEEN )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundBelowZero()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( Operator.LESS )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithASingleBoundAboveOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0 ) )
                                       .setOperator( Operator.GREATER )
                                       .setDataType( ThresholdDataType.LEFT );

        assertThrows( ThresholdException.class, () -> builder.build() );
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
