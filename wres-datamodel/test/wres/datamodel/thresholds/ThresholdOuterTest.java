package wres.datamodel.thresholds;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter.Builder;

/**
 * Tests the {@link ThresholdOuter}. 
 *
 * @author James Brown
 */
final class ThresholdOuterTest
{
    private static final String THRESHOLD_LABEL = "a threshold";

    /**
     * Tests {@link ThresholdOuter#hashCode()}.
     */

    @Test
    void testHashCode()
    {
        // One threshold
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( ThresholdOperator.GREATER_EQUAL )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();
        ThresholdOuter second = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                             .setOperator( ThresholdOperator.GREATER_EQUAL )
                                             .setDataType( ThresholdOrientation.LEFT )
                                             .build();

        assertEquals( first.hashCode(), second.hashCode() );

        // One probability threshold
        ThresholdOuter third =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER_EQUAL )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter fourth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER_EQUAL )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( third.hashCode(), fourth.hashCode() );

        // One threshold with probability
        ThresholdOuter fifth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER_EQUAL )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter sixth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER_EQUAL )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( fifth.hashCode(), sixth.hashCode() );

        // One threshold with probability and all data
        ThresholdOuter seventh =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter eighth =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( seventh.hashCode(), eighth.hashCode() );

        // Two thresholds
        ThresholdOuter ninth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter tenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( ninth.hashCode(), tenth.hashCode() );

        // Two thresholds and probabilities
        ThresholdOuter eleventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter twelfth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( eleventh.hashCode(), twelfth.hashCode() );

        ThresholdOuter thirteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "a label" )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();
        ThresholdOuter fourteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "a label" )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertEquals( thirteenth.hashCode(), fourteenth.hashCode() );

    }

    /**
     * Tests {@link ThresholdOuter#compareTo(ThresholdOuter)}.
     */

    @Test
    void testCompareTo()
    {
        // Same conditions
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( ThresholdOperator.GREATER )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();
        ThresholdOuter second = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                             .setOperator( ThresholdOperator.GREATER )
                                             .setDataType( ThresholdOrientation.LEFT )
                                             .build();

        assertEquals( 0, first.compareTo( second ) );

        // Different conditions
        ThresholdOuter third = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( ThresholdOperator.LESS )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();

        assertNotEquals( 0, first.compareTo( third ) );

        // One has real values, the other probability values
        ThresholdOuter fourth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter fifth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, fourth.compareTo( fifth ) );
        assertNotEquals( 0, fifth.compareTo( fourth ) );

        // Both have real values, one has probability values
        ThresholdOuter sixth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, fourth.compareTo( sixth ) );
        assertNotEquals( 0, sixth.compareTo( fourth ) );

        // Different ordinary values
        ThresholdOuter seventh =
                new Builder().setValues( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, fourth.compareTo( seventh ) );

        // Equal probability values
        assertEquals( 0, fifth.compareTo( fifth ) );

        // Unequal probability values
        ThresholdOuter eighth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, fifth.compareTo( eighth ) );

        // One has a label, the other not
        ThresholdOuter ninth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "A" )
                             .build();

        assertNotEquals( 0, eighth.compareTo( ninth ) );
        assertNotEquals( 0, ninth.compareTo( eighth ) );

        // Unequal labels
        ThresholdOuter tenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
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
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter twelfth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.9 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertTrue( twelfth.compareTo( eleventh ) > 0 && eleventh.compareTo( first ) > 0
                    && twelfth.compareTo( first ) > 0 );

        // Equal ordinary values for a between condition
        ThresholdOuter thirteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter fourteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( 0, thirteenth.compareTo( fourteenth ) );

        // Unequal ordinary values for between condition
        ThresholdOuter fifteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, thirteenth.compareTo( fifteenth ) );

        // Equal probability values for a between condition
        ThresholdOuter sixteenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        ThresholdOuter seventeenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertEquals( 0, sixteenth.compareTo( seventeenth ) );

        // Unequal ordinary values for between condition
        ThresholdOuter eighteenth =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( 0, sixteenth.compareTo( eighteenth ) );

        ThresholdOuter nineteenth =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        ThresholdOuter twentieth =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertNotEquals( 0, sixteenth.compareTo( nineteenth ) );

        assertEquals( 0, nineteenth.compareTo( twentieth ) );

        // One has units, the other not
        ThresholdOuter twentyFirst =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        assertNotEquals( 0, eighth.compareTo( twentyFirst ) );
        assertNotEquals( 0, twentyFirst.compareTo( eighth ) );

        // Different units
        ThresholdOuter twentySecond =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertNotEquals( 0, twentyFirst.compareTo( twentySecond ) );

        // Different data types
        ThresholdOuter twentyThird =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.RIGHT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        assertNotEquals( 0, twentySecond.compareTo( twentyThird ) );
    }

    /**
     * Tests {@link ThresholdOuter#equals(Object)}.
     */

    @Test
    void testEquals()
    {
        ThresholdOuter left = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                           .setOperator( ThresholdOperator.GREATER )
                                           .setDataType( ThresholdOrientation.LEFT )
                                           .build();
        ThresholdOuter otherLeft = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setOperator( ThresholdOperator.GREATER )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .build();
        ThresholdOuter right = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( ThresholdOperator.GREATER )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();
        ThresholdOuter full = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                           .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                           .setOperator( ThresholdOperator.BETWEEN )
                                           .setDataType( ThresholdOrientation.LEFT )
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
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "A" )
                             .build();
        ThresholdOuter rightPlusLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "A" )
                             .build();
        assertEquals( leftPlusLabel, rightPlusLabel );

        // Unequal combinations
        // Combinations of the full threshold that are unequal
        // Unequal lower threshold
        ThresholdOuter fullDiffLower = new Builder().setValues( OneOrTwoDoubles.of( 0.05, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( ThresholdOperator.BETWEEN )
                                                    .setDataType( ThresholdOrientation.LEFT )
                                                    .setLabel( "A" )
                                                    .build();
        // Unequal lower probability
        ThresholdOuter fullDiffLowerProb = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.15,
                                                                                               0.7 ) )
                                                        .setOperator( ThresholdOperator.BETWEEN )
                                                        .setDataType( ThresholdOrientation.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal upper threshold
        ThresholdOuter fullDiffUpper = new Builder().setValues( OneOrTwoDoubles.of( 0.00, 0.55 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( ThresholdOperator.BETWEEN )
                                                    .setDataType( ThresholdOrientation.LEFT )
                                                    .setLabel( "A" )
                                                    .build();
        // Unequal upper probability
        ThresholdOuter fullDiffUpperProb = new Builder().setValues( OneOrTwoDoubles.of( 0.00, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                               0.77 ) )
                                                        .setOperator( ThresholdOperator.BETWEEN )
                                                        .setDataType( ThresholdOrientation.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal condition
        ThresholdOuter fullDiffCondition = new Builder().setValues( OneOrTwoDoubles.of( 0.00 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                        .setOperator( ThresholdOperator.GREATER )
                                                        .setDataType( ThresholdOrientation.LEFT )
                                                        .setLabel( "A" )
                                                        .build();

        // Unequal label
        ThresholdOuter fullDiffLabel = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.55 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                    .setOperator( ThresholdOperator.BETWEEN )
                                                    .setDataType( ThresholdOrientation.LEFT )
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
                                              .setOperator( ThresholdOperator.GREATER )
                                              .setDataType( ThresholdOrientation.LEFT )
                                              .build();
        ThresholdOuter withProbs =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();
        assertNotEquals( noProbs, withProbs );
        assertNotEquals( withProbs, noProbs );

        ThresholdOuter bothRealNoProbs = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                      .setOperator( ThresholdOperator.BETWEEN )
                                                      .setDataType( ThresholdOrientation.LEFT )
                                                      .setLabel( "A" )
                                                      .build();

        ThresholdOuter bothRealBothProbs = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                               0.7 ) )
                                                        .setOperator( ThresholdOperator.BETWEEN )
                                                        .setDataType( ThresholdOrientation.LEFT )
                                                        .setLabel( "A" )
                                                        .build();

        assertNotEquals( bothRealNoProbs, bothRealBothProbs );

        // Differences on labels
        ThresholdOuter withLabel =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                             .setOperator( ThresholdOperator.GREATER )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setLabel( "B" )
                             .build();
        assertNotEquals( noProbs, withProbs );
        assertNotEquals( noProbs, withLabel );

        // Differences on units
        ThresholdOuter cfs =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CFS" ) )
                             .build();

        ThresholdOuter cms =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .setUnits( MeasurementUnit.of( "CMS" ) )
                             .build();

        ThresholdOuter noUnits =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertNotEquals( cfs, cms );
        assertEquals( cfs, cfs );
        assertNotEquals( cfs, noUnits );

        // Different data types
        ThresholdOuter noUnitsRightData =
                new Builder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                             .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.RIGHT )
                             .build();
        assertNotEquals( noUnits, noUnitsRightData );

    }

    /**
     * Tests the accessors to {@link ThresholdOuter}.
     */

    @Test
    void testAccessors()
    {
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( ThresholdOperator.BETWEEN )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .build();

        // Test accessors
        assertEquals( 0.0, threshold.getValues().first(), 0.01 );
        assertEquals( 0.5, threshold.getValues().second(), 0.01 );
        assertEquals( 0.0, threshold.getProbabilities().first(), 0.01 );
        assertEquals( 0.7, threshold.getProbabilities().second(), 0.01 );
        assertSame( ThresholdOperator.BETWEEN, threshold.getOperator() );
        assertEquals( THRESHOLD_LABEL, threshold.getLabel() );
    }

    /**
     * Tests the {@link ThresholdOuter#toString()}.
     */

    @Test
    void testToString()
    {

        // All data
        ThresholdOuter allData = ThresholdOuter.ALL_DATA;

        assertEquals( "All data", allData.toString() );

        // One value threshold, no label
        ThresholdOuter oneValPlusLabel = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                      .setOperator( ThresholdOperator.GREATER )
                                                      .setDataType( ThresholdOrientation.LEFT )
                                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                                      .build();

        assertEquals( "> 0.0 CMS", oneValPlusLabel.toString() );

        // One probability and value threshold
        ThresholdOuter oneValOneProb = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setOperator( ThresholdOperator.GREATER )
                                                    .setDataType( ThresholdOrientation.LEFT )
                                                    .build();

        assertEquals( "> 0.0 [Pr = 0.0]", oneValOneProb.toString() );

        // One probability threshold
        ThresholdOuter oneProb = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( ThresholdOperator.GREATER )
                                              .setDataType( ThresholdOrientation.LEFT )
                                              .build();

        assertEquals( "Pr > 0.0", oneProb.toString() );

        // Pair of probability thresholds
        ThresholdOuter twoProb = new Builder()
                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                .setOperator( ThresholdOperator.BETWEEN )
                .setDataType( ThresholdOrientation.LEFT )
                .build();

        assertEquals( "Pr >= 0.0 AND < 0.5", twoProb.toString() );

        // Pair of value thresholds
        ThresholdOuter twoVal = new Builder()
                .setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                .setOperator( ThresholdOperator.BETWEEN )
                .setDataType( ThresholdOrientation.LEFT )
                .build();

        assertEquals( ">= 0.0 AND < 0.5", twoVal.toString() );

        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( ThresholdOperator.BETWEEN )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertEquals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)", threshold.toString() );

        // Test additional conditions
        ThresholdOuter less = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                           .setOperator( ThresholdOperator.LESS )
                                           .setDataType( ThresholdOrientation.LEFT )
                                           .build();

        assertEquals( "Pr < 0.5", less.toString() );

        ThresholdOuter lessEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                .setOperator( ThresholdOperator.LESS_EQUAL )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .build();

        assertEquals( "Pr <= 0.5", lessEqual.toString() );

        ThresholdOuter greaterEqual = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                   .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                   .setDataType( ThresholdOrientation.LEFT )
                                                   .build();

        assertEquals( "Pr >= 0.5", greaterEqual.toString() );

        ThresholdOuter equal = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                            .setOperator( ThresholdOperator.EQUAL )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();

        assertEquals( "Pr = 0.5", equal.toString() );

        ThresholdOuter greaterEqualNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN ) )
                                                      .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                      .setDataType( ThresholdOrientation.LEFT )
                                                      .setLabel( "FOO" )
                                                      .build();

        assertEquals( ">= FOO", greaterEqualNaN.toString() );

        ThresholdOuter betweenNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN, 1.0 ) )
                                                 .setOperator( ThresholdOperator.BETWEEN )
                                                 .setDataType( ThresholdOrientation.LEFT )
                                                 .setLabel( "FOO" )
                                                 .build();

        assertEquals( "FOO", betweenNaN.toString() );

        ThresholdOuter quantileBetweenNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN, 1.0 ) )
                                                         .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.3 ) )
                                                         .setOperator( ThresholdOperator.BETWEEN )
                                                         .setDataType( ThresholdOrientation.LEFT )
                                                         .setLabel( "FOO" )
                                                         .build();

        assertEquals( ">= Pr = 0.1 AND < Pr = 0.3 (FOO)", quantileBetweenNaN.toString() );

        ThresholdOuter quantileNaN = new Builder().setValues( OneOrTwoDoubles.of( Double.NaN ) )
                                                  .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                  .setOperator( ThresholdOperator.LESS )
                                                  .setDataType( ThresholdOrientation.LEFT )
                                                  .setLabel( "FOO" )
                                                  .build();

        assertEquals( "< Pr = 0.1 (FOO)", quantileNaN.toString() );
    }

    /**
     * Tests the {@link ThresholdOuter#test(double)}}.
     */

    @Test
    void testTest()
    {
        // BETWEEN real values
        ThresholdOuter realVals = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                               .setOperator( ThresholdOperator.BETWEEN )
                                               .setDataType( ThresholdOrientation.LEFT )
                                               .build();

        assertTrue( realVals.test( 0.25 ) );
        assertFalse( realVals.test( 0.55 ) );
        assertFalse( realVals.test( -0.1 ) );

        // BETWEEN probabilities
        ThresholdOuter probs = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                            .setOperator( ThresholdOperator.BETWEEN )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();

        assertTrue( probs.test( 0.25 ) );
        assertFalse( probs.test( 0.55 ) );
        assertFalse( probs.test( -0.1 ) );

        // GREATER
        ThresholdOuter greater = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                              .setOperator( ThresholdOperator.GREATER )
                                              .setDataType( ThresholdOrientation.LEFT )
                                              .build();

        assertTrue( greater.test( 0.25 ) );
        assertFalse( greater.test( -0.1 ) );

        // LESS
        ThresholdOuter less = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                           .setOperator( ThresholdOperator.LESS )
                                           .setDataType( ThresholdOrientation.LEFT )
                                           .build();

        assertFalse( less.test( 0.25 ) );
        assertTrue( less.test( -0.1 ) );

        // LESS_EQUAL
        ThresholdOuter lessEqual = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setOperator( ThresholdOperator.LESS_EQUAL )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .build();

        assertFalse( lessEqual.test( 0.25 ) );
        assertTrue( lessEqual.test( -0.0 ) );

        // GREATER_EQUAL
        ThresholdOuter greaterEqual = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                   .setOperator( ThresholdOperator.GREATER_EQUAL )
                                                   .setDataType( ThresholdOrientation.LEFT )
                                                   .setDataType( ThresholdOrientation.LEFT )
                                                   .build();

        assertTrue( greaterEqual.test( -0.0 ) );
        assertFalse( greaterEqual.test( -0.1 ) );

        // EQUAL
        ThresholdOuter equal = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setOperator( ThresholdOperator.EQUAL )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();

        assertTrue( equal.test( -0.0 ) );
        assertFalse( equal.test( -0.1 ) );

    }

    /**
     * Tests the {@link ThresholdOuter#isFinite()}.
     */

    @Test
    void testIsFinite()
    {
        // Finite threshold
        ThresholdOuter realVals = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                               .setOperator( ThresholdOperator.BETWEEN )
                                               .setDataType( ThresholdOrientation.LEFT )
                                               .build();

        assertTrue( realVals.isFinite() );

        // Infinite threshold lower bound
        ThresholdOuter infiniteLower =
                new Builder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY, 0.5 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertFalse( infiniteLower.isFinite() );

        // Infinite threshold upper bound
        ThresholdOuter infiniteUpper =
                new Builder().setValues( OneOrTwoDoubles.of( 0.0, Double.POSITIVE_INFINITY ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertFalse( infiniteUpper.isFinite() );

        // Infinite threshold lower bound probability
        ThresholdOuter infiniteLowerprob =
                new Builder().setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY,
                                                                    0.5 ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertFalse( infiniteLowerprob.isFinite() );

        // Infinite threshold upper bound probability
        ThresholdOuter infiniteUpperProb =
                new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0,
                                                                    Double.POSITIVE_INFINITY ) )
                             .setOperator( ThresholdOperator.BETWEEN )
                             .setDataType( ThresholdOrientation.LEFT )
                             .build();

        assertFalse( infiniteUpperProb.isFinite() );

    }

    @Test
    void testBuildFromExistingThreshold()
    {
        ThresholdOuter first = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                            .setProbabilities( OneOrTwoDoubles.of( 0.3 ) )
                                            .setOperator( ThresholdOperator.GREATER_EQUAL )
                                            .setDataType( ThresholdOrientation.LEFT )
                                            .build();

        ThresholdOuter actual = new Builder( first.getThreshold() ).setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                                   .build();

        ThresholdOuter expected = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                               .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                               .setOperator( ThresholdOperator.GREATER_EQUAL )
                                               .setDataType( ThresholdOrientation.LEFT )
                                               .build();

        assertEquals( expected, actual );
    }

    @Test
    void testExceptionOnConstructionWithNullThresholds()
    {
        Builder builder = new Builder().setOperator( ThresholdOperator.GREATER )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "Specify one or more values" ) );
    }

    @Test
    void testExceptionOnConstructionWithNegativeProbability()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( -1.0 ) )
                                       .setOperator( ThresholdOperator.GREATER )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "out of bounds" ) );
    }

    @Test
    void testExceptionOnConstructionWithProbabilityGreaterThanOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 2.0 ) )
                                       .setOperator( ThresholdOperator.GREATER )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "out of bounds" ) );
    }

    @Test
    void testExceptionOnConstructionOfAThresholdThatChecksForProbabilitiesGreaterThanOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.1 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "out of bounds" ) );
    }

    @Test
    void testExceptionOnConstructionOfATwoSidedThresholdWithoutABetweenCondition()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                       .setOperator( ThresholdOperator.GREATER )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class,
                                             builder::build );
        assertTrue( e.getMessage().contains( "appropriate BETWEEN condition" ) );
    }

    @Test
    void testExceptionOnConstructionOfAProbabilityThresholdWithAMissingUpperBound()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "thresholds must be defined in pairs" ) );
    }

    @Test
    void testExceptionOnConstructionOfAValueThresholdWithAMissingUpperBound()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "thresholds must be defined in pairs" ) );
    }

    @Test
    void testExceptionOnConstructionOfAValueThresholdWithALowerBoundAboveTheUpperBound()
    {
        Builder builder = new Builder().setValues( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "must be greater than" ) );
    }

    @Test
    void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundAboveTheUpperBound()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "must be greater than" ) );
    }

    @Test
    void testExceptionOnConstructionOfAProbabilityThresholdWithAnUpperBoundAboveOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0, 2.0 ) )
                                       .setOperator( ThresholdOperator.BETWEEN )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "out of bounds" ) );
    }

    @Test
    void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundBelowZero()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                       .setOperator( ThresholdOperator.LESS )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e = assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "Cannot apply a threshold operator of '<'") );
    }

    @Test
    void testExceptionOnConstructionOfAProbabilityThresholdWithASingleBoundAboveOne()
    {
        Builder builder = new Builder().setProbabilities( OneOrTwoDoubles.of( 1.0 ) )
                                       .setOperator( ThresholdOperator.GREATER )
                                       .setDataType( ThresholdOrientation.LEFT );

        ThresholdException e =assertThrows( ThresholdException.class, builder::build );
        assertTrue( e.getMessage().contains( "Cannot apply a threshold operator of '>'") );
    }

    @Test
    void testExceptionOnComparingWithNullInput()
    {
        ThresholdOuter threshold =
                ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ), ThresholdOperator.GREATER, ThresholdOrientation.LEFT );

        assertThrows( NullPointerException.class,
                      () -> threshold.compareTo( null ) );
    }

}
