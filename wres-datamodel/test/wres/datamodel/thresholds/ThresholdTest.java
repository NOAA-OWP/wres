package wres.datamodel.thresholds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold.ThresholdBuilder;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link Threshold}. 
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ThresholdTest
{

    private static final String THRESHOLD_LABEL = "a threshold";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests {@link Threshold#hashCode()}.
     */

    @Test
    public void testHashCode()
    {

        // One threshold
        Threshold first = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setCondition( Operator.GREATER_EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold second = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                 .setCondition( Operator.GREATER_EQUAL )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertEquals( first.hashCode(), second.hashCode() );

        // One probability threshold
        Threshold third =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fourth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( third.hashCode(), fourth.hashCode() );

        // One threshold with probability
        Threshold fifth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold sixth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( fifth.hashCode(), sixth.hashCode() );

        // One threshold with probability and all data
        Threshold seventh =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold eighth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( seventh.hashCode(), eighth.hashCode() );

        // Two thresholds
        Threshold ninth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold tenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( ninth.hashCode(), tenth.hashCode() );

        // Two thresholds and probabilities
        Threshold eleventh =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold twelfth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( eleventh.hashCode(), twelfth.hashCode() );

        Threshold thirteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "a label" )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();
        Threshold fourteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.1 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "a label" )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();

        assertEquals( thirteenth.hashCode(), fourteenth.hashCode() );

    }

    /**
     * Tests {@link Threshold#compareTo(Threshold)}.
     */

    @Test
    public void testCompareTo()
    {
        // Same conditions
        Threshold first = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setCondition( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold second = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                 .setCondition( Operator.GREATER )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertTrue( first.compareTo( second ) == 0 );

        // Different conditions
        Threshold third = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setCondition( Operator.LESS )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertFalse( first.compareTo( third ) == 0 );

        // One has real values, the other probability values
        Threshold fourth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fifth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( fourth.compareTo( fifth ) == 0 );
        assertFalse( fifth.compareTo( fourth ) == 0 );

        // Both have real values, one has probability values
        Threshold sixth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( fourth.compareTo( sixth ) == 0 );
        assertFalse( sixth.compareTo( fourth ) == 0 );

        // Different ordinary values
        Threshold seventh =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( fourth.compareTo( seventh ) == 0 );

        // Equal probability values
        assertTrue( fifth.compareTo( fifth ) == 0 );

        // Unequal probability values
        Threshold eighth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( fifth.compareTo( eighth ) == 0 );

        // One has a label, the other not
        Threshold ninth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();

        assertFalse( eighth.compareTo( ninth ) == 0 );
        assertFalse( ninth.compareTo( eighth ) == 0 );

        // Unequal labels
        Threshold tenth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
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
        Threshold eleventh =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold twelfth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.9 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( twelfth.compareTo( eleventh ) > 0 && eleventh.compareTo( first ) > 0
                    && twelfth.compareTo( first ) > 0 );

        // Equal ordinary values for a between condition
        Threshold thirteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fourteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( thirteenth.compareTo( fourteenth ) == 0 );

        // Unequal ordinary values for between condition
        Threshold fifteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( thirteenth.compareTo( fifteenth ) == 0 );

        // Equal probability values for a between condition
        Threshold sixteenth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold seventeenth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( sixteenth.compareTo( seventeenth ) == 0 );

        // Unequal ordinary values for between condition
        Threshold eighteenth =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( sixteenth.compareTo( eighteenth ) == 0 );

        Threshold nineteenth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();

        Threshold twentieth =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();

        assertFalse( sixteenth.compareTo( nineteenth ) == 0 );

        assertTrue( nineteenth.compareTo( twentieth ) == 0 );

        // One has units, the other not
        Threshold twentyFirst =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();

        assertFalse( eighth.compareTo( twentyFirst ) == 0 );
        assertFalse( twentyFirst.compareTo( eighth ) == 0 );

        // Different units
        Threshold twentySecond =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CFS" ) )
                                      .build();

        assertFalse( twentyFirst.compareTo( twentySecond ) == 0 );

        // Different data types
        Threshold twentyThird =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.RIGHT )
                                      .setUnits( MeasurementUnit.of( "CFS" ) )
                                      .build();

        assertFalse( twentySecond.compareTo( twentyThird ) == 0 );
    }

    /**
     * Tests {@link Threshold#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        Threshold left = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                               .setCondition( Operator.GREATER )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();
        Threshold otherLeft = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setCondition( Operator.GREATER )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();
        Threshold right = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setCondition( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold full = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                               .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                               .setCondition( Operator.BETWEEN )
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
        Threshold leftPlusLabel =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();
        Threshold rightPlusLabel =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();
        assertTrue( "Expected equal thresholds.", leftPlusLabel.equals( rightPlusLabel ) );

        // Unequal combinations
        // Combinations of the full threshold that are unequal
        // Unequal lower threshold
        Threshold fullDiffLower = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.05, 0.5 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal lower probability
        Threshold fullDiffLowerProb = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.15,
                                                                                                   0.7 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();
        // Unequal upper threshold
        Threshold fullDiffUpper = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.00, 0.55 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal upper probability
        Threshold fullDiffUpperProb = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.00, 0.5 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                                   0.77 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();
        // Unequal condition
        Threshold fullDiffCondition = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.00 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.1 ) )
                                                            .setCondition( Operator.GREATER )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();

        // Unequal label
        Threshold fullDiffLabel = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.55 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
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
        Threshold noProbs = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();
        Threshold withProbs =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        assertFalse( noProbs.equals( withProbs ) );
        assertFalse( withProbs.equals( noProbs ) );

        Threshold bothRealNoProbs = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                          .setCondition( Operator.BETWEEN )
                                                          .setDataType( ThresholdDataType.LEFT )
                                                          .setLabel( "A" )
                                                          .build();

        Threshold bothRealBothProbs = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                            .setProbabilities( OneOrTwoDoubles.of( 0.1,
                                                                                                   0.7 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();

        assertFalse( bothRealNoProbs.equals( bothRealBothProbs ) );

        // Differences on labels
        Threshold withLabel =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "B" )
                                      .build();
        assertFalse( noProbs.equals( withProbs ) );
        assertFalse( noProbs.equals( withLabel ) );

        // Differences on units
        Threshold cfs =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CFS" ) )
                                      .build();

        Threshold cms =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( MeasurementUnit.of( "CMS" ) )
                                      .build();

        Threshold noUnits =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( cfs.equals( cms ) );
        assertTrue( cfs.equals( cfs ) );
        assertFalse( cfs.equals( noUnits ) );

        // Different data types
        Threshold noUnitsRightData =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0, 57.0 ) )
                                      .setProbabilities( OneOrTwoDoubles.of( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.RIGHT )
                                      .build();
        assertFalse( noUnits.equals( noUnitsRightData ) );

    }

    /**
     * Tests the accessors to {@link Threshold}.
     */

    @Test
    public void testAccessors()
    {
        Threshold threshold = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
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
     * Tests the {@link Threshold#toString()}.
     */

    @Test
    public void testToString()
    {

        // All data
        Threshold allData =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold allDataProb =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( allData.toString().equals( "All data" ) );

        assertTrue( allDataProb.toString().equals( "All data" ) );

        // One value threshold, no label
        Threshold oneValPlusLabel = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                          .setCondition( Operator.GREATER )
                                                          .setDataType( ThresholdDataType.LEFT )
                                                          .setUnits( MeasurementUnit.of( "CMS" ) )
                                                          .build();

        assertTrue( oneValPlusLabel.toString().equals( "> 0.0 CMS" ) );

        // One probability and value threshold
        Threshold oneValOneProb = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                        .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                                        .setCondition( Operator.GREATER )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .build();

        assertTrue( oneValOneProb.toString().equals( "> 0.0 [Pr = 0.0]" ) );

        // One probability threshold
        Threshold oneProb = new ThresholdBuilder()
                                                  .setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( oneProb.toString().equals( "Pr > 0.0" ) );

        // Pair of probability thresholds
        Threshold twoProb = new ThresholdBuilder()
                                                  .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                  .setCondition( Operator.BETWEEN )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( twoProb.toString().equals( "Pr >= 0.0 AND < 0.5" ) );

        // Pair of value thresholds
        Threshold twoVal = new ThresholdBuilder()
                                                 .setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                 .setCondition( Operator.BETWEEN )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertTrue( twoVal.toString().equals( ">= 0.0 AND < 0.5" ) );

        // All components
        Threshold threshold = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( THRESHOLD_LABEL )
                                                    .setUnits( MeasurementUnit.of( "CMS" ) )
                                                    .build();

        assertTrue( threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        // Test additional conditions
        Threshold less = new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                               .setCondition( Operator.LESS )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertTrue( less.toString().equals( "Pr < 0.5" ) );

        Threshold lessEqual = new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                    .setCondition( Operator.LESS_EQUAL )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertTrue( lessEqual.toString().equals( "Pr <= 0.5" ) );

        Threshold greaterEqual = new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                       .setCondition( Operator.GREATER_EQUAL )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .build();

        assertTrue( greaterEqual.toString().equals( "Pr >= 0.5" ) );

        Threshold equal = new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.5 ) )
                                                .setCondition( Operator.EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( equal.toString().equals( "Pr = 0.5" ) );

    }

    /**
     * Tests the {@link Threshold#toStringSafe()}.
     */

    @Test
    public void testToStringSafe()
    {
        // All components
        Threshold threshold = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
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
        Threshold threshold = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 23.0 ) )
                                                    .setCondition( Operator.GREATER )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setUnits( MeasurementUnit.of( "ft3/s" ) )
                                                    .build();

        assertEquals( "GT_23.0_ft3s", threshold.toStringSafe() );
    }
    
    /**
     * Tests the {@link Threshold#toStringWithoutUnits()}.
     */

    @Test
    public void testToStringWithoutUnits()
    {
        // All components
        Threshold threshold = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                    .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( THRESHOLD_LABEL )
                                                    .setUnits( MeasurementUnit.of( "CMS" ) )
                                                    .build();

        assertTrue( threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        assertTrue( threshold.toStringWithoutUnits().equals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)" ) );
    }

    /**
     * Tests the {@link Threshold#test(Double)}.
     */

    @Test
    public void testTest()
    {
        // Operator.BETWEEN real values
        Threshold realVals = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                   .setCondition( Operator.BETWEEN )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( realVals.test( 0.25 ) );
        assertFalse( realVals.test( 0.55 ) );
        assertFalse( realVals.test( -0.1 ) );

        // Operator.BETWEEN probabilities
        Threshold probs = new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setCondition( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( probs.test( 0.25 ) );
        assertFalse( probs.test( 0.55 ) );
        assertFalse( probs.test( -0.1 ) );

        // Operator.GREATER
        Threshold greater = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( greater.test( 0.25 ) );
        assertFalse( greater.test( -0.1 ) );

        // Operator.LESS
        Threshold less = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                               .setCondition( Operator.LESS )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertFalse( less.test( 0.25 ) );
        assertTrue( less.test( -0.1 ) );

        // Operator.LESS_EQUAL
        Threshold lessEqual = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                    .setCondition( Operator.LESS_EQUAL )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertFalse( lessEqual.test( 0.25 ) );
        assertTrue( lessEqual.test( -0.0 ) );

        // Operator.GREATER_EQUAL
        Threshold greaterEqual = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                       .setCondition( Operator.GREATER_EQUAL )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .build();

        assertTrue( greaterEqual.test( -0.0 ) );
        assertFalse( greaterEqual.test( -0.1 ) );

        // Operator.EQUAL
        Threshold equal = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                                                .setCondition( Operator.EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( equal.test( -0.0 ) );
        assertFalse( equal.test( -0.1 ) );

    }

    /**
     * Tests the {@link Threshold#isFinite()}.
     */

    @Test
    public void testIsFinite()
    {
        // Finite threshold
        Threshold realVals = new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                   .setCondition( Operator.BETWEEN )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( realVals.isFinite() );

        // Infinite threshold lower bound
        Threshold infiniteLower =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY, 0.5 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( infiniteLower.isFinite() );

        // Infinite threshold upper bound
        Threshold infiniteUpper =
                new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, Double.POSITIVE_INFINITY ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( infiniteUpper.isFinite() );

        // Infinite threshold lower bound probability
        Threshold infiniteLowerprob =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY,
                                                                             0.5 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( infiniteLowerprob.isFinite() );

        // Infinite threshold upper bound probability
        Threshold infiniteUpperProb =
                new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0,
                                                                             Double.POSITIVE_INFINITY ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( infiniteUpperProb.isFinite() );

    }

    @Test
    public void testExceptionOnConstructionWithoutCondition()
    {
        exception.expect( NullPointerException.class );
        new ThresholdBuilder().build();

    }

    @Test
    public void testExceptionOnConstructionWithNullThresholds()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setCondition( Operator.GREATER ).setDataType( ThresholdDataType.LEFT ).build();
    }

    @Test
    public void testExceptionOnConstructionWithNegativeProbability()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( -1.0 ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionWithProbabilityGreaterThanOne()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 2.0 ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAnInfiniteThresholdWithALabel()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .setLabel( "A" )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAThresholdThatChecksForProbabilitiesGreaterThanOne()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfATwoSidedThresholdWithoutABetweenCondition()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0, 1.0 ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAMissingUpperBound()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                              .setCondition( Operator.BETWEEN )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithAMissingUpperBound()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 0.0 ) )
                              .setCondition( Operator.BETWEEN )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAValueThresholdWithALowerBoundAboveTheUpperBound()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setValues( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                              .setCondition( Operator.BETWEEN )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundAboveTheUpperBound()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 1.0, 0.0 ) )
                              .setCondition( Operator.BETWEEN )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithAnUpperBoundAboveOne()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0, 2.0 ) )
                              .setCondition( Operator.BETWEEN )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithALowerBoundBelowZero()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 0.0 ) )
                              .setCondition( Operator.LESS )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();

    }

    @Test
    public void testExceptionOnConstructionOfAProbabilityThresholdWithASingleBoundAboveOne()
    {
        exception.expect( IllegalArgumentException.class );
        new ThresholdBuilder().setProbabilities( OneOrTwoDoubles.of( 1.0 ) )
                              .setCondition( Operator.GREATER )
                              .setDataType( ThresholdDataType.LEFT )
                              .build();
    }

    @Test
    public void testExceptionOnComparingWithNullInput()
    {
        exception.expect( NullPointerException.class );
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 1.0 ), Operator.GREATER, ThresholdDataType.LEFT );

        threshold.compareTo( null );
    }

}
