package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.SafeThreshold.ThresholdBuilder;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

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
     * Data factory.
     */

    private static final DataFactory FACTORY = DefaultDataFactory.getInstance();

    /**
     * Tests {@link SafeThreshold#hashCode()}.
     */

    @Test
    public void testHashCode()
    {

        // One threshold
        Threshold first = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                .setCondition( Operator.GREATER_EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold second = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                 .setCondition( Operator.GREATER_EQUAL )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertEquals( "Expected equal hash codes.", first.hashCode(), second.hashCode() );

        // One probability threshold
        Threshold third =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fourth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( "Expected equal hash codes.", third.hashCode(), fourth.hashCode() );

        // One threshold with probability
        Threshold fifth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold sixth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER_EQUAL )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( "Expected equal hash codes.", fifth.hashCode(), sixth.hashCode() );

        // One threshold with probability and all data
        Threshold seventh =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold eighth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( "Expected equal hash codes.", seventh.hashCode(), eighth.hashCode() );

        // Two thresholds
        Threshold ninth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold tenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( "Expected equal hash codes.", ninth.hashCode(), tenth.hashCode() );

        // Two thresholds and probabilities
        Threshold eleventh =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold twelfth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertEquals( "Expected equal hash codes.", eleventh.hashCode(), twelfth.hashCode() );

        // All attributes defined   
        MetadataFactory fac = DefaultDataFactory.getInstance().getMetadataFactory();

        Threshold thirteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "a label" )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();
        Threshold fourteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.1 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.2 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "a label" )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();

        assertEquals( "Expected equal hash codes.", thirteenth.hashCode(), fourteenth.hashCode() );

    }

    /**
     * Tests {@link SafeThreshold#compareTo(Threshold)}.
     */

    @Test
    public void testCompareTo()
    {
        // Same conditions
        Threshold first = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                .setCondition( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold second = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                 .setCondition( Operator.GREATER )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertTrue( "Expected equal values.", first.compareTo( second ) == 0 );

        // Different conditions
        Threshold third = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                .setCondition( Operator.LESS )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertFalse( "Expected unequal values.", first.compareTo( third ) == 0 );

        // One has real values, the other probability values
        Threshold fourth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fifth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", fourth.compareTo( fifth ) == 0 );
        assertFalse( "Expected unequal values.", fifth.compareTo( fourth ) == 0 );

        // Both have real values, one has probability values
        Threshold sixth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", fourth.compareTo( sixth ) == 0 );
        assertFalse( "Expected unequal values.", sixth.compareTo( fourth ) == 0 );

        // Different ordinary values
        Threshold seventh =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", fourth.compareTo( seventh ) == 0 );

        // Equal probability values
        assertTrue( "Expected equal values.", fifth.compareTo( fifth ) == 0 );

        // Unequal probability values
        Threshold eighth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", fifth.compareTo( eighth ) == 0 );

        // One has a label, the other not
        Threshold ninth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();

        assertFalse( "Expected unequal values.", eighth.compareTo( ninth ) == 0 );
        assertFalse( "Expected unequal values.", ninth.compareTo( eighth ) == 0 );

        // Unequal labels
        Threshold tenth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "B" )
                                      .build();

        assertFalse( "Expected unequal values.", ninth.compareTo( tenth ) == 0 );
        assertFalse( "Expected unequal values.", tenth.compareTo( ninth ) == 0 );

        // Equal labels
        assertTrue( "Expected equal values.", tenth.compareTo( tenth ) == 0 );

        // Anticommutative
        assertTrue( "Expected anticommutative behaviour.",
                    Math.abs( third.compareTo( first ) ) == Math.abs( first.compareTo( third ) ) );
        // Reflexive
        assertTrue( "Expected reflexive comparability.", first.compareTo( first ) == 0 );
        // Symmetric 
        assertTrue( "Expected symmetric comparability.",
                    first.compareTo( second ) == 0 && second.compareTo( first ) == 0 );

        // Transitive 
        Threshold eleventh =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold twelfth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.9 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( "Expected transitive behaviour.",
                    twelfth.compareTo( eleventh ) > 0 && eleventh.compareTo( first ) > 0
                                                      && twelfth.compareTo( first ) > 0 );

        // Equal ordinary values for a between condition
        Threshold thirteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold fourteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( "Expected equal values.", thirteenth.compareTo( fourteenth ) == 0 );

        // Unequal ordinary values for between condition
        Threshold fifteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", thirteenth.compareTo( fifteenth ) == 0 );

        // Equal probability values for a between condition
        Threshold sixteenth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        Threshold seventeenth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( "Expected equal values.", sixteenth.compareTo( seventeenth ) == 0 );

        // Unequal ordinary values for between condition
        Threshold eighteenth =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal values.", sixteenth.compareTo( eighteenth ) == 0 );

        // Add units        
        MetadataFactory fac = DefaultDataFactory.getInstance().getMetadataFactory();

        Threshold nineteenth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();

        Threshold twentieth =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();

        assertFalse( "Expected unequal values.", sixteenth.compareTo( nineteenth ) == 0 );

        assertTrue( "Expected equal values.", nineteenth.compareTo( twentieth ) == 0 );

        // One has units, the other not
        Threshold twentyFirst =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();

        assertFalse( "Expected unequal values.", eighth.compareTo( twentyFirst ) == 0 );
        assertFalse( "Expected unequal values.", twentyFirst.compareTo( eighth ) == 0 );

        // Different units
        Threshold twentySecond =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CFS" ) )
                                      .build();

        assertFalse( "Expected unequal values.", twentyFirst.compareTo( twentySecond ) == 0 );

        // Different data types
        Threshold twentyThird =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.RIGHT )
                                      .setUnits( fac.getDimension( "CFS" ) )
                                      .build();

        assertFalse( "Expected unequal values.", twentySecond.compareTo( twentyThird ) == 0 );        
        
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
    public void testEquals()
    {
        Threshold left = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                               .setCondition( Operator.GREATER )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();
        Threshold otherLeft = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                    .setCondition( Operator.GREATER )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();
        Threshold right = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                .setCondition( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();
        Threshold full = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                               .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.7 ) )
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
        assertTrue( "Expected inequality on null.", !left.equals( null ) );

        //Check equiality with a label
        Threshold leftPlusLabel =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();
        Threshold rightPlusLabel =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "A" )
                                      .build();
        assertTrue( "Expected equal thresholds.", leftPlusLabel.equals( rightPlusLabel ) );

        // Unequal combinations
        // Combinations of the full threshold that are unequal
        // Unequal lower threshold
        Threshold fullDiffLower = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.05, 0.5 ) )
                                                        .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal lower probability
        Threshold fullDiffLowerProb = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                            .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.15, 0.7 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();
        // Unequal upper threshold
        Threshold fullDiffUpper = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.00, 0.55 ) )
                                                        .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "A" )
                                                        .build();
        // Unequal upper probability
        Threshold fullDiffUpperProb = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.00, 0.5 ) )
                                                            .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.77 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();
        // Unequal condition
        Threshold fullDiffCondition = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.00 ) )
                                                            .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1 ) )
                                                            .setCondition( Operator.GREATER )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();

        // Unequal label
        Threshold fullDiffLabel = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.55 ) )
                                                        .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.7 ) )
                                                        .setCondition( Operator.BETWEEN )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .setLabel( "B" )
                                                        .build();
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffLower ) );
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffUpper ) );
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffLowerProb ) );
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffUpperProb ) );
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffCondition ) );
        assertFalse( "Expected unequal thresholds.", full.equals( fullDiffLabel ) );

        // Differences based on real vs. probability thresholds
        Threshold noProbs = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();
        Threshold withProbs =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();
        assertFalse( "Expected unequal thresholds.", noProbs.equals( withProbs ) );
        assertFalse( "Expected unequal thresholds.", withProbs.equals( noProbs ) );

        Threshold bothRealNoProbs = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                          .setCondition( Operator.BETWEEN )
                                                          .setDataType( ThresholdDataType.LEFT )
                                                          .setLabel( "A" )
                                                          .build();

        Threshold bothRealBothProbs = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                            .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.1, 0.7 ) )
                                                            .setCondition( Operator.BETWEEN )
                                                            .setDataType( ThresholdDataType.LEFT )
                                                            .setLabel( "A" )
                                                            .build();

        assertFalse( "Expected unequal thresholds.", bothRealNoProbs.equals( bothRealBothProbs ) );

        // Differences on labels
        Threshold withLabel =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setLabel( "B" )
                                      .build();
        assertFalse( "Expected unequal thresholds.", noProbs.equals( withProbs ) );
        assertFalse( "Expected unequal thresholds.", noProbs.equals( withLabel ) );

        // Add units        
        MetadataFactory fac = DefaultDataFactory.getInstance().getMetadataFactory();

        // Differences on units
        Threshold cfs =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CFS" ) )
                                      .build();

        Threshold cms =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .setUnits( fac.getDimension( "CMS" ) )
                                      .build();

        Threshold noUnits =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected unequal units.", cfs.equals( cms ) );
        assertTrue( "Expected equal units.", cfs.equals( cfs ) );
        assertFalse( "Expected unequal units.", cfs.equals( noUnits ) );
        
        // Different data types
        Threshold noUnitsRightData =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 23.0, 57.0 ) )
                                      .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.2, 0.8 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.RIGHT )
                                      .build();        
        assertFalse( "Expected unequal data types.", noUnits.equals( noUnitsRightData ) );
        
    }

    /**
     * Tests the accessors to {@link SafeThreshold}.
     */

    @Test
    public void testAccessors()
    {
        Threshold threshold = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                    .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "a threshold" )
                                                    .build();

        // Test accessors
        assertTrue( "Unexpected threshold.", threshold.getValues().first().equals( 0.0 ) );
        assertTrue( "Unexpected upper threshold.", threshold.getValues().second().equals( 0.5 ) );
        assertTrue( "Unexpected probability threshold.", threshold.getProbabilities().first().equals( 0.0 ) );
        assertTrue( "Unexpected upper probability threshold.", threshold.getProbabilities().second().equals( 0.7 ) );
        assertTrue( "Unexpected condition.", threshold.getCondition() == Operator.BETWEEN );
        assertTrue( "Unexpected label.", threshold.getLabel().equals( "a threshold" ) );
    }

    /**
     * Tests the {@link SafeThreshold#toString()}.
     */

    @Test
    public void testToString()
    {
        MetadataFactory fac = DefaultDataFactory.getInstance().getMetadataFactory();

        // All data
        Threshold allData = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();
        Threshold allDataProb =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                      .setCondition( Operator.GREATER )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertTrue( "Unexpected inequality in string representations.",
                    allData.toString().equals( "All data" ) );

        assertTrue( "Unexpected inequality in string representations.",
                    allDataProb.toString().equals( "All data" ) );

        // One value threshold, no label
        Threshold oneValPlusLabel = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                          .setCondition( Operator.GREATER )
                                                          .setDataType( ThresholdDataType.LEFT )
                                                          .setUnits( fac.getDimension( "CMS" ) )
                                                          .build();

        assertTrue( "Unexpected inequality in string representations.",
                    oneValPlusLabel.toString().equals( "> 0.0 CMS" ) );

        // One probability and value threshold
        Threshold oneValOneProb = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                        .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                        .setCondition( Operator.GREATER )
                                                        .setDataType( ThresholdDataType.LEFT )
                                                        .build();

        assertTrue( "Unexpected inequality in string representations.",
                    oneValOneProb.toString().equals( "> 0.0 [Pr = 0.0]" ) );

        // One probability threshold
        Threshold oneProb = new ThresholdBuilder()
                                                  .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( "Unexpected inequality in string representations.", oneProb.toString().equals( "Pr > 0.0" ) );

        // Pair of probability thresholds
        Threshold twoProb = new ThresholdBuilder()
                                                  .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                  .setCondition( Operator.BETWEEN )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( "Unexpected inequality in string representations.",
                    twoProb.toString().equals( "Pr >= 0.0 AND < 0.5" ) );

        // Pair of value thresholds
        Threshold twoVal = new ThresholdBuilder()
                                                 .setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                 .setCondition( Operator.BETWEEN )
                                                 .setDataType( ThresholdDataType.LEFT )
                                                 .build();

        assertTrue( "Unexpected inequality in string representations.",
                    twoVal.toString().equals( ">= 0.0 AND < 0.5" ) );

        // All components
        Threshold threshold = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                    .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "a threshold" )
                                                    .setUnits( fac.getDimension( "CMS" ) )
                                                    .build();

        assertTrue( "Unexpected inequality in string representations.",
                    threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        // Test additional conditions
        Threshold less = new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.5 ) )
                                               .setCondition( Operator.LESS )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertTrue( "Unexpected inequality in string representations.", less.toString().equals( "Pr < 0.5" ) );

        Threshold lessEqual = new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.5 ) )
                                                    .setCondition( Operator.LESS_EQUAL )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertTrue( "Unexpected inequality in string representations.", lessEqual.toString().equals( "Pr <= 0.5" ) );

        Threshold greaterEqual = new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.5 ) )
                                                       .setCondition( Operator.GREATER_EQUAL )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .build();

        assertTrue( "Unexpected inequality in string representations.", greaterEqual.toString().equals( "Pr >= 0.5" ) );

        Threshold equal = new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.5 ) )
                                                .setCondition( Operator.EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( "Unexpected inequality in string representations.", equal.toString().equals( "Pr = 0.5" ) );

    }

    /**
     * Tests the {@link SafeThreshold#toStringSafe()}.
     */

    @Test
    public void testToStringSafe()
    {
        // All components
        Threshold threshold = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                    .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "a threshold" )
                                                    .build();

        assertTrue( "Unexpected inequality in string representations.",
                    threshold.toStringSafe().equals( "GTE_0.0_Pr_EQ_0.0_AND_LT_0.5_Pr_EQ_0.7_a_threshold" ) );

    }

    /**
     * Tests the {@link SafeThreshold#toStringWithoutUnits()}.
     */

    @Test
    public void testToStringWithoutUnits()
    {
        // All components
        MetadataFactory fac = DefaultDataFactory.getInstance().getMetadataFactory();
        Threshold threshold = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                    .setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.7 ) )
                                                    .setCondition( Operator.BETWEEN )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .setLabel( "a threshold" )
                                                    .setUnits( fac.getDimension( "CMS" ) )
                                                    .build();

        assertTrue( "Unexpected inequality in string representations.",
                    threshold.toString().equals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)" ) );

        assertTrue( "Unexpected inequality in string representations.",
                    threshold.toStringWithoutUnits().equals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)" ) );
    }

    /**
     * Tests the {@link SafeThreshold#test(Double)}.
     */

    @Test
    public void testTest()
    {
        // Operator.BETWEEN real values
        Threshold realVals = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                   .setCondition( Operator.BETWEEN )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( "Expected value to fall within threshold.", realVals.test( 0.25 ) );
        assertFalse( "Expected value to fall outside threshold.", realVals.test( 0.55 ) );
        assertFalse( "Expected value to fall outside threshold.", realVals.test( -0.1 ) );

        // Operator.BETWEEN probabilities
        Threshold probs = new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                .setCondition( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( "Expected value to fall within threshold.", probs.test( 0.25 ) );
        assertFalse( "Expected value to fall outside threshold.", probs.test( 0.55 ) );
        assertFalse( "Expected value to fall outside threshold.", probs.test( -0.1 ) );

        // Operator.GREATER
        Threshold greater = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                  .setCondition( Operator.GREATER )
                                                  .setDataType( ThresholdDataType.LEFT )
                                                  .build();

        assertTrue( "Expected value to fall above threshold.", greater.test( 0.25 ) );
        assertFalse( "Expected value to fall below threshold.", greater.test( -0.1 ) );

        // Operator.LESS
        Threshold less = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                               .setCondition( Operator.LESS )
                                               .setDataType( ThresholdDataType.LEFT )
                                               .build();

        assertFalse( "Expected value to fall above threshold.", less.test( 0.25 ) );
        assertTrue( "Expected value to fall below threshold.", less.test( -0.1 ) );

        // Operator.LESS_EQUAL
        Threshold lessEqual = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                    .setCondition( Operator.LESS_EQUAL )
                                                    .setDataType( ThresholdDataType.LEFT )
                                                    .build();

        assertFalse( "Expected value to fall above threshold.", lessEqual.test( 0.25 ) );
        assertTrue( "Expected value to fall on threshold.", lessEqual.test( -0.0 ) );

        // Operator.GREATER_EQUAL
        Threshold greaterEqual = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                       .setCondition( Operator.GREATER_EQUAL )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .setDataType( ThresholdDataType.LEFT )
                                                       .build();

        assertTrue( "Expected value to fall on threshold.", greaterEqual.test( -0.0 ) );
        assertFalse( "Expected value to fall below threshold.", greaterEqual.test( -0.1 ) );

        // Operator.EQUAL
        Threshold equal = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                                .setCondition( Operator.EQUAL )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .build();

        assertTrue( "Expected value to fall on threshold.", equal.test( -0.0 ) );
        assertFalse( "Expected value to fall below threshold.", equal.test( -0.1 ) );

    }

    /**
     * Tests the {@link SafeThreshold#isFinite()}.
     */

    @Test
    public void testIsFinite()
    {
        // Finite threshold
        Threshold realVals = new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 0.5 ) )
                                                   .setCondition( Operator.BETWEEN )
                                                   .setDataType( ThresholdDataType.LEFT )
                                                   .build();

        assertTrue( "Expected finite threshold.", realVals.isFinite() );

        // Infinite threshold lower bound
        Threshold infiniteLower =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY, 0.5 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected infinite threshold.", infiniteLower.isFinite() );

        // Infinite threshold upper bound
        Threshold infiniteUpper =
                new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, Double.POSITIVE_INFINITY ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected infinite threshold.", infiniteUpper.isFinite() );

        // Infinite threshold lower bound probability
        Threshold infiniteLowerprob =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY, 0.5 ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected infinite threshold.", infiniteLowerprob.isFinite() );

        // Infinite threshold upper bound probability
        Threshold infiniteUpperProb =
                new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, Double.POSITIVE_INFINITY ) )
                                      .setCondition( Operator.BETWEEN )
                                      .setDataType( ThresholdDataType.LEFT )
                                      .build();

        assertFalse( "Expected infinite threshold.", infiniteUpperProb.isFinite() );

    }

    /**
     * Tests for exceptional cases associated with the construction and user of a {@link SafeThreshold}.
     */

    @Test
    public void testExceptions()
    {
        // Check for construction without condition
        try
        {
            new ThresholdBuilder().build();
            fail( "Expected exception on constructing without condition." );
        }
        catch ( NullPointerException e )
        {
        }
        // Check for construction without thresholds
        try
        {
            new ThresholdBuilder().setCondition( Operator.GREATER ).setDataType( ThresholdDataType.LEFT ).build();
            fail( "Expected exception on constructing with all null thresholds." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Invalid probability threshold low
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( -1.0 ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing with invalid probability." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Invalid probability threshold high
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 2.0 ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing with invalid probability." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Label with an infinite threshold
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .setLabel( "A" )
                                  .build();
            fail( "Expected exception on constructing an infinite threshold with a label." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Inappropriate conditions for the thresholds available
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a threshold with an incorrect condition." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0, 1.0 ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a threshold with an incorrect condition." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Partially defined between condition
        try
        {
            new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                  .setCondition( Operator.BETWEEN )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a threshold with a missing upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                  .setCondition( Operator.BETWEEN )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a probability threshold with a missing upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        // Invalid threshold values
        try
        {
            new ThresholdBuilder().setValues( FACTORY.ofOneOrTwoDoubles( 1.0, 0.0 ) )
                                  .setCondition( Operator.BETWEEN )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a threshold with lower bound above the upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 1.0, 0.0 ) )
                                  .setCondition( Operator.BETWEEN )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a threshold with lower bound above the upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0, 2.0 ) )
                                  .setCondition( Operator.BETWEEN )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a probability threshold with an invalid upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 0.0 ) )
                                  .setCondition( Operator.LESS )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a probability threshold with an invalid operator on the "
                    + "lower bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        try
        {
            new ThresholdBuilder().setProbabilities( FACTORY.ofOneOrTwoDoubles( 1.0 ) )
                                  .setCondition( Operator.GREATER )
                                  .setDataType( ThresholdDataType.LEFT )
                                  .build();
            fail( "Expected exception on constructing a probability threshold with an invalid operator on the "
                    + "upper bound." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }
}
