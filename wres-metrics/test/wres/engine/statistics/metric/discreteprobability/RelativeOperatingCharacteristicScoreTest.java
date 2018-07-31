package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPair;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link RelativeOperatingCharacteristicScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RelativeOperatingCharacteristicScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link RelativeOperatingCharacteristicScore}.
     */

    private RelativeOperatingCharacteristicScore rocScore;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.rocScore = RelativeOperatingCharacteristicScore.of();
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} against 
     * expected output for a dataset with ties from Mason and Graham (2002).
     */

    @Test
    public void testApplyWithTies()
    {
        //Generate some data
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 ) );

        final DiscreteProbabilityPairs input =
                DiscreteProbabilityPairs.of( values, Metadata.of() );

        //Metadata for the output
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of(),
                                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                           MetricConstants.MAIN );

        //Check the results       
        final DoubleScoreOutput actual = rocScore.apply( input );
        final DoubleScoreOutput expected = DoubleScoreOutput.of( 0.6785714285714286, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} against 
     * expected output for a dataset without ties.
     */

    @Test
    public void testApplyWithoutTies()
    {
        //Generate some data
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 0.928 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.576 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.008 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.944 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.832 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.816 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.136 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.584 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.032 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.016 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.28 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.024 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.984 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.952 ) );
        Metadata meta = Metadata.of();
        DiscreteProbabilityPairs input = DiscreteProbabilityPairs.of( values, meta );

        //Metadata for the output
        MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                     MeasurementUnit.of(),
                                                                     MeasurementUnit.of(),
                                                                     MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                     MetricConstants.MAIN );

        //Check the results       
        DoubleScoreOutput actual = rocScore.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.75, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        //Check against a baseline
        DiscreteProbabilityPairs inputBase = DiscreteProbabilityPairs.of( values, values, meta, meta );
        DoubleScoreOutput actualBase = rocScore.apply( inputBase );
        DoubleScoreOutput expectedBase = DoubleScoreOutput.of( 0.0, m1 );
        assertTrue( "Actual: " + actualBase.getData()
                    + ". Expected: "
                    + expectedBase.getData()
                    + ".",
                    actualBase.equals( expectedBase ) );
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} against 
     * expected output for a dataset with no occurrences.
     */

    @Test
    public void testApplyWithNoOccurrences() throws MetricParameterException
    {
        //Generate some data
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 0.928 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.576 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.008 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.944 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.832 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.816 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.136 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.584 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.032 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.016 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.28 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.024 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.984 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.952 ) );
        Metadata meta = Metadata.of();

        DiscreteProbabilityPairs input = DiscreteProbabilityPairs.of( values, meta );

        //Metadata for the output
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of(),
                                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                           MetricConstants.MAIN );

        //Check the results       
        DoubleScoreOutput actual = rocScore.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( Double.NaN, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                DiscreteProbabilityPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = rocScore.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#getName()} returns 
     * {@link MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( rocScore.getName().equals( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rocScore.isDecomposable() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( rocScore.isSkillScore() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#getScoreOutputGroup()} returns the result provided 
     * on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rocScore.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( rocScore.isProper() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( rocScore.isStrictlyProper() );
    }

    /**
     * Checks that the baseline identifier is correctly propagated to the metric output metadata.
     * @throws IOException if the input pairs could not be read
     */

    @Test
    public void testMetadataContainsBaselineIdentifier() throws IOException
    {
        EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();

        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                                       Operator.GREATER,
                                                       ThresholdDataType.LEFT );

        BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> mapper =
                Slicer::toDiscreteProbabilityPair;

        DiscreteProbabilityPairs transPairs =
                Slicer.toDiscreteProbabilityPairs( pairs, threshold, mapper );

        assertTrue( rocScore.apply( transPairs )
                            .getMetadata()
                            .getIdentifier()
                            .getScenarioIDForBaseline()
                            .equals( "ESP" ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'RELATIVE OPERATING CHARACTERISTIC SCORE'." );

        rocScore.apply( null );
    }

}
