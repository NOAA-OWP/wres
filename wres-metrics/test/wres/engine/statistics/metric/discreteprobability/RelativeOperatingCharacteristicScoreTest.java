package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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
    public void setupBeforeEachTest()
    {
        this.rocScore = RelativeOperatingCharacteristicScore.of();
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(SampleData)} against 
     * expected output for a dataset with ties from Mason and Graham (2002).
     */

    @Test
    public void testApplyWithTies()
    {
        //Generate some data
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );

        SampleData<Pair<Probability, Probability>> input =
                SampleDataBasic.of( values, SampleMetadata.of() );

        //Metadata for the output
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.rocScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RelativeOperatingCharacteristicScore.MAIN )
                                                                               .setValue( 0.6785714285714286 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( RelativeOperatingCharacteristicScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} against 
     * expected output for a dataset without ties.
     */

    @Test
    public void testApplyWithoutTies()
    {
        //Generate some data
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.928 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.576 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.008 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.944 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.832 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.816 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.136 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.584 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.032 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.016 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.28 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.024 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.984 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.952 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Probability, Probability>> input = SampleDataBasic.of( values, meta );

        //Metadata for the output
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.rocScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RelativeOperatingCharacteristicScore.MAIN )
                                                                               .setValue( 0.75 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( RelativeOperatingCharacteristicScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );


        assertEquals( expected, actual );

        //Check against a baseline
        SampleData<Pair<Probability, Probability>> inputBase = SampleDataBasic.of( values, meta, values, meta, null );
        DoubleScoreStatisticOuter actualBase = this.rocScore.apply( inputBase );

        DoubleScoreStatisticComponent componentBase = DoubleScoreStatisticComponent.newBuilder()
                                                                                   .setMetric( RelativeOperatingCharacteristicScore.MAIN )
                                                                                   .setValue( 0.0 )
                                                                                   .build();

        DoubleScoreStatistic scoreBase = DoubleScoreStatistic.newBuilder()
                                                             .setMetric( RelativeOperatingCharacteristicScore.BASIC_METRIC )
                                                             .addStatistics( componentBase )
                                                             .build();

        DoubleScoreStatisticOuter expectedBase = DoubleScoreStatisticOuter.of( scoreBase, m1 );

        assertEquals( expectedBase, actualBase );
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} against 
     * expected output for a dataset with no occurrences.
     */

    @Test
    public void testApplyWithNoOccurrences()
    {
        //Generate some data
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.928 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.576 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.008 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.944 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.832 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.816 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.136 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.584 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.032 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.016 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.28 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.024 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.984 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.952 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        SampleData<Pair<Probability, Probability>> input = SampleDataBasic.of( values, meta );

        //Metadata for the output
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.rocScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RelativeOperatingCharacteristicScore.MAIN )
                                                                               .setValue( Double.NaN )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( RelativeOperatingCharacteristicScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Probability, Probability>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = this.rocScore.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#getName()} returns 
     * {@link MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( this.rocScore.getName()
                                 .equals( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.rocScore.isDecomposable() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.rocScore.isSkillScore() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#getScoreOutputGroup()} returns the result provided 
     * on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.rocScore.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( this.rocScore.isProper() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( this.rocScore.isStrictlyProper() );
    }

    /**
     * Checks that the baseline identifier is correctly propagated to the metric output metadata.
     * @throws IOException if the input pairs could not be read
     */

    @Test
    public void testMetadataContainsBaselineIdentifier() throws IOException
    {
        SampleData<Pair<Double, Ensemble>> pairs = MetricTestDataFactory.getEnsemblePairsOne();

        ThresholdOuter threshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT );

        Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> mapper =
                pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        SampleData<Pair<Probability, Probability>> transPairs = Slicer.transform( pairs, mapper );

        assertEquals( "ESP", this.rocScore.apply( transPairs )
                                 .getMetadata()
                                 .getIdentifier()
                                 .getScenarioNameForBaseline() );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link RelativeOperatingCharacteristicScore#apply(DiscreteProbabilityPairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'RELATIVE OPERATING CHARACTERISTIC SCORE'." );

        this.rocScore.apply( null );
    }

}
