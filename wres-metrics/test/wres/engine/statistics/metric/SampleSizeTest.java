package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link SampleSize}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SampleSizeTest
{

    /**
     * Constructs a {@link SampleSize} and compares the actual result to the expected result. Also, checks the 
     * parameters of the metric.
     */

    @Test
    public void testSampleSize()
    {
        //Obtain the factories

        //Generate some data
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of(),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of( "COUNT" ),
                                                     MetricConstants.SAMPLE_SIZE,
                                                     MetricConstants.MAIN );

        //Build the metric
        SampleSize<SampleData<Pair<Double, Double>>> ss = SampleSize.of();

        //Check the results
        DoubleScoreStatisticOuter actual = ss.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( input.getRawData().size() )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( SampleSize.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );

        //Check the parameters
        assertTrue( "Unexpected name for the Sample Size.",
                    ss.getName().equals( MetricConstants.SAMPLE_SIZE.toString() ) );
        assertTrue( "The Sample Size is not decomposable.", !ss.isDecomposable() );
        assertTrue( "The Sample Size is not a skill score.", !ss.isSkillScore() );
        assertTrue( "The Sample Size cannot be decomposed.", ss.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Constructs a {@link SampleSize} and checks for exceptional cases.
     */

    @Test
    public void testExceptions()
    {
        //Build the metric
        SampleSize<SampleData<Pair<Double, Double>>> ss = SampleSize.of();

        SampleDataException expected = assertThrows( SampleDataException.class, () -> ss.apply( null ) );

        assertEquals( "Specify non-null input to the 'SAMPLE SIZE'.", expected.getMessage() );
    }


}
