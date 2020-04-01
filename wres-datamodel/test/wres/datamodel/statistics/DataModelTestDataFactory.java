package wres.datamodel.statistics;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DataModelTestDataFactory
{

    /**
     * Second time for testing.
     */

    private static final String SECOND_TIME = "2010-12-31T11:59:59Z";

    /**
     * First time for testing.
     */

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /**
     * Returns a {@link List} of {@link ScoreStatistic} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getScalarMetricOutputOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatistic> getScalarMetricOutputOne()
    {

        List<DoubleScoreStatistic> statistics = new ArrayList<>();

        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getScalarMetricOutputOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Source metadata
            final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                             DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                   "SQIN",
                                                                                   "HEFS",
                                                                                   "ESP" ) );

            //Iterate through the lead times
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = ( (Double) d.next().getKey() );
                final TimeWindow timeWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                             Instant.parse( SECOND_TIME ),
                                                             Duration.ofHours( (int) leadTime ) );
                final MetricResultByThreshold t =
                        (MetricResultByThreshold) data.getResult( timeWindow.getLatestLeadDuration().toHours() );
                final Iterator<MetricResultKey> e = t.getIterator();
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final OneOrTwoThresholds q =
                            OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                                  OneOrTwoDoubles.of( probConstants[0] ),
                                                                                  Operator.GREATER,
                                                                                  ThresholdDataType.LEFT ) );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreStatistic value =
                            DoubleScoreStatistic.of( res[0],
                                                     StatisticMetadata.of( SampleMetadata.of( source, timeWindow, q ),
                                                                           1000,
                                                                           MeasurementUnit.of(),
                                                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                           MetricConstants.MAIN ) );

                    //Append result
                    statistics.add( value );
                }

            }

        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        
        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link ScoreStatistic} comprising the MAE for selected
     * thresholds and forecast lead times using fake data.
     * 
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatistic> getScalarMetricOutputTwo()
    {

        List<DoubleScoreStatistic> statistics = new ArrayList<>();

        //Fake metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS",
                                                                               "ESP" ) );

        int[] leadTimes = new int[] { 1, 2, 3, 4, 5 };

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindow timeWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                         Instant.parse( SECOND_TIME ),
                                                         Duration.ofHours( leadTime ) );

            // Add first result
            OneOrTwoThresholds first =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                          OneOrTwoDoubles.of( 0.1 ),
                                                                          Operator.GREATER,
                                                                          ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT ) );

            DoubleScoreStatistic firstValue =
                    DoubleScoreStatistic.of( 66.0,
                                             StatisticMetadata.of( SampleMetadata.of( source, timeWindow, first ),
                                                                   1000,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                   MetricConstants.MAIN ) );

            statistics.add( firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 2.0 ),
                                                                          OneOrTwoDoubles.of( 0.2 ),
                                                                          Operator.GREATER,
                                                                          ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT ) );

            DoubleScoreStatistic secondValue =
                    DoubleScoreStatistic.of( 67.0,
                                             StatisticMetadata.of( SampleMetadata.of( source, timeWindow, second ),
                                                                   1000,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                   MetricConstants.MAIN ) );

            statistics.add( secondValue );


            // Add third result
            OneOrTwoThresholds third =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 3.0 ),
                                                                          OneOrTwoDoubles.of( 0.3 ),
                                                                          Operator.GREATER,
                                                                          ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 6.0 ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT ) );


            DoubleScoreStatistic thirdValue =
                    DoubleScoreStatistic.of( 68.0,
                                             StatisticMetadata.of( SampleMetadata.of( source, timeWindow, third ),
                                                                   1000,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                   MetricConstants.MAIN ) );

            statistics.add( thirdValue );

        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getVectorMetricOutputOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatistic> getVectorMetricOutputOne()
    {

        final List<DoubleScoreStatistic> statistics = new ArrayList<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getVectorMetricOutputOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Fake metadata
            final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CFS" ),
                                                             DatasetIdentifier.of( Location.of( "NPTP1" ),
                                                                                   "SQIN",
                                                                                   "HEFS",
                                                                                   "ESP" ) );

            //Iterate through the lead times
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = (Double) d.next().getKey();
                final TimeWindow timeWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                             Instant.parse( SECOND_TIME ),
                                                             Duration.ofHours( (int) leadTime ) );
                final MetricResultByThreshold t =
                        (MetricResultByThreshold) data.getResult( timeWindow.getLatestLeadDuration().toHours() );
                final Iterator<MetricResultKey> e = t.getIterator();
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final OneOrTwoThresholds q =
                            OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                                  OneOrTwoDoubles.of( probConstants[0] ),
                                                                                  Operator.GREATER,
                                                                                  ThresholdDataType.LEFT ) );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreStatistic value =
                            DoubleScoreStatistic.of( res,
                                                     MetricGroup.CR_POT,
                                                     StatisticMetadata.of( SampleMetadata.of( source, timeWindow, q ),
                                                                           1000,
                                                                           MeasurementUnit.of(),
                                                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                           MetricConstants.CR_POT ) );

                    //Append result
                    statistics.add( value );
                }

            }

        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Do not construct.
     */

    private DataModelTestDataFactory()
    {
    }

}
