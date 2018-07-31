package wres.datamodel.outputs;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Dimension;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DataModelTestDataFactory
{

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScoreOutput} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getScalarMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScalarMetricOutputMapByLeadThresholdOne()
    {

        final MetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getScalarMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = MetricOutputMetadata.of( 1000,
                                                                                 Dimension.of(),
                                                                                 Dimension.of( "CMS" ),
                                                                                 MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                                 MetricConstants.MAIN,
                                                                                 DatasetIdentifier.of( Location.of( "DRRC2" ), "SQIN", "HEFS", "ESP" ) );

            //Iterate through the lead times
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = ( (Double) d.next().getKey() );
                final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             ReferenceTime.VALID_TIME,
                                                             Duration.ofHours( (int) leadTime ) );
                final MetricResultByThreshold t =
                        (MetricResultByThreshold) data.getResult( timeWindow.getLatestLeadTimeInHours() );
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
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value = DoubleScoreOutput.of( res[0], meta );

                    //Append result
                    builder.put( key, value );
                }

            }

        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScoreOutput} comprising the MAE for selected
     * thresholds and forecast lead times using fake data.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScalarMetricOutputMapByLeadThresholdTwo()
    {

        MetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();

        //Fake metadata
        MetricOutputMetadata meta = MetricOutputMetadata.of( 1000,
                                                                       Dimension.of(),
                                                                       Dimension.of( "CMS" ),
                                                                       MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                       MetricConstants.MAIN,
                                                                       DatasetIdentifier.of( Location.of( "DRRC2" ), "SQIN", "HEFS", "ESP" ) );

        int[] leadTimes = new int[] { 1, 2, 3, 4, 5 };

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
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

            DoubleScoreOutput firstValue = DoubleScoreOutput.of( 66.0, meta );

            builder.put( Pair.of( timeWindow, first ), firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 2.0 ),
                                                                            OneOrTwoDoubles.of( 0.2 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );

            DoubleScoreOutput secondValue = DoubleScoreOutput.of( 67.0, meta );

            builder.put( Pair.of( timeWindow, second ), secondValue );


            // Add third result
            OneOrTwoThresholds third =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 3.0 ),
                                                                            OneOrTwoDoubles.of( 0.3 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 6.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );


            DoubleScoreOutput thirdValue = DoubleScoreOutput.of( 68.0, meta );

            builder.put( Pair.of( timeWindow, third ), thirdValue );

        }

        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getVectorMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getVectorMetricOutputMapByLeadThresholdOne()
    {

        final MetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getVectorMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = MetricOutputMetadata.of( 1000,
                                                                                 Dimension.of(),
                                                                                 Dimension.of( "CFS" ),
                                                                                 MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                                 MetricConstants.CR_POT,
                                                                                 DatasetIdentifier.of( Location.of( "NPTP1" ), "SQIN", "HEFS", "ESP" ) );

            //Iterate through the lead times
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = (Double) d.next().getKey();
                final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             ReferenceTime.VALID_TIME,
                                                             Duration.ofHours( (int) leadTime ) );
                final MetricResultByThreshold t =
                        (MetricResultByThreshold) data.getResult( timeWindow.getLatestLeadTimeInHours() );
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
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value =
                            DoubleScoreOutput.of( res, ScoreOutputGroup.CR_POT, meta );

                    //Append result
                    builder.put( key, value );
                }

            }

        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        return builder.build();
    }

}
