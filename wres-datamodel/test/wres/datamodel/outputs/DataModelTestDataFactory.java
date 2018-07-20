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
import wres.datamodel.SafeOneOrTwoDoubles;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.SafeMetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;
import wres.datamodel.outputs.SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
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

        final SafeMetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getScalarMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( 1000,
                                                                                 MetadataFactory.getDimension(),
                                                                                 MetadataFactory.getDimension( "CMS" ),
                                                                                 MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                                 MetricConstants.MAIN,
                                                                                 MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "DRRC2" ),
                                                                                                                       "SQIN",
                                                                                                                       "HEFS",
                                                                                                                       "ESP" ) );

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
                            OneOrTwoThresholds.of( DataFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( constants[0] ),
                                                                                    SafeOneOrTwoDoubles.of( probConstants[0] ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value = DataFactory.ofDoubleScoreOutput( res[0], meta );

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

        SafeMetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder<>();

        //Fake metadata
        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( 1000,
                                                                       MetadataFactory.getDimension(),
                                                                       MetadataFactory.getDimension( "CMS" ),
                                                                       MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                       MetricConstants.MAIN,
                                                                       MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "DRRC2" ),
                                                                                                             "SQIN",
                                                                                                             "HEFS",
                                                                                                             "ESP" ) );

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
                    OneOrTwoThresholds.of( DataFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.0 ),
                                                                            SafeOneOrTwoDoubles.of( 0.1 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           DataFactory.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );

            DoubleScoreOutput firstValue = DataFactory.ofDoubleScoreOutput( 66.0, meta );

            builder.put( Pair.of( timeWindow, first ), firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( DataFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 2.0 ),
                                                                            SafeOneOrTwoDoubles.of( 0.2 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           DataFactory.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );

            DoubleScoreOutput secondValue = DataFactory.ofDoubleScoreOutput( 67.0, meta );

            builder.put( Pair.of( timeWindow, second ), secondValue );


            // Add third result
            OneOrTwoThresholds third =
                    OneOrTwoThresholds.of( DataFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 3.0 ),
                                                                            SafeOneOrTwoDoubles.of( 0.3 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           DataFactory.ofThreshold( SafeOneOrTwoDoubles.of( 6.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );


            DoubleScoreOutput thirdValue = DataFactory.ofDoubleScoreOutput( 68.0, meta );

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

        final SafeMetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> builder =
                new SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getVectorMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( 1000,
                                                                                 MetadataFactory.getDimension(),
                                                                                 MetadataFactory.getDimension( "CFS" ),
                                                                                 MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                                 MetricConstants.CR_POT,
                                                                                 MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "NPTP1" ),
                                                                                                                       "SQIN",
                                                                                                                       "HEFS",
                                                                                                                       "ESP" ) );

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
                            OneOrTwoThresholds.of( DataFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( constants[0] ),
                                                                                    SafeOneOrTwoDoubles.of( probConstants[0] ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value =
                            DataFactory.ofDoubleScoreOutput( res, ScoreOutputGroup.CR_POT, meta );

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
