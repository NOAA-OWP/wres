package wres.datamodel;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.SafeMetricOutputForProjectByTimeAndThreshold.SafeMetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.SafeMetricOutputMapByTimeAndThreshold.Builder;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
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
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Builder<DoubleScoreOutput> builder = new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getScalarMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                             metaFactory.getDimension(),
                                                                             metaFactory.getDimension( "CMS" ),
                                                                             MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                             MetricConstants.MAIN,
                                                                             metaFactory.getDatasetIdentifier( "DRRC2",
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
                    final Threshold q = outputFactory.ofQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    final Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value = outputFactory.ofDoubleScoreOutput( res[0], meta );

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
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getVectorMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getVectorMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Builder<DoubleScoreOutput> builder = new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
        try
        {
            //Create the input file
            final File resultFile =
                    new File( "testinput/wres/datamodel/getVectorMetricOutputMapByLeadThresholdOne.xml" );
            final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                             metaFactory.getDimension(),
                                                                             metaFactory.getDimension( "CFS" ),
                                                                             MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                             MetricConstants.CR_POT,
                                                                             metaFactory.getDatasetIdentifier( "NPTP1",
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
                    final Threshold q = outputFactory.ofQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    final Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, q );

                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final DoubleScoreOutput value =
                            outputFactory.ofDoubleScoreOutput( res, ScoreOutputGroup.CR_POT, meta );

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
     * Returns a {@link MetricOutputForProjectByTimeAndThreshold} with fake data.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThreshold} with fake data
     */

    public static MetricOutputForProjectByTimeAndThreshold getMetricOutputForProjectByLeadThreshold()
    {
        //Prep
        SafeMetricOutputForProjectByTimeAndThresholdBuilder builder =
                new SafeMetricOutputForProjectByTimeAndThresholdBuilder();
        DataFactory factory = DefaultDataFactory.getInstance();
        MetadataFactory metaFactory = factory.getMetadataFactory();
        final MetricOutputMetadata fakeMeta =
                factory.getMetadataFactory()
                       .getOutputMetadata( 1000,
                                           metaFactory.getDimension(),
                                           metaFactory.getDimension( "CMS" ),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        List<DoubleScoreOutput> fakeData = new ArrayList<>();

        //Add some fake numbers
        fakeData.add( factory.ofDoubleScoreOutput( 10.0, fakeMeta ) );
        fakeData.add( factory.ofDoubleScoreOutput( 6.0, fakeMeta ) );
        fakeData.add( factory.ofDoubleScoreOutput( 7.0, fakeMeta ) );
        fakeData.add( factory.ofDoubleScoreOutput( 16.0, fakeMeta ) );

        //Build the input map
        MetricOutputMapByMetric<DoubleScoreOutput> in = factory.ofMap( fakeData );

        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 1 ) );
        //Fake lead time and threshold
        builder.addDoubleScoreOutput( factory.ofMapKeyByTimeThreshold( timeWindow, 23.0, Operator.GREATER ),
                                CompletableFuture.completedFuture( in ) );

        //Return data
        return builder.build();
    }

}
