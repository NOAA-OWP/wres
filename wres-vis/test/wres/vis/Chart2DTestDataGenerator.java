package wres.vis;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.DoubleMatrix2DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

public abstract class Chart2DTestDataGenerator
{

    /**
     * Data factory.
     */
    
    private static final DataFactory FACTORY = DefaultDataFactory.getInstance();
    
    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from
     * {@link #getScalarMetricOutputMapByLeadThreshold()} and slices.
     *
     * @return an output map of verification scores
     */

    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getMetricOutputMapByLeadThresholdOne()
            throws IOException
    {

        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> full = getScalarMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> combine = new ArrayList<>();
        final double[][] allow =
                new double[][] { { Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY }, { 0.5, 2707.5 },
                                 { 0.95, 13685.0 }, { 0.99, 26648.0 } };
        for ( final double[] next : allow )
        {
            combine.add( full.filterByThreshold( OneOrTwoThresholds.of( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( next[1] ),
                                                                                             FACTORY.ofOneOrTwoDoubles( next[0] ),
                                                                                             Operator.GREATER ) ) ) );
        }
        return FACTORY.combine( combine );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     *
     * @return an output map of verification scores
     */
    public static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getMetricOutputMapByLeadThresholdTwo()
            throws IOException
    {

        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> full = getScalarMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> combine = new ArrayList<>();
        final int[] allow = new int[] { 42, 258, 474, 690 };
        for ( final int next : allow )
        {
            combine.add( full.filterByTime( TimeWindow.of( Instant.MIN,
                                                          Instant.MAX,
                                                          ReferenceTime.VALID_TIME,
                                                          Duration.ofHours( next ) ) ) );
        }
        return FACTORY.combine( combine );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScalarMetricOutputMapByLeadThreshold()
    throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata: add fake sample sizes as these are not available in the test input file
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NPTP1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS",
                                                                                                           "ESP" ) );

        //Iterate through the lead times
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            final MetricResultByThreshold t = ( MetricResultByThreshold ) data.getResult( leadTime );
            final Iterator<MetricResultKey> e = t.getIterator();

            //Iterate through the thresholds
            while ( e.hasNext() )
            {
                //Build the quantile
                final DoubleProcedureParameter f = ( DoubleProcedureParameter ) e.next().getKey();
                final double[] constants = f.getParValReal().getConstants();
                final double[] probConstants = f.getParVal().getConstants();
                final Threshold q = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( constants[0] ),
                                                                 FACTORY.ofOneOrTwoDoubles( probConstants[0] ),
                                                                 Operator.GREATER );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( ( long ) leadTime ) );
                final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( q ) );

                //Build the scalar result
                final MetricResult result = t.getResult( f );
                final double[] res = ( ( DoubleMatrix1DResult ) result ).getResult().toArray();
                final DoubleScoreOutput value = FACTORY.ofDoubleScoreOutput( res[0], meta );

                //Append result
                rawData.put( key, value );
            }
        }

        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScoreMetricOutputMapByLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata: add fake sample sizes as these are not available in the test input file
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NPTP1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS",
                                                                                                           "ESP" ) );

        //Iterate through the lead times
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            final MetricResultByThreshold t = ( MetricResultByThreshold ) data.getResult( leadTime );
            final Iterator<MetricResultKey> e = t.getIterator();
            //Iterate through the thresholds
            while ( e.hasNext() )
            {
                //Build the quantile
                final DoubleProcedureParameter f = ( DoubleProcedureParameter ) e.next().getKey();
                final double[] constants = f.getParValReal().getConstants();
                final double[] probConstants = f.getParVal().getConstants();
                final Threshold q = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( constants[0] ),
                                                                 FACTORY.ofOneOrTwoDoubles( probConstants[0] ),
                                                                 Operator.GREATER );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( ( long ) leadTime ) );
                final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( q ) );

                //Build the scalar result
                final MetricResult result = t.getResult( f );
                final double res = ( ( DoubleMatrix1DResult ) result ).getResult().toArray()[0];
                final DoubleScoreOutput value = FACTORY.ofDoubleScoreOutput( res, meta );

                //Append result
                rawData.put( key, value );
            }
        }

        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiVectorOutput} that contains the components of the
     * reliability diagram (forecast probabilities, observed given forecast probabilities, and sample sizes) for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml.
     *
     * @return an output map of reliability diagrams
     */
    static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> getReliabilityDiagramByLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, MultiVectorOutput> rawData = new TreeMap<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( next[1] ),
                                                      FACTORY.ofOneOrTwoDoubles( next[0] ),
                                                      Operator.GREATER ) );
        }

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata: add fake sample sizes as these are not available in the test input file
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.RELIABILITY_DIAGRAM,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NPTP1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS" ) );

        //Iterate through the lead times.
        int count = -1;
        while ( d.hasNext() )
        {
            //Hank: I'm going to start with the first and include every six: 0, 6, 12, etc.
            count++;
            if ( count % 6 != 0 )
            {
                d.next();
                continue;
            }

            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            final MetricResultByThreshold t = ( MetricResultByThreshold ) data.getResult( leadTime );
            final Iterator<MetricResultKey> e = t.getIterator();
            boolean firstOne = true; //Used to track if this is the first time through the e loop.  See HDH comment below.

            //Iterate through the thresholds
            while ( e.hasNext() )
            {
                //Build the quantile
                final DoubleProcedureParameter f = ( DoubleProcedureParameter ) e.next().getKey();
                final double[] constants = f.getParValReal().getConstants();
                final double[] probConstants = f.getParVal().getConstants();
                final Threshold q = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( constants[0] ),
                                                                 FACTORY.ofOneOrTwoDoubles( probConstants[0] ),
                                                                 Operator.GREATER );
                //Read only selected quantiles
                if ( allowed.contains( q ) )
                {
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       ReferenceTime.VALID_TIME,
                                                       Duration.ofHours( ( long ) leadTime ) );
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( q ) );

                    //Build the result
                    final MetricResult result = t.getResult( f );
                    final double[][] res = ( ( DoubleMatrix2DResult ) result ).getResult().toArray();

                    //Ensure missings are NaN by brute force.
                    for ( int i = 0; i < res.length; i++ )
                    {
                        for ( int j = 0; j < res[i].length; j++ )
                        {
                            if ( res[i][j] == -999D )
                            {
                                res[i][j] = Double.NaN;
                            }

                            //HDH (8/15/17): Forcing a NaN in the first time series within the reliability diagram at index 2.
                            if ( firstOne && ( i == 0 ) && ( j == 2 ) )
                            {
                                res[i][j] = Double.NaN;
                                firstOne = false;
                            }
                        }
                    }

                    final Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
                    output.put( MetricDimension.FORECAST_PROBABILITY, res[0] ); //Forecast probabilities
                    output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, res[1] ); //Observed | forecast probabilities
                    output.put( MetricDimension.SAMPLE_SIZE, res[2] ); //Observed | forecast probabilities
                    final MultiVectorOutput value = FACTORY.ofMultiVectorOutput( output, meta );

                    //Append result
                    rawData.put( key, value );
                }
            }
        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Relative Operating Characteristic (ROC) diagram (probability of detection and probability of false detection) for
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getROCDiagramByLeadThreshold.xml.
     *
     * @return an output map of ROC diagrams
     */

    static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> getROCDiagramByLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, MultiVectorOutput> rawData = new TreeMap<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( next[1] ),
                                                      FACTORY.ofOneOrTwoDoubles( next[0] ),
                                                      Operator.GREATER ) );
        }

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getROCDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NPTP1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS" ) );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            final MetricResultByThreshold t = ( MetricResultByThreshold ) data.getResult( leadTime );
            final Iterator<MetricResultKey> e = t.getIterator();

            //Iterate through the thresholds
            while ( e.hasNext() )
            {
                //Build the quantile
                final DoubleProcedureParameter f = ( DoubleProcedureParameter ) e.next().getKey();
                final double[] constants = f.getParValReal().getConstants();
                final double[] probConstants = f.getParVal().getConstants();
                final Threshold q = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( constants[0] ),
                                                                 FACTORY.ofOneOrTwoDoubles( probConstants[0] ),
                                                                 Operator.GREATER );
                //Read only selected quantiles
                if ( allowed.contains( q ) )
                {
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       ReferenceTime.VALID_TIME,
                                                       Duration.ofHours( ( long ) leadTime ) );
                    final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( q ) );

                    //Build the result
                    final MetricResult result = t.getResult( f );
                    final double[][] roc = ( ( DoubleMatrix2DResult ) result ).getResult().toArray();

                    //Ensure missings are NaN by brute force.
                    for ( int i = 0; i < roc.length; i++ )
                    {
                        for ( int j = 0; j < roc[i].length; j++ )
                        {
                            if ( roc[i][j] == -999D )
                            {
                                roc[i][j] = Double.NaN;
                            }
                        }
                    }

                    final Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
                    output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, roc[0] ); //PoFD
                    output.put( MetricDimension.PROBABILITY_OF_DETECTION, roc[1] ); //PoD
                    final MultiVectorOutput value = FACTORY.ofMultiVectorOutput( output, meta );

                    //Append result
                    rawData.put( key, value );
                }
            }
        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Rank Histogram (rank position, which represents the number of gaps between ensemble members plus one) and
     * the relative frequency of observations that fall within each gap. The results include various thresholds and
     * forecast lead times. Reads the input data from testinput/chart2DTest/getRankHistogramByLeadThreshold.xml.
     *
     * @return an output map of rank histograms
     */

    static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> getRankHistogramByLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, MultiVectorOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getRankHistogramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.RANK_HISTOGRAM,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NPTP1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS" ) );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {

            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            final MetricResultByThreshold t = ( MetricResultByThreshold ) data.getResult( leadTime );
            final Iterator<MetricResultKey> e = t.getIterator();

            //HDH - Limiting the lead times to 42h and every 4 days thereafter (there are way too many lead times!).
            if ( ( leadTime - 42.0 ) % 96 != 0 )
            {
                continue;
            }

            //Iterate through the thresholds
            while ( e.hasNext() )
            {
                //Build the quantile
                final DoubleProcedureParameter f = ( DoubleProcedureParameter ) e.next().getKey();
                final double[] constants = f.getParValReal().getConstants();
                final double[] probConstants = f.getParVal().getConstants();
                final Threshold q = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( constants[0] ),
                                                                 FACTORY.ofOneOrTwoDoubles( probConstants[0] ),
                                                                 Operator.GREATER );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( ( long ) leadTime ) );
                final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( q ) );

                //Build the result
                final MetricResult result = t.getResult( f );
                final double[][] rh = ( ( DoubleMatrix2DResult ) result ).getResult().toArray();

                //Ensure missings are NaN by brute force.
                for ( int i = 0; i < rh.length; i++ )
                {
                    for ( int j = 0; j < rh[i].length; j++ )
                    {
                        if ( rh[i][j] == -999D )
                        {
                            rh[i][j] = Double.NaN;
                        }
                    }
                }

                final Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
                output.put( MetricDimension.RANK_ORDER, rh[0] );
                output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, rh[1] );
                final MultiVectorOutput value = FACTORY.ofMultiVectorOutput( output, meta );

                //Append result
                rawData.put( key, value );
            }
        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Quantile-Quantile Diagram (predicted quantiles and observed quantiles) for various thresholds and forecast lead
     * times. Reads the input data from testinput/chart2DTest/getQQDiagramByLeadThreshold.xml.
     *
     * @return an output map of QQ diagrams
     */

    static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> getQQDiagramByLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, MultiVectorOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getQQDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        TimeWindow windowMeta = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.parse( "2015-12-31T11:59:59Z" ),
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( 24 ),
                                               Duration.ofHours( 120 ) );
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "MILLIMETER" ),
                                                                         MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "WGCM8",
                                                                                                           "PRECIPITATION",
                                                                                                           "HEFS" ),
                                                                         windowMeta );

        //Single threshold
        final Threshold threshold = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 Operator.GREATER );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {

            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.parse( "2015-12-31T11:59:59Z" ),
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( ( long ) leadTime ) );
            final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( threshold ) );
            final DoubleMatrix2DResult t = ( DoubleMatrix2DResult ) data.getResult( leadTime );
            final double[][] qq = t.getResult().toArray();

            final Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
            output.put( MetricDimension.PREDICTED_QUANTILES, qq[0] );
            output.put( MetricDimension.OBSERVED_QUANTILES, qq[1] );
            final MultiVectorOutput value = FACTORY.ofMultiVectorOutput( output, meta );

            //Append result
            rawData.put( key, value );

        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link BoxPlotOutput} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times.
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByObservedAndLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */

    static MetricOutputMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotErrorsByObservedAndLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, BoxPlotOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getBoxPlotErrorsByObservedAndLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension( "INCH" ),
                                                                         metaFactory.getDimension( "INCH" ),
                                                                         MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NBBC1",
                                                                                                           "PRECIPITATION",
                                                                                                           "HEFS" ) );
        //Single threshold
        final Threshold threshold = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 Operator.GREATER );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.MIN,
                                               Instant.MAX,
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( ( long ) leadTime ) );
            final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of(  threshold ) );
            final DoubleMatrix2DResult t = ( DoubleMatrix2DResult ) data.getResult( leadTime );
            final double[][] bp = t.getResult().toArray();
            //Thresholds in the first row
            VectorOfDoubles probabilities = FACTORY.vectorOf( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
            //Boxes in the remaining rows
            final List<PairOfDoubleAndVectorOfDoubles> output = new ArrayList<>();
            for ( double[] next : bp )
            {
                if ( Double.compare( next[0], -999 ) != 0 )
                {
                    output.add( FACTORY.pairOf( next[0], Arrays.copyOfRange( next, 1, next.length ) ) );
                }
            }
            final BoxPlotOutput out = FACTORY.ofBoxPlotOutput( output,
                                                                     probabilities,
                                                                     meta,
                                                                     MetricDimension.OBSERVED_VALUE,
                                                                     MetricDimension.FORECAST_ERROR );

            //Append result
            rawData.put( key, out );
        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link BoxPlotOutput} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times.
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByForecastAndLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */

    static MetricOutputMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotErrorsByForecastAndLeadThreshold()
            throws IOException
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, BoxPlotOutput> rawData = new TreeMap<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getBoxPlotErrorsByForecastAndLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 1000,
                                                                         metaFactory.getDimension( "INCH" ),
                                                                         metaFactory.getDimension( "INCH" ),
                                                                         MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "NBBC1",
                                                                                                           "PRECIPITATION",
                                                                                                           "HEFS" ) );
        //Single threshold
        final Threshold threshold =
                FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                   FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                   Operator.GREATER );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = ( Double ) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.MIN,
                                               Instant.MAX,
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( ( long ) leadTime ) );
            final Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( window, OneOrTwoThresholds.of( threshold ) );
            final DoubleMatrix2DResult t = ( DoubleMatrix2DResult ) data.getResult( leadTime );
            final double[][] bp = t.getResult().toArray();
            //Thresholds in the first row
            VectorOfDoubles probabilities = FACTORY.vectorOf( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
            //Boxes in the remaining rows
            final List<PairOfDoubleAndVectorOfDoubles> output = new ArrayList<>();
            for ( double[] next : bp )
            {
                if ( Double.compare( next[0], -999 ) != 0 )
                {
                    output.add( FACTORY.pairOf( next[0], Arrays.copyOfRange( next, 1, next.length ) ) );
                }
            }
            final BoxPlotOutput out = FACTORY.ofBoxPlotOutput( output,
                                                                     probabilities,
                                                                     meta,
                                                                     MetricDimension.ENSEMBLE_MEAN,
                                                                     MetricDimension.FORECAST_ERROR );

            //Append result
            rawData.put( key, out );
        }

        //Return the results
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the CRPSS for various
     * rolling time windows at one threshold (all data). Corresponds to the use case in Redmine ticket #40785.
     *
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScoreOutputForPoolingWindowsFirst()
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> rawData = new TreeMap<>();

        // Create the metric output metadata: add fake sample sizes as these are not available in the test input file
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 90,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "DOSC1",
                                                                                                           "STREAMFLOW",
                                                                                                           "HEFS",
                                                                                                           "ESP" ) );

        // Rolling window parameters
        Instant start = Instant.parse( "2015-12-01T00:00:00Z" );
        Duration period = Duration.ofDays( 91 );
        Duration frequency = Duration.ofDays( 30 );

        // Threshold
        final Threshold threshold = FACTORY.ofQuantileThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                 Operator.GREATER );

        // Source data for the outputs
        double[] sixHourOutputs = new double[] { 0.42, 0.32, 0.54, 0.56, 0.52, 0.82, 0.85, 0.63, 0.79, 0.86 };
        double[] twelveHourOutputs = new double[] { 0.37, 0.29, 0.49, 0.53, 0.49, 0.61, 0.67, 0.59, 0.48, 0.52 };
        double[] eighteenHourOutputs = new double[] { 0.28, 0.2, 0.29, 0.45, 0.36, 0.56, 0.48, 0.42, 0.295, 0.415 };
        double[] twentyFourHourOutputs = new double[] { 0.14, 0.11, 0.13, 0.16, 0.15, 0.2, 0.23, 0.16, 0.22, 0.35 };

        // Iterate through 10 rotations of the frequency
        for ( int i = 0; i < 10; i++ )
        {
            Instant begin = start.plus( frequency.multipliedBy( i ) );
            Instant end = begin.plus( period );
            //Add the 6h data
            TimeWindow sixHourWindow = TimeWindow.of( begin,
                                                      end,
                                                      ReferenceTime.ISSUE_TIME,
                                                      Duration.ofHours( 6 ) );
            DoubleScoreOutput sixHourOutput =
                    FACTORY.ofDoubleScoreOutput( sixHourOutputs[i],
                                                       metaFactory.getOutputMetadata( meta, sixHourWindow ) );
            rawData.put( Pair.of( sixHourWindow, OneOrTwoThresholds.of( threshold ) ), sixHourOutput );
            //Add the 12h data
            TimeWindow twelveHourWindow = TimeWindow.of( begin,
                                                         end,
                                                         ReferenceTime.ISSUE_TIME,
                                                         Duration.ofHours( 12 ) );
            DoubleScoreOutput twelveHourOutput =
                    FACTORY.ofDoubleScoreOutput( twelveHourOutputs[i],
                                                       metaFactory.getOutputMetadata( meta, twelveHourWindow ) );
            rawData.put( Pair.of( twelveHourWindow, OneOrTwoThresholds.of( threshold ) ), twelveHourOutput );
            //Add the 18h data
            TimeWindow eighteenHourWindow = TimeWindow.of( begin,
                                                           end,
                                                           ReferenceTime.ISSUE_TIME,
                                                           Duration.ofHours( 18 ) );
            DoubleScoreOutput eighteenHourOutput =
                    FACTORY.ofDoubleScoreOutput( eighteenHourOutputs[i],
                                                       metaFactory.getOutputMetadata( meta, eighteenHourWindow ) );
            rawData.put( Pair.of( eighteenHourWindow, OneOrTwoThresholds.of( threshold ) ), eighteenHourOutput );
            //Add the 24h data
            TimeWindow twentyFourHourWindow = TimeWindow.of( begin,
                                                             end,
                                                             ReferenceTime.ISSUE_TIME,
                                                             Duration.ofHours( 24 ) );
            DoubleScoreOutput twentyFourHourOutput =
                    FACTORY.ofDoubleScoreOutput( twentyFourHourOutputs[i],
                                                       metaFactory.getOutputMetadata( meta, twentyFourHourWindow ) );
            rawData.put( Pair.of( twentyFourHourWindow, OneOrTwoThresholds.of( threshold ) ), twentyFourHourOutput );
        }

        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link DoubleScoreOutput} comprising the bias fraction
     * for various pooling windows at one threshold (all data). Corresponds to the use case in Redmine ticket #46461.
     *
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> getScoreOutputForPoolingWindowsSecond()
    {

        final MetadataFactory metaFactory = FACTORY.getMetadataFactory();
        final Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> rawData = new TreeMap<>();

        // Create the metric output metadata: add fake sample sizes as these are not available in the test input file
        final MetricOutputMetadata meta = metaFactory.getOutputMetadata( 18,
                                                                         metaFactory.getDimension(),
                                                                         metaFactory.getDimension( "CMS" ),
                                                                         MetricConstants.BIAS_FRACTION,
                                                                         MetricConstants.MAIN,
                                                                         metaFactory.getDatasetIdentifier( "ABEC2",
                                                                                                           "STREAMFLOW",
                                                                                                           "NWM" ) );
        double[] scores = new double[] {
                                         -0.39228763627058233,
                                         -0.38540392640098137,
                                         -0.37290595138891640,
                                         -0.29294118442636000,
                                         -0.21904815321579500,
                                         -0.15832253472025700,
                                         -0.29244152171401800,
                                         -0.28854939865963400,
                                         -0.32666816357502900,
                                         -0.29652842873636000,
                                         -0.28174289655134900,
                                         -0.26014386674719100,
                                         -0.20220839431888500,
                                         -0.26801048204027200,
                                         -0.28350781433349200,
                                         -0.27907401971041900,
                                         -0.25723312071583900,
                                         -0.28349542374488600,
                                         -0.27544986528110100,
                                         -0.25307837568226800,
                                         -0.24993043930250200,
                                         -0.27070337571167200,
                                         -0.25422214821455900,
                                         -0.28105802405674500
        };
        // Build the map
        Threshold threshold =
                FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ), Operator.GREATER );
        for ( int i = 0; i < scores.length; i++ )
        {
            String nextDate = "2017-08-08T" + String.format( "%02d", i ) + ":00:00Z";
            rawData.put( Pair.of( TimeWindow.of( Instant.parse( nextDate ),
                                                 Instant.parse( nextDate ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 0 ),
                                                 Duration.ofHours( 18 ) ),
                                  OneOrTwoThresholds.of( threshold ) ),
                         FACTORY.ofDoubleScoreOutput( scores[i], meta ) );
        }
        return FACTORY.ofMap( rawData );
    }

    /**
     * Returns a {@link PairedOutput} that comprises a {@link Duration} that represents a time-to-peak error against an
     * {@link Instant} that represents the origin (basis time) of the time-series from which the timing error
     * originates. Contains results for forecasts issued at 12Z each day from 1985-01-01T12:00:00Z to
     * 1985-01-10T12:00:00Z and with a forecast horizon of 336h.
     *
     * @return a paired output of timing errors by basis time
     */

    public static MetricOutputMapByTimeAndThreshold<PairedOutput<Instant,Duration>> getTimeToPeakErrors()
    {
        // Create a list of pairs

        MetadataFactory metaFac = FACTORY.getMetadataFactory();
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        // Add some fake time-to-peak errors to the list
        input.add( Pair.of( Instant.parse( "1985-01-01T12:00:00Z" ), Duration.ofHours( -12 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-02T12:00:00Z" ), Duration.ofHours( -2 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-03T12:00:00Z" ), Duration.ofHours( +2 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-04T12:00:00Z" ), Duration.ofHours( +4 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-05T12:00:00Z" ), Duration.ofHours( +8 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-06T12:00:00Z" ), Duration.ofHours( -12 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-07T12:00:00Z" ), Duration.ofHours( -16 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-08T12:00:00Z" ), Duration.ofHours( -22 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-09T12:00:00Z" ), Duration.ofHours( 0 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-10T12:00:00Z" ), Duration.ofHours( 24 ) ) );
        // Create the metadata
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-10T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 336 ) );
        MetricOutputMetadata meta = metaFac.getOutputMetadata( input.size(),
                                                               metaFac.getDimension( "DURATION" ),
                                                               metaFac.getDimension( "CMS" ),
                                                               MetricConstants.TIME_TO_PEAK_ERROR,
                                                               MetricConstants.MAIN,
                                                               metaFac.getDatasetIdentifier( "DRRC2",
                                                                                             "Streamflow",
                                                                                             "HEFS" ),
                                                               window );
        // Build and return
        Map<Pair<TimeWindow, OneOrTwoThresholds>, PairedOutput<Instant, Duration>> rawData = new TreeMap<>();
        rawData.put( Pair.of( window,
                              OneOrTwoThresholds.of( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                  Operator.GREATER ) ) ),
                     FACTORY.ofPairedOutput( input, meta ) );
        return FACTORY.ofMap( rawData );
    }

    /**
     * <p>Returns a {@link DurationOutput} that summarizes the time-to-peak errors associated with
     * {@link getTimeToPeakErrors()}. The output includes:</p>
     * <ol>
     * <li>{@link MetricConstants#MEAN}</li>
     * <li>{@link MetricConstants#MEDIAN}</li>
     * <li>{@link MetricConstants#STANDARD_DEVIATION}</li>
     * <li>{@link MetricConstants#MINIMUM}</li>
     * <li>{@link MetricConstants#MAXIMUM}</li>
     * <li>{@link MetricConstants#MEAN_ABSOLUTE}</li>
     * </ol>
     *
     * @return a set of summary statistics for time-to-peak errors
     */

    public static MetricOutputMapByTimeAndThreshold<DurationScoreOutput> getTimeToPeakErrorStatistics()
    {
        // Create a list of pairs

        MetadataFactory metaFac = FACTORY.getMetadataFactory();
        Map<MetricConstants, Duration> returnMe = new HashMap<>();
        returnMe.put( MetricConstants.MEAN, Duration.ofMinutes( 156 ) );
        returnMe.put( MetricConstants.MEDIAN, Duration.ofHours( -1 ) );
        returnMe.put( MetricConstants.STANDARD_DEVIATION, Duration.ofMillis( 48364615 ) );
        returnMe.put( MetricConstants.MINIMUM, Duration.ofHours( -22 ) );
        returnMe.put( MetricConstants.MAXIMUM, Duration.ofHours( 24 ) );
        returnMe.put( MetricConstants.MEAN_ABSOLUTE, Duration.ofMinutes( 612 ) );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-10T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 336 ) );
        MetricOutputMetadata meta = metaFac.getOutputMetadata( 10,
                                                               metaFac.getDimension( "DURATION" ),
                                                               metaFac.getDimension( "CMS" ),
                                                               MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                               MetricConstants.MAIN,
                                                               metaFac.getDatasetIdentifier( "DRRC2",
                                                                                             "Streamflow",
                                                                                             "HEFS" ),
                                                               window );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, DurationScoreOutput> rawData = new TreeMap<>();
        rawData.put( Pair.of( window,
                              OneOrTwoThresholds.of( FACTORY.ofThreshold( FACTORY.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                  Operator.GREATER ) ) ),
                     FACTORY.ofDurationScoreOutput( returnMe, meta ) );
        return FACTORY.ofMap( rawData );
    }

}
