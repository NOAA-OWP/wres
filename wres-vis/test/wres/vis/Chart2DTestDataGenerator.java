package wres.vis;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

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
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;

public abstract class Chart2DTestDataGenerator
{

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScalarOutput} comprising the CRPSS for a subset of
     * thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     * 
     * @return an output map of verification scores
     */
    
    public static MetricOutputMapByTimeAndThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> full = getScalarMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByTimeAndThreshold<ScalarOutput>> combine = new ArrayList<>();
        final double[][] allow =
                new double[][] { { Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY }, { 0.5, 2707.5 },
                                 { 0.95, 13685.0 }, { 0.99, 26648.0 } };
        for ( final double[] next : allow )
        {
            combine.add( full.filterByThreshold( outputFactory.getQuantileThreshold( next[1],
                                                                                    next[0],
                                                                                    Operator.GREATER ) ) );
        }
        return outputFactory.combine( combine );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScalarOutput} comprising the CRPSS for a subset of
     * thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     * 
     * @return an output map of verification scores
     */
    public static MetricOutputMapByTimeAndThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdTwo()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> full = getScalarMetricOutputMapByLeadThreshold();
        final List<MetricOutputMapByTimeAndThreshold<ScalarOutput>> combine = new ArrayList<>();
        final int[] allow = new int[] { 42, 258, 474, 690 };
        for ( final int next : allow )
        {
            combine.add( full.filterByTime( TimeWindow.of( Instant.MIN,
                                                          Instant.MAX,
                                                          ReferenceTime.VALID_TIME,
                                                          Duration.ofHours( next ) ) ) );
        }
        return outputFactory.combine( combine );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScalarOutput} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<ScalarOutput> getScalarMetricOutputMapByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, ScalarOutput> rawData = new TreeMap<>();
    
        try
        {
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
                final double leadTime = (Double) d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold) data.getResult( leadTime );
                final Iterator<MetricResultKey> e = t.getIterator();
                
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Threshold q = outputFactory.getQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       ReferenceTime.VALID_TIME,
                                                       Duration.ofHours( (long) leadTime ) );
                    final Pair<TimeWindow, Threshold> key = Pair.of( window, q );
    
                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final ScalarOutput value = outputFactory.ofScalarOutput( res[0], meta );
    
                    //Append result
                    rawData.put( key, value );
                }
            }
    
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        return outputFactory.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiValuedScoreOutput} comprising the CRPSS for 
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<MultiValuedScoreOutput> getVectorMetricOutputMapByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, MultiValuedScoreOutput> rawData = new TreeMap<>();
    
        try
        {
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
                final double leadTime = (Double) d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold) data.getResult( leadTime );
                final Iterator<MetricResultKey> e = t.getIterator();
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Threshold q = outputFactory.getQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       ReferenceTime.VALID_TIME,
                                                       Duration.ofHours( (long) leadTime ) );
                    final Pair<TimeWindow, Threshold> key = Pair.of( window, q );
    
                    //Build the scalar result
                    final MetricResult result = t.getResult( f );
                    final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                    final MultiValuedScoreOutput value = outputFactory.ofMultiValuedScoreOutput( res, meta );
    
                    //Append result
                    rawData.put( key, value );
                }
            }
    
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        return outputFactory.ofMap( rawData );
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
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( outputFactory.getQuantileThreshold( next[1], next[0], Operator.GREATER ) );
        }
        try
        {
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
                final double leadTime = (Double) d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold) data.getResult( leadTime );
                final Iterator<MetricResultKey> e = t.getIterator();
                boolean firstOne = true; //Used to track if this is the first time through the e loop.  See HDH comment below.
    
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Threshold q = outputFactory.getQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    //Read only selected quantiles
                    if ( allowed.contains( q ) )
                    {
                        TimeWindow window = TimeWindow.of( Instant.MIN,
                                                           Instant.MAX,
                                                           ReferenceTime.VALID_TIME,
                                                           Duration.ofHours( (long) leadTime ) );
                        final Pair<TimeWindow, Threshold> key = Pair.of( window, q );
    
                        //Build the result
                        final MetricResult result = t.getResult( f );
                        final double[][] res = ( (DoubleMatrix2DResult) result ).getResult().toArray();
    
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
                        final MultiVectorOutput value = outputFactory.ofMultiVectorOutput( output, meta );
    
                        //Append result
                        rawData.put( key, value );
                    }
                }
            }
    
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
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
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( outputFactory.getQuantileThreshold( next[1], next[0], Operator.GREATER ) );
        }
        try
        {
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
                final double leadTime = (Double) d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold) data.getResult( leadTime );
                final Iterator<MetricResultKey> e = t.getIterator();
    
                //Iterate through the thresholds
                while ( e.hasNext() )
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Threshold q = outputFactory.getQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    //Read only selected quantiles
                    if ( allowed.contains( q ) )
                    {
                        TimeWindow window = TimeWindow.of( Instant.MIN,
                                                           Instant.MAX,
                                                           ReferenceTime.VALID_TIME,
                                                           Duration.ofHours( (long) leadTime ) );
                        final Pair<TimeWindow, Threshold> key = Pair.of( window, q );
    
                        //Build the result
                        final MetricResult result = t.getResult( f );
                        final double[][] roc = ( (DoubleMatrix2DResult) result ).getResult().toArray();
    
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
                        final MultiVectorOutput value = outputFactory.ofMultiVectorOutput( output, meta );
    
                        //Append result
                        rawData.put( key, value );
                    }
                }
            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
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
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        try
        {
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
                final double leadTime = (Double) d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold) data.getResult( leadTime );
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
                    final DoubleProcedureParameter f = (DoubleProcedureParameter) e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Threshold q = outputFactory.getQuantileThreshold( constants[0],
                                                                            probConstants[0],
                                                                            Operator.GREATER );
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       ReferenceTime.VALID_TIME,
                                                       Duration.ofHours( (long) leadTime ) );
                    final Pair<TimeWindow, Threshold> key = Pair.of( window, q );
    
                    //Build the result
                    final MetricResult result = t.getResult( f );
                    final double[][] rh = ( (DoubleMatrix2DResult) result ).getResult().toArray();
    
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
                    final MultiVectorOutput value = outputFactory.ofMultiVectorOutput( output, meta );
    
                    //Append result
                    rawData.put( key, value );
                }
            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link MultiVectorOutput} that contains the components of the
     * Quantile-Quantile Diagram (predicted quantiles and observed quantiles) for various thresholds and forecast lead
     * times. Reads the input data from testinput/chart2DTest/getQQDiagramByLeadThreshold.xml.
     * 
     * @return an output map of QQ diagrams
     */
    
    static MetricOutputMapByTimeAndThreshold<MultiVectorOutput> getQQDiagramByLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, MultiVectorOutput> rawData = new TreeMap<>();
        try
        {
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
            final Threshold threshold = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                            Double.NEGATIVE_INFINITY,
                                                                            Operator.GREATER );

            //Iterate through the lead times.
            while ( d.hasNext() )
            {

                //Set the lead time
                final double leadTime = (Double) d.next().getKey();
                TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2015-12-31T11:59:59Z" ),
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( (long) leadTime ) );
                final Pair<TimeWindow, Threshold> key = Pair.of( window, threshold );
                final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
                final double[][] qq = t.getResult().toArray();

                final Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
                output.put( MetricDimension.PREDICTED_QUANTILES, qq[0] );
                output.put( MetricDimension.OBSERVED_QUANTILES, qq[1] );
                final MultiVectorOutput value = outputFactory.ofMultiVectorOutput( output, meta );

                //Append result
                rawData.put( key, value );

            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link BoxPlotOutput} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times. 
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByObservedAndLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */
    
    static MetricOutputMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotErrorsByObservedAndLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, BoxPlotOutput> rawData = new TreeMap<>();
        try
        {
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
            final Threshold threshold = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                            Double.NEGATIVE_INFINITY,
                                                                            Operator.GREATER );
    
            //Iterate through the lead times.
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = (Double) d.next().getKey();
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( (long) leadTime ) );
                final Pair<TimeWindow, Threshold> key = Pair.of( window, threshold );
                final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
                final double[][] bp = t.getResult().toArray();
                //Thresholds in the first row
                VectorOfDoubles probabilities = outputFactory.vectorOf( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
                //Boxes in the remaining rows
                final List<PairOfDoubleAndVectorOfDoubles> output = new ArrayList<>();
                for ( double[] next : bp )
                {
                    if ( Double.compare( next[0], -999 ) != 0 )
                    {
                        output.add( outputFactory.pairOf( next[0], Arrays.copyOfRange( next, 1, next.length ) ) );
                    }
                }
                final BoxPlotOutput out = outputFactory.ofBoxPlotOutput( output,
                                                                         probabilities,
                                                                         meta,
                                                                         MetricDimension.OBSERVED_VALUE,
                                                                         MetricDimension.FORECAST_ERROR );
    
                //Append result
                rawData.put( key, out );
            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link BoxPlotOutput} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times. 
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByForecastAndLeadThreshold.xml.
     * 
     * @return an output map of verification scores
     */
    
    static MetricOutputMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotErrorsByForecastAndLeadThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, BoxPlotOutput> rawData = new TreeMap<>();
        try
        {
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
            final Threshold threshold = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                            Double.NEGATIVE_INFINITY,
                                                                            Operator.GREATER );
    
            //Iterate through the lead times.
            while ( d.hasNext() )
            {
                //Set the lead time
                final double leadTime = (Double) d.next().getKey();
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( (long) leadTime ) );
                final Pair<TimeWindow, Threshold> key = Pair.of( window, threshold );
                final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
                final double[][] bp = t.getResult().toArray();
                //Thresholds in the first row
                VectorOfDoubles probabilities = outputFactory.vectorOf( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
                //Boxes in the remaining rows
                final List<PairOfDoubleAndVectorOfDoubles> output = new ArrayList<>();
                for ( double[] next : bp )
                {
                    if ( Double.compare( next[0], -999 ) != 0 )
                    {
                        output.add( outputFactory.pairOf( next[0], Arrays.copyOfRange( next, 1, next.length ) ) );
                    }
                }
                final BoxPlotOutput out = outputFactory.ofBoxPlotOutput( output,
                                                                         probabilities,
                                                                         meta,
                                                                         MetricDimension.ENSEMBLE_MEAN,
                                                                         MetricDimension.FORECAST_ERROR );
    
                //Append result
                rawData.put( key, out );
            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        //Return the results
        return outputFactory.ofMap( rawData );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} of {@link ScalarOutput} comprising the CRPSS for various
     * rolling time windows at one threshold (all data). Corresponds to the use case in Redmine ticket #40785.
     * 
     * @return an output map of verification scores
     */
    static MetricOutputMapByTimeAndThreshold<ScalarOutput> getScalarMetricOutputMapForRollingWindows()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Map<Pair<TimeWindow, Threshold>, ScalarOutput> rawData = new TreeMap<>();
    
        try
        {
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
            final Threshold threshold = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                            Double.NEGATIVE_INFINITY,
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
                ScalarOutput sixHourOutput =
                        outputFactory.ofScalarOutput( sixHourOutputs[i],
                                                      metaFactory.getOutputMetadata( meta, sixHourWindow ) );
                rawData.put( Pair.of( sixHourWindow, threshold ), sixHourOutput );
                //Add the 12h data
                TimeWindow twelveHourWindow = TimeWindow.of( begin,
                                                             end,
                                                             ReferenceTime.ISSUE_TIME,
                                                             Duration.ofHours( 12 ) );
                ScalarOutput twelveHourOutput =
                        outputFactory.ofScalarOutput( twelveHourOutputs[i],
                                                      metaFactory.getOutputMetadata( meta, twelveHourWindow ) );
                rawData.put( Pair.of( twelveHourWindow, threshold ), twelveHourOutput );
                //Add the 18h data
                TimeWindow eighteenHourWindow = TimeWindow.of( begin,
                                                               end,
                                                               ReferenceTime.ISSUE_TIME,
                                                               Duration.ofHours( 18 ) );
                ScalarOutput eighteenHourOutput =
                        outputFactory.ofScalarOutput( eighteenHourOutputs[i],
                                                      metaFactory.getOutputMetadata( meta, eighteenHourWindow ) );
                rawData.put( Pair.of( eighteenHourWindow, threshold ), eighteenHourOutput );
                //Add the 24h data
                TimeWindow twentyFourHourWindow = TimeWindow.of( begin,
                                                                 end,
                                                                 ReferenceTime.ISSUE_TIME,
                                                                 Duration.ofHours( 24 ) );
                ScalarOutput twentyFourHourOutput =
                        outputFactory.ofScalarOutput( twentyFourHourOutputs[i],
                                                      metaFactory.getOutputMetadata( meta, twentyFourHourWindow ) );
                rawData.put( Pair.of( twentyFourHourWindow, threshold ), twentyFourHourOutput );
    
            }
        }
        catch ( final Exception e )
        {
            Assert.fail( "Test failed : " + e.getMessage() );
        }
        return outputFactory.ofMap( rawData );
    }
}
