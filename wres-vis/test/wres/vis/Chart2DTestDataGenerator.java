package wres.vis;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.DoubleMatrix2DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

public abstract class Chart2DTestDataGenerator
{

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from
     * {@link #getScalarMetricOutputMapByLeadThreshold()} and slices.
     *
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatistic> getMetricOutputMapByLeadThresholdOne()
            throws IOException
    {
        List<DoubleScoreStatistic> full = getScalarMetricOutputMapByLeadThreshold();
        List<DoubleScoreStatistic> statistics = new ArrayList<>();

        double[][] allow =
                new double[][] { { Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY }, { 0.5, 2707.5 },
                                 { 0.95, 13685.0 }, { 0.99, 26648.0 } };
        for ( final double[] next : allow )
        {
            OneOrTwoThresholds filter =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( next[1] ),
                                                                          OneOrTwoDoubles.of( next[0] ),
                                                                          Operator.GREATER,
                                                                          ThresholdDataType.LEFT ) );
            Slicer.filter( full, data -> data.getSampleMetadata().getThresholds().equals( filter ) ).forEach( statistics::add );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     *
     * @return an output map of verification scores
     */
    public static List<DoubleScoreStatistic> getMetricOutputMapByLeadThresholdTwo()
            throws IOException
    {
        List<DoubleScoreStatistic> full = getScalarMetricOutputMapByLeadThreshold();
        List<DoubleScoreStatistic> statistics = new ArrayList<>();

        final int[] allow = new int[] { 42, 258, 474, 690 };
        for ( final int next : allow )
        {
            TimeWindow filter = TimeWindow.of( Instant.MIN,
                                               Instant.MAX,
                                               Duration.ofHours( next ) );
            Slicer.filter( full, data -> data.getSampleMetadata().getTimeWindow().equals( filter ) ).forEach( statistics::add );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatistic> getScalarMetricOutputMapByLeadThreshold()
            throws IOException
    {
        final List<DoubleScoreStatistic> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NPTP1" ),
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
                final OneOrTwoThresholds q =
                        OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                              OneOrTwoDoubles.of( probConstants[0] ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   Duration.ofHours( (long) leadTime ) );

                //Build the scalar result
                final MetricResult result = t.getResult( f );
                final double[] res = ( (DoubleMatrix1DResult) result ).getResult().toArray();
                final DoubleScoreStatistic value =
                        DoubleScoreStatistic.of( res[0],
                                                 StatisticMetadata.of( SampleMetadata.of( source, window, q ),
                                                                       1000,
                                                                       MeasurementUnit.of(),
                                                                       MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                       MetricConstants.MAIN ) );

                //Append result
                rawData.add( value );
            }
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatistic> getScoreMetricOutputMapByLeadThreshold()
            throws IOException
    {
        final List<DoubleScoreStatistic> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getMetricOutputMapByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NPTP1" ),
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
                final OneOrTwoThresholds q =
                        OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                              OneOrTwoDoubles.of( probConstants[0] ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   Duration.ofHours( (long) leadTime ) );

                //Build the scalar result
                final MetricResult result = t.getResult( f );
                final double res = ( (DoubleMatrix1DResult) result ).getResult().toArray()[0];
                final DoubleScoreStatistic value =
                        DoubleScoreStatistic.of( res,
                                                 StatisticMetadata.of( SampleMetadata.of( source, window, q ),
                                                                       1000,
                                                                       MeasurementUnit.of(),
                                                                       MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                       MetricConstants.MAIN ) );

                //Append result
                rawData.add( value );
            }
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatistic} that contains the components of the
     * reliability diagram (forecast probabilities, observed given forecast probabilities, and sample sizes) for various
     * thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml.
     *
     * @return an output map of reliability diagrams
     */
    static List<DiagramStatistic> getReliabilityDiagramByLeadThreshold()
            throws IOException
    {
        final List<DiagramStatistic> rawData = new ArrayList<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( next[1] ),
                                                        OneOrTwoDoubles.of( next[0] ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        }

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getReliabilityDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NPTP1" ),
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
                final Threshold q = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                   OneOrTwoDoubles.of( probConstants[0] ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );
                //Read only selected quantiles
                if ( allowed.contains( q ) )
                {
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       Duration.ofHours( (long) leadTime ) );

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

                    final Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
                    output.put( MetricDimension.FORECAST_PROBABILITY, VectorOfDoubles.of( res[0] ) ); //Forecast probabilities
                    output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, VectorOfDoubles.of( res[1] ) ); //Observed | forecast probabilities
                    output.put( MetricDimension.SAMPLE_SIZE, VectorOfDoubles.of( res[2] ) ); //Observed | forecast probabilities
                    final DiagramStatistic value =
                            DiagramStatistic.of( output,
                                                 StatisticMetadata.of( SampleMetadata.of( source,
                                                                                          window,
                                                                                          OneOrTwoThresholds.of( q ) ),
                                                                       1000,
                                                                       MeasurementUnit.of(),
                                                                       MetricConstants.RELIABILITY_DIAGRAM,
                                                                       MetricConstants.MAIN ) );

                    //Append result
                    rawData.add( value );
                }
            }
        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatistic} that contains the components of the
     * Relative Operating Characteristic (ROC) diagram (probability of detection and probability of false detection) for
     * various thresholds and forecast lead times. Reads the input data from
     * testinput/chart2DTest/getROCDiagramByLeadThreshold.xml.
     *
     * @return an output map of ROC diagrams
     */

    static List<DiagramStatistic> getROCDiagramByLeadThreshold()
            throws IOException
    {
        final List<DiagramStatistic> rawData = new ArrayList<>();
        //Read only selected quantiles
        final List<Threshold> allowed = new ArrayList<>();
        final double[][] allow =
                new double[][] { { 0.1, 858.04 }, { 0.5, 2707.5 }, { 0.9, 9647.0 }, { 0.95, 13685.0 } };
        for ( final double[] next : allow )
        {
            allowed.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( next[1] ),
                                                        OneOrTwoDoubles.of( next[0] ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        }

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getROCDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NPTP1" ),
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
                final Threshold q = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                   OneOrTwoDoubles.of( probConstants[0] ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );
                //Read only selected quantiles
                if ( allowed.contains( q ) )
                {
                    TimeWindow window = TimeWindow.of( Instant.MIN,
                                                       Instant.MAX,
                                                       Duration.ofHours( (long) leadTime ) );

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

                    final Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
                    output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( roc[0] ) ); //PoFD
                    output.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( roc[1] ) ); //PoD
                    final DiagramStatistic value =
                            DiagramStatistic.of( output,
                                                 StatisticMetadata.of( SampleMetadata.of( source,
                                                                                          window,
                                                                                          OneOrTwoThresholds.of( q ) ),
                                                                       1000,
                                                                       MeasurementUnit.of(),
                                                                       MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                                       MetricConstants.MAIN ) );

                    //Append result
                    rawData.add( value );
                }
            }
        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatistic} that contains the components of the
     * Rank Histogram (rank position, which represents the number of gaps between ensemble members plus one) and
     * the relative frequency of observations that fall within each gap. The results include various thresholds and
     * forecast lead times. Reads the input data from testinput/chart2DTest/getRankHistogramByLeadThreshold.xml.
     *
     * @return an output map of rank histograms
     */

    static List<DiagramStatistic> getRankHistogramByLeadThreshold()
            throws IOException
    {
        final List<DiagramStatistic> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getRankHistogramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NPTP1" ),
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
                final Threshold q = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( constants[0] ),
                                                                   OneOrTwoDoubles.of( probConstants[0] ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );
                TimeWindow window = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   Duration.ofHours( (long) leadTime ) );

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

                final Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
                output.put( MetricDimension.RANK_ORDER, VectorOfDoubles.of( rh[0] ) );
                output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, VectorOfDoubles.of( rh[1] ) );
                final DiagramStatistic value =
                        DiagramStatistic.of( output,
                                             StatisticMetadata.of( SampleMetadata.of( source,
                                                                                      window,
                                                                                      OneOrTwoThresholds.of( q ) ),
                                                                   1000,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.RANK_HISTOGRAM,
                                                                   MetricConstants.MAIN ) );

                //Append result
                rawData.add( value );
            }
        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatistic} that contains the components of the
     * Quantile-Quantile Diagram (predicted quantiles and observed quantiles) for various thresholds and forecast lead
     * times. Reads the input data from testinput/chart2DTest/getQQDiagramByLeadThreshold.xml.
     *
     * @return an output map of QQ diagrams
     */

    static List<DiagramStatistic> getQQDiagramByLeadThreshold()
            throws IOException
    {
        final List<DiagramStatistic> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getQQDiagramByLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Metric output metadata
        TimeWindow windowMeta = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.parse( "2015-12-31T11:59:59Z" ),
                                               Duration.ofHours( 24 ),
                                               Duration.ofHours( 120 ) );
        final TimeWindow timeWindow = windowMeta;
        
        //Source metadata
        final SampleMetadata source =
                new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "MILLIMETER" ) )
                                           .setIdentifier( DatasetIdentifier.of( FeatureKey.of( "WGCM8" ),
                                                                                 "PRECIPITATION",
                                                                                 "HEFS" ) )
                                           .setTimeWindow( timeWindow )
                                           .build();

        //Single threshold
        final OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ) );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {

            //Set the lead time
            final double leadTime = (Double) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.parse( "2015-12-31T11:59:59Z" ),
                                               Duration.ofHours( (long) leadTime ) );

            final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
            final double[][] qq = t.getResult().toArray();

            final Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
            output.put( MetricDimension.PREDICTED_QUANTILES, VectorOfDoubles.of( qq[0] ) );
            output.put( MetricDimension.OBSERVED_QUANTILES, VectorOfDoubles.of( qq[1] ) );
            final DiagramStatistic value =
                    DiagramStatistic.of( output,
                                         StatisticMetadata.of( SampleMetadata.of( source,
                                                                                  window,
                                                                                  threshold ),
                                                               1000,
                                                               MeasurementUnit.of( "MILLIMETER" ),
                                                               MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                               MetricConstants.MAIN ) );

            //Append result
            rawData.add( value );

        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link BoxPlotStatistics} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times.
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByObservedAndLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */

    static List<BoxPlotStatistics> getBoxPlotErrorsByObservedAndLeadThreshold()
            throws IOException
    {
        final List<BoxPlotStatistics> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getBoxPlotErrorsByObservedAndLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "INCH" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NBBC1" ),
                                                                               "PRECIPITATION",
                                                                               "HEFS" ) );

        //Single threshold
        final OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ) );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = (Double) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.MIN,
                                               Instant.MAX,
                                               Duration.ofHours( (long) leadTime ) );

            final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
            final double[][] bp = t.getResult().toArray();
            //Thresholds in the first row
            VectorOfDoubles probabilities = VectorOfDoubles.of( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
            //Boxes in the remaining rows
            final List<BoxPlotStatistic> output = new ArrayList<>();

            StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of( source,
                                                                              window,
                                                                              threshold ),
                                                           1000,
                                                           MeasurementUnit.of( "INCH" ),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                           MetricConstants.MAIN );

            for ( double[] next : bp )
            {
                if ( Double.compare( next[0], -999 ) != 0 )
                {
                    output.add( BoxPlotStatistic.of( probabilities,
                                                     VectorOfDoubles.of( Arrays.copyOfRange( next, 1, next.length ) ),
                                                     meta,
                                                     next[0],
                                                     MetricDimension.OBSERVED_VALUE ) );
                }
            }
            
            final BoxPlotStatistics out = BoxPlotStatistics.of( output,
                                                                meta );

            //Append result
            rawData.add( out );
        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link BoxPlotStatistics} that contains a box plot of forecast
     * errors against observed value for a single threshold (all data) and for several forecast lead times.
     * Reads the input data from testinput/chart2DTest/getBoxPlotErrorsByForecastAndLeadThreshold.xml.
     *
     * @return an output map of verification scores
     */

    static List<BoxPlotStatistics> getBoxPlotErrorsByForecastAndLeadThreshold()
            throws IOException
    {
        final List<BoxPlotStatistics> rawData = new ArrayList<>();

        //Create the input file
        final File resultFile = new File( "testinput/chart2DTest/getBoxPlotErrorsByForecastAndLeadThreshold.xml" );
        final MetricResultByLeadTime data = ProductFileIO.read( resultFile );

        final Iterator<MetricResultKey> d = data.getIterator();

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "INCH" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "NBBC1" ),
                                                                               "PRECIPITATION",
                                                                               "HEFS" ) );
        //Single threshold
        final OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ) );

        //Iterate through the lead times.
        while ( d.hasNext() )
        {
            //Set the lead time
            final double leadTime = (Double) d.next().getKey();
            TimeWindow window = TimeWindow.of( Instant.MIN,
                                               Instant.MAX,
                                               Duration.ofHours( (long) leadTime ) );

            final DoubleMatrix2DResult t = (DoubleMatrix2DResult) data.getResult( leadTime );
            final double[][] bp = t.getResult().toArray();
            //Thresholds in the first row
            VectorOfDoubles probabilities = VectorOfDoubles.of( Arrays.copyOfRange( bp[0], 1, bp[0].length ) );
            //Boxes in the remaining rows
            final List<BoxPlotStatistic> output = new ArrayList<>();
            
            StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of( source,
                                                                              window,
                                                                              threshold ),
                                                           1000,
                                                           MeasurementUnit.of( "INCH" ),
                                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                           MetricConstants.MAIN );

            for ( double[] next : bp )
            {
                if ( Double.compare( next[0], -999 ) != 0 )
                {
                    output.add( BoxPlotStatistic.of( probabilities,
                                                     VectorOfDoubles.of( Arrays.copyOfRange( next, 1, next.length ) ),
                                                     meta,
                                                     next[0],
                                                     MetricDimension.ENSEMBLE_MEAN ) );
                }
            }
            final BoxPlotStatistics out = BoxPlotStatistics.of( output,
                                                                meta );

            //Append result
            rawData.add( out );
        }

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the CRPSS for various
     * rolling time windows at one threshold (all data). Corresponds to the use case in Redmine ticket #40785.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatistic> getScoreOutputForPoolingWindowsFirst()
    {
        final List<DoubleScoreStatistic> rawData = new ArrayList<>();

        // Threshold
        final OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ) );

        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "DOSC1" ),
                                                                               "STREAMFLOW",
                                                                               "HEFS",
                                                                               "ESP"),
                                                         null,
                                                         threshold );

        // Rolling window parameters
        Instant start = Instant.parse( "2015-12-01T00:00:00Z" );
        Duration period = Duration.ofDays( 91 );
        Duration frequency = Duration.ofDays( 30 );

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
                                                      Duration.ofHours( 6 ) );
            DoubleScoreStatistic sixHourOutput =
                    DoubleScoreStatistic.of( sixHourOutputs[i],
                                             StatisticMetadata.of( SampleMetadata.of( source,
                                                                                      sixHourWindow ),
                                                                   90,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                   MetricConstants.MAIN ) );
            rawData.add( sixHourOutput );
            //Add the 12h data
            TimeWindow twelveHourWindow = TimeWindow.of( begin,
                                                         end,
                                                         Duration.ofHours( 12 ) );
            DoubleScoreStatistic twelveHourOutput =
                    DoubleScoreStatistic.of( twelveHourOutputs[i],
                                             StatisticMetadata.of( SampleMetadata.of( source,
                                                                                      twelveHourWindow ),
                                                                   90,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                   MetricConstants.MAIN ) );
            rawData.add( twelveHourOutput );
            //Add the 18h data
            TimeWindow eighteenHourWindow = TimeWindow.of( begin,
                                                           end,
                                                           Duration.ofHours( 18 ) );
            DoubleScoreStatistic eighteenHourOutput =
                    DoubleScoreStatistic.of( eighteenHourOutputs[i],
                                             StatisticMetadata.of( SampleMetadata.of( source,
                                                                                      eighteenHourWindow ),
                                                                   90,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                   MetricConstants.MAIN ) );
            rawData.add( eighteenHourOutput );
            //Add the 24h data
            TimeWindow twentyFourHourWindow = TimeWindow.of( begin,
                                                             end,
                                                             Duration.ofHours( 24 ) );
            DoubleScoreStatistic twentyFourHourOutput =
                    DoubleScoreStatistic.of( twentyFourHourOutputs[i],
                                             StatisticMetadata.of( SampleMetadata.of( source,
                                                                                      twentyFourHourWindow ),
                                                                   90,
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                   MetricConstants.MAIN ) );
            rawData.add( twentyFourHourOutput );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic} comprising the bias fraction
     * for various pooling windows at one threshold (all data). Corresponds to the use case in Redmine ticket #46461.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatistic> getScoreOutputForPoolingWindowsSecond()
    {
        final List<DoubleScoreStatistic> rawData = new ArrayList<>();

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        
        //Source metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( FeatureKey.of( "ABEC2" ),
                                                                               "STREAMFLOW",
                                                                               "NWM" ),
                                                         null,
                                                         threshold );

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
        for ( int i = 0; i < scores.length; i++ )
        {
            String nextDate = "2017-08-08T" + String.format( "%02d", i ) + ":00:00Z";

            TimeWindow timeWindow = TimeWindow.of( Instant.parse( nextDate ),
                                                   Instant.parse( nextDate ),
                                                   Duration.ofHours( 0 ),
                                                   Duration.ofHours( 18 ) );

            rawData.add( DoubleScoreStatistic.of( scores[i],
                                                  StatisticMetadata.of( SampleMetadata.of( source,
                                                                                           timeWindow ),
                                                                        18,
                                                                        MeasurementUnit.of(),
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        MetricConstants.MAIN ) ) );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link PairedStatistic} that comprises a {@link Duration} that represents a time-to-peak error against an
     * {@link Instant} that represents the origin (basis time) of the time-series from which the timing error
     * originates. Contains results for forecasts issued at 12Z each day from 1985-01-01T12:00:00Z to
     * 1985-01-10T12:00:00Z and with a forecast horizon of 336h.
     *
     * @return a paired output of timing errors by basis time
     */

    public static List<PairedStatistic<Instant, Duration>> getTimeToPeakErrors()
    {
        // Create a list of pairs
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
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 336 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                          DatasetIdentifier.of( FeatureKey.of( "DRRC2" ),
                                                                                                "Streamflow",
                                                                                                "HEFS" ),
                                                                          window,
                                                                          threshold ),
                                                       input.size(),
                                                       MeasurementUnit.of( "DURATION" ),
                                                       MetricConstants.TIME_TO_PEAK_ERROR,
                                                       MetricConstants.MAIN );
        // Build and return
        return Arrays.asList( PairedStatistic.of( input, meta ) );
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

    public static List<DurationScoreStatistic> getTimeToPeakErrorStatistics()
    {
        // Create a list of pairs
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
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 336 ) );

        OneOrTwoThresholds threshold = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ) );
        
        StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                          DatasetIdentifier.of( FeatureKey.of( "DRRC2" ),
                                                                                                "Streamflow",
                                                                                                "HEFS" ),
                                                                          window,
                                                                          threshold ),
                                                       10,
                                                       MeasurementUnit.of( "DURATION" ),
                                                       MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                       MetricConstants.MAIN );

        return Arrays.asList( DurationScoreStatistic.of( returnMe, meta ) );
    }

}
