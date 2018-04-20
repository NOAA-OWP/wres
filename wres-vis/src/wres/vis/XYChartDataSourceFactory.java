package wres.vis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.CategoricalXYChartDataSource;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * Used to produce {@link XYChartDataSource} instances for use in constructing charts.  
 * @author Hank.Herr
 *
 */
public abstract class XYChartDataSourceFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( XYChartDataSourceFactory.class );

    /**
     * Number of milliseconds in an hour for conversion of {@link Duration} to decimal hours for plotting.
     */

    private static final BigDecimal MILLIS_PER_HOUR = BigDecimal.valueOf( TimeUnit.HOURS.toMillis( 1 ) );

    /**
     * Factory method for box-plot output for a box-plot of errors.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @param subPlotIndex 0 for bottom, 1 for the one above, etc.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource ofBoxPlotOutput( int orderIndex,
                                                            final BoxPlotOutput input,
                                                            Integer subPlotIndex )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofBoxPlotOutput( orderIndex, input, subPlotIndex );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new BoxPlotDiagramXYDataset( input );
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                input.getProbabilities().size() );

        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( input.getDomainAxisDimension()
                                               .toString()
                                          + "@inputUnitsLabelSuffix@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( input.getRangeAxisDimension()
                                              .toString()
                                         + "@outputUnitsLabelSuffix@" );

        if ( ( subPlotIndex != null ) && ( subPlotIndex >= 0 ) )
        {
            source.getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( subPlotIndex );
        }

        return source;
    }

    /**
     * Factory method for single-valued pairs.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource ofSingleValuedPairs( int orderIndex, final SingleValuedPairs input )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofSingleValuedPairs( orderIndex, input );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new SingleValuedPairsXYDataset( input );
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                1 );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "@domainAxisLabelPrefix@@inputUnitsLabelSuffix@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@rangeAxisLabelPrefix@@inputUnitsLabelSuffix@" );

        return source;
    }

    /**
     * Factory method for paired (instant, duration) output.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource
            ofPairedOutputInstantDuration( int orderIndex,
                                           final MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> input )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofPairedOutputInstantDuration( orderIndex, input );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                // Build the TimeSeriesCollection
                TimeSeriesCollection returnMe = new TimeSeriesCollection();

                Set<OneOrTwoThresholds> thresholds = input.setOfThresholdKey();
                
                // Filter by by threshold
                for ( OneOrTwoThresholds nextSeries : thresholds )
                {
                    TimeSeries next =
                            new TimeSeries( nextSeries.toStringWithoutUnits(), FixedMillisecond.class );
                    
                    MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> filtered = input.filterByThreshold( nextSeries );
                    // Create the series
                    for ( PairedOutput<Instant, Duration> nextSet : filtered.values() )
                    {
                        for ( Pair<Instant, Duration> oneValue : nextSet )
                        {
                            // Find the decimal hours
                            BigDecimal result = BigDecimal.valueOf( oneValue.getRight().toMillis() )
                                                          .divide( MILLIS_PER_HOUR, 2, RoundingMode.HALF_DOWN );

                            next.add( new FixedMillisecond( oneValue.getLeft().toEpochMilli() ),
                                      result.doubleValue() );
                        }
                    }
                    returnMe.addSeries( next );
                }
                
                return returnMe;
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                input.setOfThresholdKey().size() ); //# of series = number of thresholds in input.
        source.setXAxisType( ChartConstants.AXIS_IS_TIME );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "FORECAST ISSUE DATE/TIME [UTC]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );

        return source;
    }


    /**
     * Factory method for diagrams.  Because of the flexible nature of diagrams, a larger number of arguments is required.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @param xConstant The metric defining the x-values.
     * @param yConstant The metric defining the y-values.
     * @param domainTitle Title for the domain.
     * @param rangeTitle Title for the range.
     * @param subPlotIndex The subplot index to use.  If null or negative, zero is assumed.
     * @param buildDataSetSupplier Supplies the {@link XYDataset} to be used in the data source.  If null, then a simple {@link MultiVectorOutputDiagramXYDataset}
     * is used.
     * @return A data source that can be used to draw the diagram.
     */
    public static DefaultXYChartDataSource ofMultiVectorOutputDiagram( final int orderIndex,
                                                                       final MetricOutputMapByTimeAndThreshold<MultiVectorOutput> input,
                                                                       final MetricDimension xConstant,
                                                                       final MetricDimension yConstant,
                                                                       final String domainTitle,
                                                                       final String rangeTitle,
                                                                       final Integer subPlotIndex,
                                                                       Supplier<XYDataset> buildDataSetSupplier )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofMultiVectorOutputDiagram( orderIndex,
                                                                                 input,
                                                                                 xConstant,
                                                                                 yConstant,
                                                                                 domainTitle,
                                                                                 rangeTitle,
                                                                                 subPlotIndex,
                                                                                 buildDataSetSupplier );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                if ( buildDataSetSupplier != null )
                {
                    return buildDataSetSupplier.get();
                }
                return new MultiVectorOutputDiagramXYDataset( input,
                                                              xConstant,
                                                              yConstant );
            }
        };

        buildInitialParameters( source, orderIndex, input.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( domainTitle );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( rangeTitle );

        if ( ( subPlotIndex != null ) && ( subPlotIndex >= 0 ) )
        {
            source.getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( subPlotIndex );
        }

        return source;
    }


    /**
     * Factory method for scalar output plotted by pooling window.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource
            ofDoubleScoreOutputByPoolingWindow( int orderIndex,
                                                final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofDoubleScoreOutputByThresholdAndLead( orderIndex, input );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                // Build the TimeSeriesCollection
                TimeSeriesCollection returnMe = new TimeSeriesCollection();

                // Filter by the lead time window, as contained within the TimeWindow portion of the key.
                for ( TimeWindow nextTime : input.setOfTimeWindowKeyByLeadTime() )
                {
                    // Slice the data by the lead time in the window.  The resulting output will span
                    // multiple issued time windows and thresholds.
                    MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> slice =
                            input.filterByLeadTime( nextTime );

                    // Filter by threshold
                    for ( OneOrTwoThresholds nextThreshold : input.setOfThresholdKey() )
                    {
                        // Slice the data by threshold.  The resulting data will still contain potentially
                        // multiple issued time pooling windows.
                        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> finalSlice =
                                slice.filterByThreshold( nextThreshold );

                        // Create the time series with a label determined by whether the lead time is a 
                        // single value or window.
                        String leadKey = Long.toString( nextTime.getLatestLeadTime().toHours() );
                        if ( !nextTime.getEarliestLeadTime().equals( nextTime.getLatestLeadTime() ) )
                        {
                            leadKey = "(" + nextTime.getEarliestLeadTime().toHours()
                                      + ","
                                      + nextTime.getLatestLeadTime().toHours()
                                      + "]";
                        }
                        TimeSeries next = new TimeSeries( leadKey + ", "
                                                          + nextThreshold.toStringWithoutUnits(),
                                                          FixedMillisecond.class );

                        // Loop through the slice, forming a time series from the issued time pooling windows
                        // and corresponding values.
                        for ( Pair<TimeWindow, OneOrTwoThresholds> key : finalSlice.keySet() )
                        {
                            next.add( new FixedMillisecond( key.getLeft().getMidPointTime().toEpochMilli() ),
                                      finalSlice.get( key ).getData() );
                        }
                        returnMe.addSeries( next );
                    }
                }
                return returnMe;
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                input.setOfTimeWindowKeyByLeadTime().size() * input.setOfThresholdKey().size() ); //one series per lead and threshold
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "TIME AT CENTER OF WINDOW [UTC]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        source.setXAxisType( ChartConstants.AXIS_IS_TIME );

        return source;
    }

    /**
     * Factory method for scalar output plotted against threshold with lead time in the legend.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource
            ofDoubleScoreOutputByThresholdAndLead( int orderIndex,
                                                   final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {

            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofDoubleScoreOutputByThresholdAndLead( orderIndex, input );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new ScoreOutputByThresholdAndLeadXYDataset( input );
            }
        };

        buildInitialParameters( source, orderIndex, input.setOfTimeWindowKey().size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "THRESHOLD VALUE@inputUnitsLabelSuffix@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );

        return source;
    }


    /**     
     * Factory method for scalar output plotted against lead time with threshold in the legend.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @return A data source to be used to draw the plot.
     */
    public static DefaultXYChartDataSource
            ofDoubleScoreOutputByLeadAndThreshold( int orderIndex,
                                                   final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {

            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource = ofDoubleScoreOutputByLeadAndThreshold( orderIndex, input );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new ScoreOutputByLeadAndThresholdXYDataset( input );
            }
        };

        buildInitialParameters( source, orderIndex, input.setOfThresholdKey().size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "FORECAST LEAD TIME [HOUR]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );

        return source;
    }


    /**
     * Creates an instance for duration score output; i.e., summary stats of the time-to-peak errors.
     * @param orderIndex The data source order index.
     * @param input The input required for this of method.
     * @return An instance of {@link CategoricalXYChartDataSource}.
     */
    public static CategoricalXYChartDataSource
            ofDurationScoreCategoricalOutput( int orderIndex,
                                              MetricOutputMapByTimeAndThreshold<DurationScoreOutput> input )
    {
        String[] xCategories = null;
        List<double[]> yAxisValuesBySeries = new ArrayList<>();
        List<String> legendEntryBySeries = new ArrayList<>();
        boolean populateCategories = false;

        //Build the categories and category values to be passed into the categorical source.
        for ( Entry<Pair<TimeWindow, OneOrTwoThresholds>, DurationScoreOutput> entry : input.entrySet() )
        {
            if ( xCategories == null )
            {
                xCategories = new String[entry.getValue().getComponents().size()];
                populateCategories = true;
            }
            else
            {
                populateCategories = false;
            }
            double[] yValues = new double[xCategories.length];

            DurationScoreOutput output = entry.getValue();
            int index = 0;
            for ( MetricConstants metric : output.getComponents() )
            {
                if ( populateCategories )
                {
                    xCategories[index] = metric.toString();
                }
                else if ( !xCategories[index].equals( metric.toString() ) )
                {
                    throw new IllegalArgumentException( "The named categories are not consistent across all provided input." );
                }

                Duration durationStat = output.getComponent( metric ).getData();

                // Find the decimal hours
                double doubleResult = Double.NaN;
                if ( Objects.nonNull( durationStat ) )
                {
                    BigDecimal result = BigDecimal.valueOf( durationStat.toMillis() )
                                                  .divide( MILLIS_PER_HOUR, 2, RoundingMode.HALF_DOWN );
                    doubleResult = result.doubleValue();
                }
                yValues[index] = doubleResult;
                index++;
            }
            yAxisValuesBySeries.add( yValues );

            //Define the legend entry.  The commented out code specifies the lead time.  But we decided
            //not to include it for this plot.
//            TimeWindow timeWindow = entry.getKey().getLeft();
//            String leadKey = Long.toString( timeWindow.getLatestLeadTime().toHours() );
//            if ( !timeWindow.getEarliestLeadTime().equals( timeWindow.getLatestLeadTime() ) )
//            {
//                leadKey = "(" + timeWindow.getEarliestLeadTime().toHours()
//                          + ","
//                          + timeWindow.getLatestLeadTime().toHours()
//                          + "]";
//            }
            legendEntryBySeries.add( entry.getKey().getRight().toStringWithoutUnits() );
        }

        //Creates the source.
        CategoricalXYChartDataSource source;
        try
        {
            source = new CategoricalXYChartDataSource( null,
                                                       orderIndex,
                                                       xCategories,
                                                       yAxisValuesBySeries )
            {
                @Override
                public CategoricalXYChartDataSource returnNewInstanceWithCopyOfInitialParameters()
                {
                    CategoricalXYChartDataSource newSource =
                            ofDurationScoreCategoricalOutput( getDataSourceOrderIndex(), input );
                    this.copyTheseParametersIntoDataSource( newSource );
                    return newSource;
                }
            };
        }
        catch ( XYChartDataSourceException e )
        {
            String message = "Construction of CategoricalXYChartDataSource "
                             + "with null generator, orderIndex '" + orderIndex
                             + "', xCategories '" + String.valueOf( xCategories )
                             + "', and yAxisValuesBySeries '" + yAxisValuesBySeries
                             + "' failed when it shouldn't have.";
            throw new IllegalStateException( message, e );
        }

        //Some appearance options specific to the input provided.
        buildInitialParameters( source, orderIndex, yAxisValuesBySeries.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName( "Bar" );
        source.setXAxisType( ChartConstants.AXIS_IS_CATEGORICAL );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "@metricName@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        applyEvenNonOverlappingBarDistribution( source.getDefaultFullySpecifiedDataSourceDrawingParameters() );

        //Pass in the legend entries here.
        int index = 0;
        for ( String legendEntry : legendEntryBySeries )
        {
            source.getDefaultFullySpecifiedDataSourceDrawingParameters()
                  .getSeriesDrawingParametersForSeriesIndex( index )
                  .setNameInLegend( legendEntry );
            index++;
        }

        return source;
    }

    /**
     * Applies parameters that will force the bars in the categorical plot to fill in all available space, 
     * each bar being equal width (if multiple chart series), and no overlap between bars. This should be 
     * moved to the CHPS code base, ChartTools class, I think.
     * @param dataSourceParms The parameters to be modified, which must have already had their series 
     *     drawing parameters initialized for the number of series.
     */
    private static void applyEvenNonOverlappingBarDistribution( DataSourceDrawingParameters dataSourceParms )
    {
        int count = dataSourceParms.getSeriesParametersCount();
        for ( SeriesDrawingParameters seriesParms : dataSourceParms.getSeriesDrawingParameters() )
        {
            seriesParms.setBarWidth( (float) ( 0.9 / count ) );
            seriesParms.setBoxWidth( 0.0d );
        }
    }


    /**
     * Initializes the parameters, including creating {@link SeriesDrawingParameters} for each series.  
     * See the method code to identify the default options and which options must be overridden.
     * @param source The source.
     * @param orderIndex The order index of the source.  Note this sets the default source drawing parameters to use this source index.
     * @param numberOfSeries The number of series.
     */
    private static void buildInitialParameters( DefaultXYChartDataSource source,
                                                int orderIndex,
                                                final int numberOfSeries )
    {
        source.setXAxisType( ChartConstants.AXIS_IS_NUMERICAL );
        source.setComputedDataType( ChartConstants.AXIS_IS_NUMERICAL );

        //Setup the initial data source drawing parameters.
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex( orderIndex );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName( "LineAndScatter" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( 0 );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex( 0 );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "INSERT DEFAULT DOMAIN AXIS TITLE HERE!" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "INSERT DEFAULT RANGE AXIS TITLE HERE!" );

        //Prepare series parameters.  
        //The synchronize method will create the needed series parameter objects.
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .synchronizeSeriesParametersListWithNumberOfSeriesPlotted( numberOfSeries );
        for ( int i = 0; i < numberOfSeries; i++ )
        {
            final SeriesDrawingParameters seriesParms =
                    source.getDefaultFullySpecifiedDataSourceDrawingParameters()
                          .getSeriesDrawingParametersForSeriesIndex( i );
            seriesParms.setupDefaultParameters();
            seriesParms.setNameInLegend( "" );
        }

        //Apply looping color and shape scheme.
        WRESTools.applyDefaultJFreeChartColorSequence( source.getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( source.getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    /**
     * Prevent construction.
     */

    private XYChartDataSourceFactory()
    {
    }

}
