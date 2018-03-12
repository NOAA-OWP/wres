package wres.vis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
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
import wres.datamodel.Thresholds;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

/**
 * Used to produce {@link XYChartDataSource} instances for use in constructing charts.  
 * @author Hank.Herr
 *
 */
public abstract class XYChartDataSourceFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( XYChartDataSourceFactory.class );

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

                // Filter by lead time and then by threshold
                for ( Entry<Pair<TimeWindow, Thresholds>, PairedOutput<Instant, Duration>> entry : input.entrySet() )
                {
                    Long time = entry.getKey().getLeft().getLatestLeadTimeInHours();
                    Thresholds threshold = entry.getKey().getRight();
                    TimeSeries next = new TimeSeries( time + ", " + threshold, FixedMillisecond.class );
                    for ( Pair<Instant, Duration> oneValue : entry.getValue() )
                    {
                        next.add( new FixedMillisecond( oneValue.getLeft().toEpochMilli() ),
                                  oneValue.getRight().toHours() );
                    }
                    returnMe.addSeries( next );
                }
                return returnMe;
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                input.size() ); //# of series = number of entries in input.
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
                    for ( Thresholds nextThreshold : input.setOfThresholdKey() )
                    {
                        // Slice the data by threshold.  The resulting data will still contain potentiall
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
                        TimeSeries next = new TimeSeries( leadKey + ", " + nextThreshold, FixedMillisecond.class );
                        
                        // Loop through the slice, forming a time series from the issued time pooling windows
                        // and corresponding values.
                        for ( Pair<TimeWindow, Thresholds> key : finalSlice.keySet() )
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
        boolean populateCategories = false;

        //Build the categories and category values to be passed into the categorical source.
        for ( Entry<Pair<TimeWindow, Thresholds>, DurationScoreOutput> entry : input.entrySet() )
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
                yValues[index] = durationStat.toHours();
                index++;
            }

            yAxisValuesBySeries.add( yValues );
            populateCategories = false;
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
                        throws XYChartDataSourceException
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
            LOGGER.error( "Well, how the hell did that happen?  Nothing within CategoricalXYChartDataSource even throws the exception, so this should have been impossible!",
                          e );
            throw new IllegalStateException( "Something very bad happened: CategoricalXYChartDataSource threw an XYChartDataSourceException when it should have been impossible." );
        }

        //Some appearance options specific to the input provided.
        buildInitialParameters( source, orderIndex, yAxisValuesBySeries.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName( "Bar" );
        source.setXAxisType( ChartConstants.AXIS_IS_CATEGORICAL );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "@metricName@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );

        return source;
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
}
