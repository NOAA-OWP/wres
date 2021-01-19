package wres.vis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.datasource.instances.CategoricalXYChartDataSource;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.util.TimeHelper;

/**
 * Used to produce {@link XYChartDataSource} instances for use in constructing charts.  
 * @author Hank.Herr
 *
 */
abstract class XYChartDataSourceFactory
{

    private static final String METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX =
            "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@";
    /**
     * Number of milliseconds in an hour for conversion of {@link Duration} to decimal hours for plotting.
     */

    private static final BigDecimal MILLIS_PER_HOUR = BigDecimal.valueOf( TimeUnit.HOURS.toMillis( 1 ) );

    /**
     * Factory method for box-plot output for a box-plot of errors.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @param subPlotIndex 0 for bottom, 1 for the one above, etc.
     * @param durationUnits the duration units
     * @return A data source to be used to draw the plot.
     * @throws IllegalArgumentException if the input is empty
     */
    static DefaultXYChartDataSource ofBoxPlotOutput( int orderIndex,
                                                     List<BoxplotStatisticOuter> input,
                                                     Integer subPlotIndex,
                                                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( durationUnits );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot generate box plot output with empty input." );
        }

        // One box per pool? See #62374
        boolean pooledInput = input.get( 0 ).getMetricName().isInGroup( StatisticType.BOXPLOT_PER_POOL );

        // Should be one dataset only if it is not per-pool
        if ( !pooledInput && input.size() > 1 )
        {
            throw new IllegalArgumentException( "Cannot generate box plot output per pool with more than one dataset." );
        }

        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource =
                        XYChartDataSourceFactory.ofBoxPlotOutput( orderIndex, input, subPlotIndex, durationUnits );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                // Add a boxplot for output that contains one box per pool. See #62374
                if ( pooledInput )
                {
                    return new BoxPlotDiagramByLeadXYDataset( input, durationUnits );
                }

                return new BoxPlotDiagramXYDataset( input );
            }
        };

        int seriesCount = 0;

        if ( input.get( 0 ).getData().getStatisticsCount() != 0 )
        {
            seriesCount = input.get( 0 ).getData().getMetric().getQuantilesCount();
        }

        XYChartDataSourceFactory.buildInitialParameters( source,
                                                         orderIndex,
                                                         seriesCount );

        if ( pooledInput )
        {
            source.getDefaultFullySpecifiedDataSourceDrawingParameters()
                  .setDefaultDomainAxisTitle( "FORECAST LEAD TIME [" + durationUnits.toString().toUpperCase() + "]" );
        }
        else
        {
            // See #65503 - need a default in case the input is empty
            String inputUnits = "unknown";

            if ( input.get( 0 ).getData().getStatisticsCount() != 0 )
            {
                inputUnits = input.get( 0 ).getData().getMetric().getLinkedValueType().toString();
                inputUnits = inputUnits.replace( "_", " " );
            }

            source.getDefaultFullySpecifiedDataSourceDrawingParameters()
                  .setDefaultDomainAxisTitle( inputUnits + "@inputUnitsLabelSuffix@" );
        }

        // See #65503 - need a default in case the input is empty
        String outputUnits = "unknown";

        if ( input.get( 0 ).getData().getStatisticsCount() != 0 )
        {
            outputUnits = input.get( 0 ).getData().getMetric().getQuantileValueType().toString();
            outputUnits = outputUnits.replace( "_", " " );
        }

        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( outputUnits + "@outputUnitsLabelSuffix@" );

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
    static DefaultXYChartDataSource ofSingleValuedPairs( int orderIndex,
                                                         final SampleData<Pair<Double, Double>> input )
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
    static DefaultXYChartDataSource
            ofPairedOutputInstantDuration( int orderIndex,
                                           final List<DurationDiagramStatisticOuter> input )
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

                Set<OneOrTwoThresholds> thresholds =
                        Slicer.discover( input, next -> next.getMetadata().getThresholds() );

                // Filter by by threshold
                for ( OneOrTwoThresholds nextSeries : thresholds )
                {
                    TimeSeries next =
                            new TimeSeries( nextSeries.toStringWithoutUnits(), FixedMillisecond.class );

                    List<DurationDiagramStatisticOuter> filtered =
                            Slicer.filter( input,
                                           data -> data.getMetadata().getThresholds().equals( nextSeries ) );

                    // Create a set-view by instant, because JFreeChart cannot handle duplicates
                    // TODO: possibly upgrade JFreeChart or use an alternative
                    Set<Instant> instants = new HashSet<>();

                    // Create the series
                    for ( DurationDiagramStatisticOuter nextSet : filtered )
                    {
                        for ( Pair<Instant, Duration> oneValue : nextSet.getPairs() )
                        {
                            if ( !instants.contains( oneValue.getKey() ) )
                            {
                                // Find the decimal hours
                                BigDecimal result = BigDecimal.valueOf( oneValue.getRight().toMillis() )
                                                              .divide( MILLIS_PER_HOUR, 2, RoundingMode.HALF_DOWN );

                                next.add( new FixedMillisecond( oneValue.getLeft().toEpochMilli() ),
                                          result.doubleValue() );

                                instants.add( oneValue.getKey() );
                            }
                        }
                    }
                    returnMe.addSeries( next );
                }

                return returnMe;
            }
        };

        buildInitialParameters( source,
                                orderIndex,
                                Slicer.discover( input, next -> next.getMetadata().getThresholds() )
                                      .size() ); //# of series = number of thresholds in input.
        source.setXAxisType( ChartConstants.AXIS_IS_TIME );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "FORECAST ISSUE DATE/TIME [UTC]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX );

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
     * @param buildDataSetSupplier Supplies the {@link XYDataset} to be used in the data source.  
     *            If null, then a simple {@link DiagramStatisticXYDataset} is used.
     * @param durationUnits the duration units
     * @return A data source that can be used to draw the diagram.
     */
    static DefaultXYChartDataSource ofMultiVectorOutputDiagram( final int orderIndex,
                                                                final List<DiagramStatisticOuter> input,
                                                                final MetricDimension xConstant,
                                                                final MetricDimension yConstant,
                                                                final String domainTitle,
                                                                final String rangeTitle,
                                                                final Integer subPlotIndex,
                                                                final Supplier<XYDataset> buildDataSetSupplier,
                                                                final ChronoUnit durationUnits )
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
                                                                                 buildDataSetSupplier,
                                                                                 durationUnits );
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
                return new DiagramStatisticXYDataset( input,
                                                      xConstant,
                                                      yConstant,
                                                      durationUnits );
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
     * @param durationUnits the duration units
     * @return A data source to be used to draw the plot.
     * @throws NullPointerException if the input or durationUnits are null
     */
    static DefaultXYChartDataSource
            ofDoubleScoreOutputByPoolingWindow( final int orderIndex,
                                                final List<DoubleScoreComponentOuter> input,
                                                final ChronoUnit durationUnits )
    {
        Objects.requireNonNull( input, "Specify non-null input." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {
            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource =
                        ofDoubleScoreOutputByThresholdAndLead( orderIndex, input, durationUnits );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                // Build the TimeSeriesCollection
                TimeSeriesCollection returnMe = new TimeSeriesCollection();

                // Filter by the lead time window, as contained within the TimeWindow portion of the key.
                SortedSet<Pair<Duration, Duration>> durations = Slicer.discover( input,
                                                                                 next -> Pair.of( next.getMetadata()
                                                                                                      .getTimeWindow()
                                                                                                      .getEarliestLeadDuration(),
                                                                                                  next.getMetadata()
                                                                                                      .getTimeWindow()
                                                                                                      .getLatestLeadDuration() ) );

                for ( Pair<Duration, Duration> nextTime : durations )
                {
                    // Slice the data by the lead time in the window.  The resulting output will span
                    // multiple issued time windows and thresholds.
                    List<DoubleScoreComponentOuter> slice = Slicer.filter( input,
                                                                           next -> next.getMetadata()
                                                                                       .getTimeWindow()
                                                                                       .getEarliestLeadDuration()
                                                                                       .equals( nextTime.getLeft() )
                                                                                   && next.getMetadata()
                                                                                          .getTimeWindow()
                                                                                          .getLatestLeadDuration()
                                                                                          .equals( nextTime.getRight() ) );

                    // Filter by threshold
                    SortedSet<OneOrTwoThresholds> thresholds =
                            Slicer.discover( slice, next -> next.getMetadata().getThresholds() );
                    for ( OneOrTwoThresholds nextThreshold : thresholds )
                    {
                        // Slice the data by threshold.  The resulting data will still contain potentially
                        // multiple issued time pooling windows.
                        List<DoubleScoreComponentOuter> finalSlice =
                                Slicer.filter( slice,
                                               next -> next.getMetadata()
                                                           .getThresholds()
                                                           .equals( nextThreshold ) );

                        // Create the time series with a label determined by whether the lead time is a 
                        // single value or window.
                        String leadKey =
                                Long.toString( TimeHelper.durationToLongUnits( nextTime.getRight(), durationUnits ) );
                        if ( !nextTime.getLeft().equals( nextTime.getRight() ) )
                        {
                            leadKey = "(" + TimeHelper.durationToLongUnits( nextTime.getLeft(), durationUnits )
                                      + ","
                                      + TimeHelper.durationToLongUnits( nextTime.getRight(), durationUnits )
                                      + "]";
                        }
                        TimeSeries next = new TimeSeries( leadKey + ", "
                                                          + nextThreshold.toStringWithoutUnits(),
                                                          FixedMillisecond.class );

                        // Loop through the slice, forming a time series from the issued time pooling windows
                        // and corresponding values.
                        for ( DoubleScoreComponentOuter nextDouble : finalSlice )
                        {
                            next.add( new FixedMillisecond( nextDouble.getMetadata()
                                                                      .getTimeWindow()
                                                                      .getMidPointBetweenEarliestAndLatestReferenceTimes()
                                                                      .toEpochMilli() ),
                                      nextDouble.getData().getValue() );
                        }
                        returnMe.addSeries( next );
                    }
                }
                return returnMe;
            }
        };

        SortedSet<Pair<Duration, Duration>> durations = Slicer.discover( input,
                                                                         next -> Pair.of( next.getMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getEarliestLeadDuration(),
                                                                                          next.getMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getLatestLeadDuration() ) );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( input, next -> next.getMetadata().getThresholds() );

        buildInitialParameters( source,
                                orderIndex,
                                durations.size() * thresholds.size() ); //one series per lead and threshold
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "TIME AT CENTER OF WINDOW [UTC]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX );
        source.setXAxisType( ChartConstants.AXIS_IS_TIME );

        return source;
    }

    /**
     * Factory method for scalar output plotted against threshold with lead time in the legend.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @param durationUnits the duration units
     * @return A data source to be used to draw the plot.
     */
    static DefaultXYChartDataSource
            ofDoubleScoreOutputByThresholdAndLead( final int orderIndex,
                                                   final List<DoubleScoreComponentOuter> input,
                                                   final ChronoUnit durationUnits )
    {
        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {

            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource =
                        ofDoubleScoreOutputByThresholdAndLead( orderIndex, input, durationUnits );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new ScoreOutputByThresholdAndLeadXYDataset( input, durationUnits );
            }
        };

        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( input, next -> next.getMetadata().getTimeWindow() );

        buildInitialParameters( source, orderIndex, timeWindows.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "THRESHOLD VALUE@inputUnitsLabelSuffix@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX );

        return source;
    }


    /**     
     * Factory method for scalar output plotted against lead time with threshold in the legend.
     * @param orderIndex Order index of the data source; lower index sources are drawn on top of higher index sources.
     * @param input The data to plot.
     * @param durationUnits the duration units
     * @return A data source to be used to draw the plot.
     * @throws NullPointerException if the input or durationUnits are null
     */
    static DefaultXYChartDataSource
            ofDoubleScoreOutputByLeadAndThreshold( final int orderIndex,
                                                   final List<DoubleScoreComponentOuter> input,
                                                   final ChronoUnit durationUnits )
    {
        Objects.requireNonNull( input, "Specify non-null input." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        DefaultXYChartDataSource source = new DefaultXYChartDataSource()
        {

            @Override
            public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
            {
                DefaultXYChartDataSource newSource =
                        ofDoubleScoreOutputByLeadAndThreshold( orderIndex, input, durationUnits );
                this.copyTheseParametersIntoDataSource( newSource );
                return newSource;
            }

            @Override
            protected XYDataset buildXYDataset( DataSourceDrawingParameters arg0 ) throws XYChartDataSourceException
            {
                return new ScoreOutputByLeadAndThresholdXYDataset( input, durationUnits );
            }
        };

        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( input, next -> next.getMetadata().getThresholds() );

        buildInitialParameters( source, orderIndex, thresholds.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultDomainAxisTitle( "FORECAST LEAD TIME [" + durationUnits.name().toUpperCase() + "]" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX );

        return source;
    }


    /**
     * Creates an instance for duration score output; i.e., summary stats of the time-to-peak errors.
     * @param orderIndex The data source order index.
     * @param input The input required for this of method.
     * @return An instance of {@link CategoricalXYChartDataSource}.
     */
    static CategoricalXYChartDataSource
            ofDurationScoreCategoricalOutput( int orderIndex,
                                              List<DurationScoreStatisticOuter> input )
    {
        String[] xCategories = null;
        List<double[]> yAxisValuesBySeries = new ArrayList<>();
        List<String> legendEntryBySeries = new ArrayList<>();
        boolean populateCategories = false;

        //Build the categories and category values to be passed into the categorical source.
        for ( DurationScoreStatisticOuter entry : input )
        {
            if ( xCategories == null )
            {
                xCategories = new String[entry.getComponents().size()];
                populateCategories = true;
            }
            else
            {
                populateCategories = false;
            }
            double[] yValues = new double[xCategories.length];

            int index = 0;
            for ( MetricConstants metric : entry.getComponents() )
            {
                if ( populateCategories )
                {
                    xCategories[index] = metric.toString();
                }
                else if ( !xCategories[index].equals( metric.toString() ) )
                {
                    throw new IllegalArgumentException( "The named categories are not consistent across all provided input." );
                }

                Duration durationStat = MessageFactory.parse( entry.getComponent( metric ).getData().getValue() );

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

            legendEntryBySeries.add( entry.getMetadata().getThresholds().toStringWithoutUnits() );
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
                             + "with null generator, orderIndex '"
                             + orderIndex
                             + "', xCategories '"
                             + xCategories
                             + "', and yAxisValuesBySeries '"
                             + yAxisValuesBySeries
                             + "' failed when it shouldn't have.";
            throw new IllegalStateException( message, e );
        }

        //Some appearance options specific to the input provided.
        buildInitialParameters( source, orderIndex, yAxisValuesBySeries.size() );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName( "Bar" );
        source.setXAxisType( ChartConstants.AXIS_IS_CATEGORICAL );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "@metricName@" );
        source.getDefaultFullySpecifiedDataSourceDrawingParameters()
              .setDefaultRangeAxisTitle( METRIC_SHORT_NAME_METRIC_COMPONENT_NAME_SUFFIX_OUTPUT_UNITS_LABEL_SUFFIX );
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
