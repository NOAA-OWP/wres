package wres.vis.data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYIntervalSeries;
import org.jfree.data.xy.XYIntervalSeriesCollection;
import org.jfree.data.xy.YIntervalSeries;
import org.jfree.data.xy.YIntervalSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.vis.charts.ChartFactory.ChartType;

/**
 * Used to create datasets for constructing charts.
 *
 * @author James Brown
 * @author Hank Herr
 */
public class ChartDataFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartDataFactory.class );

    /** Number of milliseconds in an hour for conversion of {@link Duration} to decimal hours for plotting. */
    private static final BigDecimal MILLIS_PER_HOUR = BigDecimal.valueOf( TimeUnit.HOURS.toMillis( 1 ) );

    /**
     * Returns a dataset for one verification score organized by lead duration and threshold.
     * @param statistics the statistics to plot
     * @param durationUnits the duration units
     * @return a data source to be used to draw the plot
     * @throws NullPointerException if either input is null
     */
    public static XYDataset ofDoubleScoreByLeadAndThreshold( List<DoubleScoreComponentOuter> statistics,
                                                             ChronoUnit durationUnits )
    {
        // Arrange the series by threshold
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( statistics, next -> next.getPoolMetadata().getThresholds() );

        YIntervalSeriesCollection dataset = new YIntervalSeriesCollection();

        // Create the function that generates a domain axis value
        ToDoubleFunction<DoubleScoreComponentOuter> domainValue = score ->
        {
            Duration lead = score.getPoolMetadata()
                                 .getTimeWindow()
                                 .getLatestLeadDuration();

            Number durationNumber = DataUtilities.durationToNumericUnits( lead, durationUnits );
            return durationNumber.doubleValue();
        };

        for ( OneOrTwoThresholds key : thresholds )
        {
            List<DoubleScoreComponentOuter> sliced = Slicer.filter( statistics,
                                                                    next -> next.getPoolMetadata()
                                                                                .getThresholds()
                                                                                .equals( key ) );

            String name = DataUtilities.toStringWithoutUnits( key );
            YIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( sliced, name, domainValue );
            dataset.addSeries( nextSeries );
        }

        return dataset;
    }

    /**
     * Returns a dataset for one verification score organized by threshold and lead duration.
     * @param statistics the statistics to plot
     * @param durationUnits the duration units
     * @return a data source to be used to draw the plot
     * @throws NullPointerException if either input is null
     */
    public static XYDataset ofDoubleScoreByThresholdAndLead( List<DoubleScoreComponentOuter> statistics,
                                                             ChronoUnit durationUnits )
    {
        // Arrange by time window
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( statistics, next -> next.getPoolMetadata().getTimeWindow() );

        // Create the function that generates a domain axis value
        ToDoubleFunction<DoubleScoreComponentOuter> domainValue = score -> score.getPoolMetadata()
                                                                                .getThresholds()
                                                                                .first()
                                                                                .getValues()
                                                                                .first();

        YIntervalSeriesCollection dataset = new YIntervalSeriesCollection();

        for ( TimeWindowOuter key : timeWindows )
        {
            List<DoubleScoreComponentOuter> sliced = Slicer.filter( statistics,
                                                                    next -> next.getPoolMetadata()
                                                                                .getTimeWindow()
                                                                                .equals( key )
                                                                            && !next.getPoolMetadata()
                                                                                    .getThresholds()
                                                                                    .first()
                                                                                    .isAllDataThreshold() );

            Number leadDuration = DataUtilities.durationToNumericUnits( key.getLatestLeadDuration(),
                                                                        durationUnits );
            String name = leadDuration.toString();

            YIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( sliced, name, domainValue );
            dataset.addSeries( nextSeries );
        }

        return dataset;
    }

    /**
     * Returns a dataset with one verification score organized by lead time pool window.
     * @param statistics the rank histogram statistics
     * @param durationUnits the duration units
     * @param graphicShape the graphic shape
     * @return a data source to be used to draw the plot
     * @throws NullPointerException if any input is null
     */
    public static XYDataset ofDoubleScoreByPoolingWindow( List<DoubleScoreComponentOuter> statistics,
                                                          ChronoUnit durationUnits,
                                                          GraphicShape graphicShape )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );
        Objects.requireNonNull( graphicShape );

        // Set the chart parameters for each series
        // Find the lead durations
        SortedSet<Pair<Duration, Duration>> durations = Slicer.discover( statistics,
                                                                         next -> Pair.of( next.getPoolMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getEarliestLeadDuration(),
                                                                                          next.getPoolMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getLatestLeadDuration() ) );

        // Build the TimeSeriesCollection
        YIntervalSeriesCollection returnMe = new YIntervalSeriesCollection();

        // Filter by times if each series should contain issued or valid pools, else do not filter
        SortedSet<Pair<Instant, Instant>> times = new TreeSet<>();

        // Series by issued time 
        if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS )
        {
            SortedSet<Pair<Instant, Instant>> uniqueValidTimes = Slicer.discover( statistics,
                                                                                  next -> Pair.of( next.getPoolMetadata()
                                                                                                       .getTimeWindow()
                                                                                                       .getEarliestValidTime(),
                                                                                                   next.getPoolMetadata()
                                                                                                       .getTimeWindow()
                                                                                                       .getLatestValidTime() ) );
            times.addAll( uniqueValidTimes );
        }
        else if ( graphicShape == GraphicShape.VALID_DATE_POOLS )
        {
            SortedSet<Pair<Instant, Instant>> uniqueReferenceTimes = Slicer.discover( statistics,
                                                                                      next -> Pair.of( next.getPoolMetadata()
                                                                                                           .getTimeWindow()
                                                                                                           .getEarliestReferenceTime(),
                                                                                                       next.getPoolMetadata()
                                                                                                           .getTimeWindow()
                                                                                                           .getLatestReferenceTime() ) );
            times.addAll( uniqueReferenceTimes );
        }
        else
        {
            times.add( Pair.of( Instant.MIN, Instant.MAX ) );
        }

        // Iterate the durations
        for ( Pair<Duration, Duration> nextDuration : durations )
        {
            // Iterate the times
            for ( Pair<Instant, Instant> nextTime : times )
            {
                // Slice the data by the lead duration
                List<DoubleScoreComponentOuter> slice = Slicer.filter( statistics,
                                                                       next -> next.getPoolMetadata()
                                                                                   .getTimeWindow()
                                                                                   .getEarliestLeadDuration()
                                                                                   .equals( nextDuration.getLeft() )
                                                                               && next.getPoolMetadata()
                                                                                      .getTimeWindow()
                                                                                      .getLatestLeadDuration()
                                                                                      .equals( nextDuration.getRight() ) );

                // Slice the data by valid time if the series should contain issued times
                if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS )
                {
                    slice = Slicer.filter( slice,
                                           next -> next.getPoolMetadata()
                                                       .getTimeWindow()
                                                       .getEarliestValidTime()
                                                       .equals( nextTime.getLeft() )
                                                   && next.getPoolMetadata()
                                                          .getTimeWindow()
                                                          .getLatestValidTime()
                                                          .equals( nextTime.getRight() ) );
                }
                else if ( graphicShape == GraphicShape.VALID_DATE_POOLS )
                {
                    slice = Slicer.filter( slice,
                                           next -> next.getPoolMetadata()
                                                       .getTimeWindow()
                                                       .getEarliestReferenceTime()
                                                       .equals( nextTime.getLeft() )
                                                   && next.getPoolMetadata()
                                                          .getTimeWindow()
                                                          .getLatestReferenceTime()
                                                          .equals( nextTime.getRight() ) );
                }

                // Add the next set of series
                ChartDataFactory.addSeriesForPoolingWindow( returnMe,
                                                            slice,
                                                            nextDuration,
                                                            nextTime,
                                                            graphicShape,
                                                            durationUnits );
            }
        }

        return returnMe;
    }

    /**
     * Returns a dataset for duration score statistics.
     * @param statistics the categorical statistics
     * @return a data source to be used to draw the plot
     * @throws NullPointerException if any input is null
     */
    public static CategoryDataset ofDurationScoreSummaryStatistics( List<DurationScoreStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics );

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // Add the data items
        for ( DurationScoreStatisticOuter entry : statistics )
        {
            String rowKey = DataUtilities.toStringWithoutUnits( entry.getPoolMetadata()
                                                                     .getThresholds() );

            for ( MetricConstants metric : entry.getComponents() )
            {
                com.google.protobuf.Duration score = entry.getComponent( metric )
                                                          .getStatistic()
                                                          .getValue();
                Duration durationStat = MessageFactory.parse( score );

                // Find the decimal hours
                double doubleResult = Double.NaN;
                if ( Objects.nonNull( durationStat ) )
                {
                    BigDecimal result = BigDecimal.valueOf( durationStat.toMillis() )
                                                  .divide( MILLIS_PER_HOUR, 2, RoundingMode.HALF_DOWN );
                    doubleResult = result.doubleValue();
                }

                String columnKey = metric.toString();

                dataset.addValue( doubleResult, rowKey, columnKey );
            }
        }

        return dataset;
    }

    /**
     * Returns a dataset for paired (instant, duration) statistics.
     * @param statistics the statistics to plot
     * @return a data source to be used to draw the plot
     * @throws NullPointerException if the input is null
     */
    public static XYDataset ofDurationDiagramStatistics( List<DurationDiagramStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics );

        // Build the TimeSeriesCollection
        TimeSeriesCollection returnMe = new TimeSeriesCollection();

        Set<OneOrTwoThresholds> thresholds =
                Slicer.discover( statistics, next -> next.getPoolMetadata().getThresholds() );

        // Filter by threshold
        for ( OneOrTwoThresholds nextSeries : thresholds )
        {
            String noUnits = DataUtilities.toStringWithoutUnits( nextSeries );
            TimeSeries next = new TimeSeries( noUnits );

            List<DurationDiagramStatisticOuter> filtered =
                    Slicer.filter( statistics,
                                   data -> data.getPoolMetadata().getThresholds().equals( nextSeries ) );

            // Create a set-view by instant, because JFreeChart cannot handle duplicates
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

    /**
     * Returns a dataset for a verification diagram organized by lead duration (one) and then threshold (up to many).
     * @param statistics the statistics
     * @param xDimension the metric dimension for the X axis
     * @param yDimension the metric dimension for the Y axis
     * @param durationUnits the duration units
     * @return a data source that can be used to draw a plot
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dataset contains more than one lead duration
     */
    public static XYDataset ofVerificationDiagramByLeadAndThreshold( List<DiagramStatisticOuter> statistics,
                                                                     MetricDimension xDimension,
                                                                     MetricDimension yDimension,
                                                                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );

        DiagramStatisticOuter first = statistics.get( 0 );
        MetricConstants metricName = first.getMetricName();

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getThresholds() )
                                   .size();

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a diagram chart with no statistics." );
        }

        if ( timeWindowCount > 1 )
        {
            throw new IllegalArgumentException( "Received an unexpected collection of statistics for the " + metricName
                                                + ". Expected a single time window and one or more thresholds, but "
                                                + "found "
                                                + timeWindowCount
                                                + " time windows and "
                                                + thresholdCount
                                                + " thresholds." );
        }

        // Arrange the series by threshold
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( statistics, next -> next.getPoolMetadata()
                                                         .getThresholds() );

        XYIntervalSeriesCollection dataset = new XYIntervalSeriesCollection();

        for ( OneOrTwoThresholds key : thresholds )
        {
            List<DiagramStatisticOuter> sliced = Slicer.filter( statistics,
                                                                next -> next.getPoolMetadata()
                                                                            .getThresholds()
                                                                            .equals( key ) );

            SortedSet<String> qualified = sliced.stream()
                                                .flatMap( n -> n.getStatistic()
                                                                .getStatisticsList()
                                                                .stream() )
                                                .map( DiagramStatisticComponent::getName )
                                                .filter( n -> !n.isBlank() )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

            // No qualifiers
            if ( qualified.isEmpty() )
            {
                String name = DataUtilities.toStringWithoutUnits( key );
                XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( sliced,
                                                                                  name,
                                                                                  xDimension,
                                                                                  yDimension );
                dataset.addSeries( nextSeries );
            }
            // Slice by qualifier
            else
            {
                for ( String name : qualified )
                {
                    List<DiagramStatisticOuter> slicedInner = Slicer.filter( sliced,
                                                                             next -> next.getComponentNameQualifiers()
                                                                                         .stream()
                                                                                         .anyMatch( name::equals ) );
                    XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( slicedInner,
                                                                                      name,
                                                                                      xDimension,
                                                                                      yDimension );
                    dataset.addSeries( nextSeries );
                }
            }
        }

        return dataset;
    }

    /**
     * Returns a dataset for a verification diagram organized by threshold (one) and then lead duration (up to many).
     * @param statistics the statistics
     * @param xDimension the metric dimension for the X axis
     * @param yDimension the metric dimension for the Y axis
     * @param durationUnits the duration units
     * @return a data source that can be used to draw a plot
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */
    public static XYDataset ofVerificationDiagramByThresholdAndLead( List<DiagramStatisticOuter> statistics,
                                                                     MetricDimension xDimension,
                                                                     MetricDimension yDimension,
                                                                     ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );


        DiagramStatisticOuter first = statistics.get( 0 );
        MetricConstants metricName = first.getMetricName();

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata().getThresholds() )
                                   .size();

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a diagram chart with no statistics." );
        }

        if ( thresholdCount > 1 )
        {
            throw new IllegalArgumentException( "Received an unexpected collection of statistics for the " + metricName
                                                + ". Expected a single threshold and one or more time windows, but "
                                                + "found "
                                                + timeWindowCount
                                                + " time windows and "
                                                + thresholdCount
                                                + " thresholds." );
        }

        // Arrange by time window
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( statistics, next -> next.getPoolMetadata().getTimeWindow() );

        XYIntervalSeriesCollection dataset = new XYIntervalSeriesCollection();

        for ( TimeWindowOuter key : timeWindows )
        {
            List<DiagramStatisticOuter> sliced = Slicer.filter( statistics,
                                                                next -> next.getPoolMetadata()
                                                                            .getTimeWindow()
                                                                            .equals( key ) );

            SortedSet<String> qualified = sliced.stream()
                                                .flatMap( n -> n.getStatistic()
                                                                .getStatisticsList()
                                                                .stream() )
                                                .map( DiagramStatisticComponent::getName )
                                                .filter( n -> !n.isBlank() )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

            // No qualifiers
            if ( qualified.isEmpty() )
            {
                Number leadDuration = DataUtilities.durationToNumericUnits( key.getLatestLeadDuration(),
                                                                            durationUnits );
                String name = leadDuration.toString();

                XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( sliced,
                                                                                  name,
                                                                                  xDimension,
                                                                                  yDimension );
                dataset.addSeries( nextSeries );
            }
            // Slice by qualifier
            else
            {
                for ( String name : qualified )
                {
                    List<DiagramStatisticOuter> slicedInner = Slicer.filter( sliced,
                                                                             next -> next.getComponentNameQualifiers()
                                                                                         .stream()
                                                                                         .anyMatch( name::equals ) );
                    XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( slicedInner,
                                                                                      name,
                                                                                      xDimension,
                                                                                      yDimension );
                    dataset.addSeries( nextSeries );
                }
            }
        }

        return dataset;
    }

    /**
     * Returns a dataset for a box plot diagram.
     * @param statistics the statistics
     * @param durationUnits the duration units
     * @return a box plot dataset for charting
     * @throws NullPointerException if any input is null
     */
    public static XYDataset ofBoxplotStatistics( List<BoxplotStatisticOuter> statistics,
                                                 ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );

        Objects.requireNonNull( durationUnits );

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot generate box plot output with empty input." );
        }

        // One box per pool? See #62374
        boolean pooledInput = statistics.get( 0 )
                                        .getMetricName()
                                        .isInGroup( StatisticType.BOXPLOT_PER_POOL );

        // Add a boxplot for output that contains one box per pool. See #62374
        if ( pooledInput )
        {
            return BoxplotByLead.of( statistics, durationUnits );
        }

        return Boxplot.of( statistics );
    }

    /**
     * Slices a list of {@link DiagramStatisticOuter} by key type and decomposes by component name.
     * @param keyInstance the key instance corresponding to the slice to create
     * @param statistics the statistics from which to form a slice
     * @param chartType the plot type.
     * @return a single slice for use in drawing the diagram.
     */
    public static List<DiagramStatisticOuter> getSlicedStatisticsForDiagram( Object keyInstance,
                                                                             List<DiagramStatisticOuter> statistics,
                                                                             ChartType chartType )
    {
        List<DiagramStatisticOuter> inputSlice;
        if ( chartType == ChartType.LEAD_THRESHOLD )
        {
            inputSlice =
                    Slicer.filter( statistics,
                                   next -> next.getPoolMetadata()
                                               .getTimeWindow()
                                               .equals( keyInstance ) );
        }
        else if ( chartType == ChartType.THRESHOLD_LEAD )
        {
            inputSlice =
                    Slicer.filter( statistics,
                                   next -> next.getPoolMetadata()
                                               .getThresholds()
                                               .equals( keyInstance ) );
        }
        else if ( chartType == ChartType.POOLING_WINDOW )
        {
            inputSlice =
                    Slicer.filter( statistics,
                                   next -> next.getPoolMetadata()
                                               .getTimeWindow()
                                               .equals( keyInstance ) );
        }
        else
        {
            throw new IllegalArgumentException( "Plot type " + chartType
                                                + " is invalid for this diagram." );
        }

        // One diagram for each qualifier name in the diagram statistic. This accounts for diagrams that contain the
        // same metric dimensions multiple times, such as ensemble QQ diagrams
        return ChartDataFactory.getDecomposedDiagrams( inputSlice );
    }

    /**
     * Obtains the quantiles from the statistics, if any.
     *
     * @param <T> the type opf statistics
     * @param statistics the statistics
     * @return the sample quantiles
     */

    public static <T extends Statistic<?>> SortedSet<Double> getQuantiles( List<T> statistics )
    {
        SortedSet<Double> quantiles = statistics.stream()
                                                .filter( Statistic::hasQuantile )
                                                .mapToDouble( n -> n.getSampleQuantile() )
                                                .boxed()
                                                .collect( Collectors.toCollection( TreeSet::new ) );

        return Collections.unmodifiableSortedSet( quantiles );
    }

    /**
     * @param diagrams the diagrams to decompose
     * @return one diagram for each qualifier name
     */

    private static List<DiagramStatisticOuter> getDecomposedDiagrams( List<DiagramStatisticOuter> diagrams )
    {
        List<DiagramStatisticOuter> returnMe = new ArrayList<>();

        for ( DiagramStatisticOuter diagram : diagrams )
        {
            SortedSet<MetricDimension> names = diagram.getComponentNames();
            SortedSet<String> qualifiers = diagram.getComponentNameQualifiers();

            DiagramStatistic.Builder builder = diagram.getStatistic().toBuilder();
            for ( String qualifier : qualifiers )
            {
                List<DiagramStatisticComponent> components = new ArrayList<>();

                for ( MetricDimension name : names )
                {
                    DiagramStatisticComponent next = diagram.getComponent( name, qualifier );
                    components.add( next );
                }

                // Add the new diagram for this qualifier
                builder.clearStatistics();
                builder.addAllStatistics( components );

                DiagramStatistic newDiagram = builder.build();
                DiagramStatisticOuter newWrappedDiagram =
                        DiagramStatisticOuter.of( newDiagram, diagram.getPoolMetadata(), diagram.getSampleQuantile() );
                returnMe.add( newWrappedDiagram );
            }
        }

        LOGGER.debug( "Decomposed {} diagrams into {} diagrams for plotting.", diagrams.size(), returnMe.size() );

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Adds one or more series to a collection of time-based pools based on their lead durations and pooling times.
     *
     * @param collection the collection to expand
     * @param slice the slice of statistics from which to generate series
     * @param leadDurations the lead durations by which to filter series
     * @param poolingTimes the times by which to filter series
     * @param graphicShape the graphic shape
     * @param durationUnits the duration units
     */

    private static void addSeriesForPoolingWindow( YIntervalSeriesCollection collection,
                                                   List<DoubleScoreComponentOuter> slice,
                                                   Pair<Duration, Duration> leadDurations,
                                                   Pair<Instant, Instant> poolingTimes,
                                                   GraphicShape graphicShape,
                                                   ChronoUnit durationUnits )
    {
        // Filter by threshold
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( slice, next -> next.getPoolMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            // Slice the data by threshold.  The resulting data will still contain potentially
            // multiple issued time pooling windows and/or valid time pooling windows.
            List<DoubleScoreComponentOuter> finalSlice =
                    Slicer.filter( slice,
                                   next -> next.getPoolMetadata()
                                               .getThresholds()
                                               .equals( nextThreshold ) );

            // Create the time series with a label
            String seriesName = ChartDataFactory.getNameForPoolingWindowSeries( leadDurations,
                                                                                poolingTimes,
                                                                                nextThreshold,
                                                                                durationUnits );

            ToDoubleFunction<DoubleScoreComponentOuter> mapper = score ->
            {
                Instant midpoint =
                        ChartDataFactory.getMidpointBetweenTimes( score.getPoolMetadata()
                                                                       .getTimeWindow(),
                                                                  graphicShape == GraphicShape.ISSUED_DATE_POOLS );
                return midpoint.toEpochMilli();
            };

            YIntervalSeries series = ChartDataFactory.getIntervalSeries( finalSlice, seriesName, mapper );
            collection.addSeries( series );
        }
    }

    /**
     * Returns a series name from the inputs.
     *
     * @param leadDurations the lead durations by which to filter series
     * @param poolingTimes the  times by which to filter series
     * @param thresholds the thresholds
     * @param durationUnits the duration units
     * @return a series name
     */
    private static String getNameForPoolingWindowSeries( Pair<Duration, Duration> leadDurations,
                                                         Pair<Instant, Instant> poolingTimes,
                                                         OneOrTwoThresholds thresholds,
                                                         ChronoUnit durationUnits )
    {
        String key = "";

        Duration earliest = leadDurations.getLeft();
        Duration latest = leadDurations.getRight();
        Instant earliestTime = poolingTimes.getLeft();
        Instant latestTime = poolingTimes.getRight();

        // Lead durations
        if ( !earliest.equals( TimeWindowOuter.DURATION_MIN ) || !latest.equals( TimeWindowOuter.DURATION_MAX ) )
        {
            // Zero-width interval
            if ( earliest.equals( latest ) )
            {
                Number duration = DataUtilities.durationToNumericUnits( latest, durationUnits );
                key = key + duration + ", ";
            }
            else
            {
                key = key + "("
                      + DataUtilities.durationToNumericUnits( earliest, durationUnits )
                      + ","
                      + DataUtilities.durationToNumericUnits( latest, durationUnits )
                      + "], ";
            }
        }

        // Times
        if ( !earliestTime.equals( Instant.MIN ) || !latestTime.equals( Instant.MAX ) )
        {
            // Zero-width interval
            if ( earliestTime.equals( latestTime ) )
            {
                key = key + latestTime.toString().replace( "Z", "" ) + ","; // Zone in legend title
            }
            else
            {
                key = key + "("
                      + earliestTime.toString().replace( "Z", "" )
                      + ","
                      + latestTime.toString().replace( "Z", "" )
                      + "], "; // Zone in legend title
            }
        }

        return key + DataUtilities.toStringWithoutUnits( thresholds );
    }

    /**
     * Returns the midpoint between the reference times or valid times.
     *
     * @param timeWindow the time window
     * @param referenceTimes is true to return the midpoint between the reference times, false for valid times
     * @return the midpoint between the times
     */

    private static Instant getMidpointBetweenTimes( TimeWindowOuter timeWindow, boolean referenceTimes )
    {
        if ( referenceTimes )
        {
            return TimeSeriesSlicer.getMidPointBetweenTimes( timeWindow.getEarliestReferenceTime(),
                                                             timeWindow.getLatestReferenceTime() );
        }
        else
        {
            return TimeSeriesSlicer.getMidPointBetweenTimes( timeWindow.getEarliestValidTime(),
                                                             timeWindow.getLatestValidTime() );
        }
    }

    /**
     * Creates an {@link YIntervalSeries} from the inputs.
     * @param scores the scores
     * @param name the series name
     * @param domainValue a function that produces the domain value associated with a score
     * @return the series
     */

    private static YIntervalSeries getIntervalSeries( List<DoubleScoreComponentOuter> scores,
                                                      String name,
                                                      ToDoubleFunction<DoubleScoreComponentOuter> domainValue )
    {
        ToDoubleFunction<DoubleScoreComponentOuter> toDouble = score ->
        {
            if ( !score.hasQuantile() )
            {
                return Double.NaN;
            }
            return score.getSampleQuantile();
        };

        Map<Double, List<DoubleScoreComponentOuter>> mappedByQuantile
                = scores.stream()
                        .collect( Collectors.groupingBy( toDouble::applyAsDouble ) );

        List<DoubleScoreComponentOuter> nominal = mappedByQuantile.remove( Double.NaN );

        YIntervalSeries series = new YIntervalSeries( name );

        double min = mappedByQuantile.keySet()
                                     .stream()
                                     .mapToDouble( Double::doubleValue )
                                     .min()
                                     .orElse( Double.NaN );
        double max = mappedByQuantile.keySet()
                                     .stream()
                                     .mapToDouble( Double::doubleValue )
                                     .max()
                                     .orElse( Double.NaN );

        List<DoubleScoreComponentOuter> lower = nominal;
        List<DoubleScoreComponentOuter> upper = nominal;

        if ( max > min )
        {
            lower = mappedByQuantile.get( min );
            upper = mappedByQuantile.get( max );
        }

        for ( int i = 0; i < nominal.size(); i++ )
        {
            DoubleScoreComponentOuter nextNominal = nominal.get( i );
            DoubleScoreComponentOuter nextLower = lower.get( i );
            DoubleScoreComponentOuter nextUpper = upper.get( i );

            double domainAxisValue = domainValue.applyAsDouble( nextNominal );
            double nominalValue = nextNominal.getStatistic()
                                             .getValue();
            double lowerValue = nextLower.getStatistic()
                                         .getValue();
            double upperValue = nextUpper.getStatistic()
                                         .getValue();

            if ( Double.isInfinite( nominalValue ) )
            {
                nominalValue = Double.NaN;
            }

            if ( Double.isInfinite( lowerValue ) )
            {
                lowerValue = Double.NaN;
            }

            if ( Double.isInfinite( upperValue ) )
            {
                upperValue = Double.NaN;
            }

            series.add( domainAxisValue, nominalValue, lowerValue, upperValue );
        }

        return series;
    }

    /**
     * Creates an {@link XYIntervalSeries} from the inputs.
     * @param diagram the diagram
     * @param name the series name
     * @param xDimension the domain axis dimension
     * @param yDimension the range axis dimension
     * @return the series
     */

    private static XYIntervalSeries getIntervalSeries( List<DiagramStatisticOuter> diagram,
                                                       String name,
                                                       MetricDimension xDimension,
                                                       MetricDimension yDimension )
    {
        ToDoubleFunction<DiagramStatisticOuter> toDouble = statistic ->
        {
            if ( !statistic.hasQuantile() )
            {
                return Double.NaN;
            }
            return statistic.getSampleQuantile();
        };

        Map<Double, List<DiagramStatisticOuter>> mappedByQuantile
                = diagram.stream()
                         .collect( Collectors.groupingBy( toDouble::applyAsDouble ) );

        List<DiagramStatisticOuter> nominal = mappedByQuantile.remove( Double.NaN );

        XYIntervalSeries series = new XYIntervalSeries( name );

        double min = mappedByQuantile.keySet()
                                     .stream()
                                     .mapToDouble( Double::doubleValue )
                                     .min()
                                     .orElse( Double.NaN );
        double max = mappedByQuantile.keySet()
                                     .stream()
                                     .mapToDouble( Double::doubleValue )
                                     .max()
                                     .orElse( Double.NaN );

        List<DiagramStatisticOuter> lower = nominal;
        List<DiagramStatisticOuter> upper = nominal;

        // Quantiles available and this is not a sample size
        if ( max > min && yDimension != MetricDimension.SAMPLE_SIZE )
        {
            lower = mappedByQuantile.get( min );
            upper = mappedByQuantile.get( max );
        }

        // Iterate the series
        for ( int i = 0; i < nominal.size(); i++ )
        {
            DiagramStatisticOuter nextNominal = nominal.get( i );
            DiagramStatisticOuter nextLower = lower.get( i );
            DiagramStatisticOuter nextUpper = upper.get( i );

            String nameQualifier = nextNominal.getComponentNameQualifiers()
                                              .first();

            DiagramStatisticComponent xNominal = nextNominal.getComponent( xDimension, nameQualifier );
            DiagramStatisticComponent xLower = nextLower.getComponent( xDimension, nameQualifier );
            DiagramStatisticComponent xUpper = nextUpper.getComponent( xDimension, nameQualifier );
            DiagramStatisticComponent yNominal = nextNominal.getComponent( yDimension, nameQualifier );
            DiagramStatisticComponent yLower = nextLower.getComponent( yDimension, nameQualifier );
            DiagramStatisticComponent yUpper = nextUpper.getComponent( yDimension, nameQualifier );

            // Add the series data
            int valueCount = xNominal.getValuesCount();
            for ( int j = 0; j < valueCount; j++ )
            {
                series.add( xNominal.getValues( j ),
                            xLower.getValues( j ),
                            xUpper.getValues( j ),
                            yNominal.getValues( j ),
                            yLower.getValues( j ),
                            yUpper.getValues( j ) );
            }
        }

        return series;
    }

    /**
     * Prevent construction.
     */

    private ChartDataFactory()
    {
    }

}
