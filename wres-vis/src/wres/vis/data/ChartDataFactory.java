package wres.vis.data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
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

import static wres.vis.charts.GraphicsUtils.BASELINE_SCENARIO_LABEL;
import static wres.vis.charts.GraphicsUtils.PAIR_THEME_SEPARATOR;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.SummaryStatistic;
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

    /** Re-used string. */
    private static final String TIME_WINDOWS_AND = " time windows and ";

    /** Re-used string. */
    private static final String THRESHOLDS = " thresholds.";

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

            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DoubleScoreComponentOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( sliced );

            for ( Map.Entry<DatasetOrientation, List<DoubleScoreComponentOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DoubleScoreComponentOuter> nextScenario = nextEntry.getValue();
                String name = DataUtilities.toStringWithoutUnits( key );

                if ( sorted.size() > 1
                     && nextOrientation == DatasetOrientation.BASELINE )
                {
                    name += BASELINE_SCENARIO_LABEL;
                }

                YIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( nextScenario, name, domainValue );
                dataset.addSeries( nextSeries );
            }
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

            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DoubleScoreComponentOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( sliced );

            for ( Map.Entry<DatasetOrientation, List<DoubleScoreComponentOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DoubleScoreComponentOuter> nextScenario = nextEntry.getValue();
                String name = ChartDataFactory.getDurationLabelForThresholdLeadLegend( key.getLatestLeadDuration(),
                                                                                       durationUnits );

                if ( sorted.size() > 1
                     && nextOrientation == DatasetOrientation.BASELINE )
                {
                    name += BASELINE_SCENARIO_LABEL;
                }

                YIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( nextScenario, name, domainValue );
                dataset.addSeries( nextSeries );
            }
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

        Pair<Instant, Instant> unboundedTime = Pair.of( Instant.MIN, Instant.MAX );

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
            times.add( unboundedTime );
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
                // Slice by issued time if the series should contain valid times
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

                // Only qualify the label in the legend name if there are multiple unique times
                Pair<Instant, Instant> lableTime = ChartDataFactory.getTimeLabelForPoolingWindowsLegend( nextTime,
                                                                                                         times,
                                                                                                         unboundedTime );

                // Add the next set of series
                ChartDataFactory.addSeriesForPoolingWindow( returnMe,
                                                            slice,
                                                            nextDuration,
                                                            lableTime,
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

        // Slice by main/baseline and sort with baseline last
        Map<DatasetOrientation, List<DurationScoreStatisticOuter>> sorted =
                ChartDataFactory.sliceByDatasetOrientation( statistics );

        for ( Map.Entry<DatasetOrientation, List<DurationScoreStatisticOuter>> nextEntry : sorted.entrySet() )
        {
            DatasetOrientation orientation = nextEntry.getKey();
            List<DurationScoreStatisticOuter> scores = nextEntry.getValue();

            // Add the data items
            for ( DurationScoreStatisticOuter entry : scores )
            {
                String rowKey = DataUtilities.toStringWithoutUnits( entry.getPoolMetadata()
                                                                         .getThresholds() );

                if ( sorted.size() > 1
                     && orientation == DatasetOrientation.BASELINE )
                {
                    rowKey = rowKey + " baseline";
                }

                for ( MetricConstants metric : entry.getComponents() )
                {
                    com.google.protobuf.Duration score = entry.getComponent( metric )
                                                              .getStatistic()
                                                              .getValue();
                    Duration durationStat = MessageUtilities.getDuration( score );

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
                Slicer.discover( statistics, next -> next.getPoolMetadata()
                                                         .getThresholds() );

        // Filter by threshold
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            // JFreeChart does not currently support an interval series, so omit any resampled quantiles: GitHub 399
            List<DurationDiagramStatisticOuter> filtered =
                    Slicer.filter( statistics,
                                   data -> data.getPoolMetadata()
                                               .getThresholds()
                                               .equals( nextThreshold )
                                           && ( !data.isSummaryStatistic()
                                                || !data.getSummaryStatistic()
                                                        .getDimensionList()
                                                        .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) ) );

            // Group the series by dataset
            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DurationDiagramStatisticOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( filtered );

            for ( Map.Entry<DatasetOrientation, List<DurationDiagramStatisticOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DurationDiagramStatisticOuter> nextScenario = nextEntry.getValue();
                String seriesName = DataUtilities.toStringWithoutUnits( nextThreshold );

                if ( sorted.size() > 1
                     && nextOrientation == DatasetOrientation.BASELINE )
                {
                    seriesName += BASELINE_SCENARIO_LABEL;
                }

                TimeSeries nextSeries = ChartDataFactory.getDurationDiagramSeries( seriesName, nextScenario );
                returnMe.addSeries( nextSeries );
            }
        }

        return returnMe;
    }

    /**
     * Returns a dataset for a verification diagram organized by lead duration (one) and then threshold (up to many).
     * @param statistics the statistics
     * @param domainDimension the metric dimension for the X axis
     * @param rangeDimension the metric dimension for the Y axis
     * @param durationUnits the duration units
     * @return a data source that can be used to draw a plot
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dataset contains more than one lead duration
     */
    public static XYDataset ofDiagramStatisticsByLeadAndThreshold( List<DiagramStatisticOuter> statistics,
                                                                   MetricDimension domainDimension,
                                                                   MetricDimension rangeDimension,
                                                                   ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( domainDimension );
        Objects.requireNonNull( rangeDimension );
        Objects.requireNonNull( durationUnits );

        DiagramStatisticOuter first = statistics.get( 0 );
        MetricConstants metricName = first.getMetricName();

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata()
                                                                       .getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata()
                                                                      .getThresholds() )
                                   .size();

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a diagram chart without statistics." );
        }

        if ( timeWindowCount > 1 )
        {
            throw new IllegalArgumentException( "Received an unexpected collection of statistics for the " + metricName
                                                + ". Expected a single time window and one or more thresholds, but "
                                                + "found "
                                                + timeWindowCount
                                                + TIME_WINDOWS_AND
                                                + thresholdCount
                                                + THRESHOLDS );
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

            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DiagramStatisticOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( sliced );

            SortedSet<String> qualified = sliced.stream()
                                                .flatMap( n -> n.getStatistic()
                                                                .getStatisticsList()
                                                                .stream() )
                                                .map( DiagramStatisticComponent::getName )
                                                .filter( n -> !n.isBlank() )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

            String baseName = DataUtilities.toStringWithoutUnits( key );

            for ( Map.Entry<DatasetOrientation, List<DiagramStatisticOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DiagramStatisticOuter> nextScenario = nextEntry.getValue();

                ChartDataFactory.addDiagramSeries( nextOrientation,
                                                   nextScenario,
                                                   qualified,
                                                   sorted.size(),
                                                   baseName,
                                                   dataset,
                                                   Pair.of( domainDimension, rangeDimension ) );
            }
        }

        return dataset;
    }

    /**
     * Returns a dataset for a verification diagram organized by threshold (one) and then lead duration (up to many).
     * @param statistics the statistics
     * @param domainDimension the metric dimension for the X axis
     * @param rangeDimension the metric dimension for the Y axis
     * @param durationUnits the duration units
     * @return a data source that can be used to draw a plot
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */
    public static XYDataset ofDiagramStatisticsByThresholdAndLead( List<DiagramStatisticOuter> statistics,
                                                                   MetricDimension domainDimension,
                                                                   MetricDimension rangeDimension,
                                                                   ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( domainDimension );
        Objects.requireNonNull( rangeDimension );
        Objects.requireNonNull( durationUnits );

        DiagramStatisticOuter first = statistics.get( 0 );
        MetricConstants metricName = first.getMetricName();

        int timeWindowCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata()
                                                                       .getTimeWindow() )
                                    .size();

        int thresholdCount = Slicer.discover( statistics, meta -> meta.getPoolMetadata()
                                                                      .getThresholds() )
                                   .size();

        if ( statistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a diagram chart without statistics." );
        }

        if ( thresholdCount > 1 )
        {
            throw new IllegalArgumentException( "Received an unexpected collection of statistics for the " + metricName
                                                + ". Expected a single threshold and one or more time windows, but "
                                                + "found "
                                                + timeWindowCount
                                                + TIME_WINDOWS_AND
                                                + thresholdCount
                                                + THRESHOLDS );
        }

        // Arrange by time window
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( statistics, next -> next.getPoolMetadata()
                                                         .getTimeWindow() );

        XYIntervalSeriesCollection dataset = new XYIntervalSeriesCollection();

        for ( TimeWindowOuter key : timeWindows )
        {
            List<DiagramStatisticOuter> sliced = Slicer.filter( statistics,
                                                                next -> next.getPoolMetadata()
                                                                            .getTimeWindow()
                                                                            .equals( key ) );

            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DiagramStatisticOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( sliced );

            SortedSet<String> qualified = sliced.stream()
                                                .flatMap( n -> n.getStatistic()
                                                                .getStatisticsList()
                                                                .stream() )
                                                .map( DiagramStatisticComponent::getName )
                                                .filter( n -> !n.isBlank() )
                                                .collect( Collectors.toCollection( TreeSet::new ) );

            Number leadDuration = DataUtilities.durationToNumericUnits( key.getLatestLeadDuration(),
                                                                        durationUnits );
            String baseName = leadDuration.toString();

            for ( Map.Entry<DatasetOrientation, List<DiagramStatisticOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DiagramStatisticOuter> nextScenario = nextEntry.getValue();

                ChartDataFactory.addDiagramSeries( nextOrientation,
                                                   nextScenario,
                                                   qualified,
                                                   sorted.size(),
                                                   baseName,
                                                   dataset,
                                                   Pair.of( domainDimension, rangeDimension ) );
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
     * Returns a dataset for pairs statistics.
     *
     * @param statistics the statistics
     * @return a data source that can be used to draw a plot
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */
    public static XYDataset ofPairsStatistics( PairsStatisticOuter statistics )
    {
        Objects.requireNonNull( statistics );

        List<String> leftNames = statistics.getStatistic()
                                           .getStatistics()
                                           .getLeftVariableNamesList();

        List<String> rightNames = statistics.getStatistic()
                                            .getStatistics()
                                            .getRightVariableNamesList();

        int seriesNumber = 1;
        List<TimeSeries> allSeries = new ArrayList<>();
        for ( Pairs.TimeSeriesOfPairs nextSeries : statistics.getStatistic()
                                                             .getStatistics()
                                                             .getTimeSeriesList() )
        {
            // Add a placeholder series for each variable
            List<TimeSeries> leftSeries = new ArrayList<>();
            List<TimeSeries> rightSeries = new ArrayList<>();

            // Create a placeholder time-series for each variable
            for ( String nextLeftName : leftNames )
            {
                TimeSeries next = new TimeSeries( nextLeftName + PAIR_THEME_SEPARATOR + seriesNumber );
                leftSeries.add( next );
            }
            for ( String nextRightName : rightNames )
            {
                TimeSeries next = new TimeSeries( nextRightName + PAIR_THEME_SEPARATOR + seriesNumber );
                rightSeries.add( next );
            }

            for ( Pairs.Pair pair : nextSeries.getPairsList() )
            {
                Timestamp validTime = pair.getValidTime();

                // Millisecond precision
                Instant instant = MessageUtilities.getInstant( validTime );
                long millis = instant.toEpochMilli();
                FixedMillisecond time = new FixedMillisecond( millis );

                // Add the value for each left series
                int leftCount = pair.getLeftCount();
                for ( int i = 0; i < leftCount; i++ )
                {
                    double nextValue = pair.getLeft( i );
                    leftSeries.get( i )
                              .add( time, nextValue );
                }

                // Add the values for each right series
                for ( int i = 0; i < pair.getRightCount(); i++ )
                {
                    double nextValue = pair.getRight( i );
                    rightSeries.get( i )
                               .add( time, nextValue );
                }
            }

            allSeries.addAll( leftSeries );
            allSeries.addAll( rightSeries );
            seriesNumber++;
        }

        return PerformantTimeSeriesCollection.of( Collections.unmodifiableList( allSeries ) );
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
            inputSlice = Slicer.filter( statistics,
                                        next -> next.getPoolMetadata()
                                                    .getTimeWindow()
                                                    .equals( keyInstance ) );
        }
        else if ( chartType == ChartType.THRESHOLD_LEAD )
        {
            inputSlice = Slicer.filter( statistics,
                                        next -> next.getPoolMetadata()
                                                    .getThresholds()
                                                    .equals( keyInstance ) );
        }
        else if ( chartType == ChartType.POOLING_WINDOW )
        {
            inputSlice = Slicer.filter( statistics,
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
                                                .filter( s -> s.isSummaryStatistic()
                                                              && s.getSummaryStatistic()
                                                                  .getStatistic()
                                                                 == SummaryStatistic.StatisticName.QUANTILE )
                                                .mapToDouble( n -> n.getSummaryStatistic()
                                                                    .getProbability() )
                                                .boxed()
                                                .collect( Collectors.toCollection( TreeSet::new ) );

        return Collections.unmodifiableSortedSet( quantiles );
    }

    /**
     * Slices the statistics by {@link DatasetOrientation} and places them in a sorted map with the baseline ordered
     * last, if available.
     * @param statistics the statistics
     * @return the sliced statistics
     * @param <T> the type of statistic
     */

    private static <T extends Statistic<?>> Map<DatasetOrientation, List<T>> sliceByDatasetOrientation( List<T> statistics )
    {
        Map<DatasetOrientation, List<T>> groups = Slicer.getGroupedStatistics( statistics );
        Comparator<DatasetOrientation> baselineDown = ( a, b ) ->
        {
            if ( a == DatasetOrientation.RIGHT
                 && b == DatasetOrientation.BASELINE )
            {
                return -1;
            }
            else if ( a == DatasetOrientation.BASELINE
                      && b == DatasetOrientation.RIGHT )
            {
                return 1;
            }
            else
            {
                return 0;
            }
        };
        Map<DatasetOrientation, List<T>> sorted = new TreeMap<>( baselineDown );
        sorted.putAll( groups );

        return Collections.unmodifiableMap( sorted );
    }

    /**
     * Returns a named duration diagram series.
     * @param name the series name
     * @param data the series data
     * @return the series
     */

    private static TimeSeries getDurationDiagramSeries( String name,
                                                        List<DurationDiagramStatisticOuter> data )
    {
        TimeSeries next = new TimeSeries( name );

        // Create a set-view by instant, because JFreeChart cannot handle duplicates
        Set<Instant> instants = new HashSet<>();

        // Create the series
        for ( DurationDiagramStatisticOuter nextSet : data )
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
        return next;
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

            DiagramStatistic.Builder builder = diagram.getStatistic()
                                                      .toBuilder();
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
                        DiagramStatisticOuter.of( newDiagram,
                                                  diagram.getPoolMetadata(),
                                                  diagram.getSummaryStatistic() );
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
     * @param poolingTimes the times by which to name series
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
                Slicer.discover( slice, next -> next.getPoolMetadata()
                                                    .getThresholds() );


        ToDoubleFunction<DoubleScoreComponentOuter> mapper = score ->
        {
            Instant midpoint =
                    ChartDataFactory.getMidpointBetweenTimes( score.getPoolMetadata()
                                                                   .getTimeWindow(),
                                                              graphicShape == GraphicShape.ISSUED_DATE_POOLS );
            return midpoint.toEpochMilli();
        };

        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            // Slice the data by threshold.  The resulting data will still contain potentially
            // multiple issued time pooling windows and/or valid time pooling windows.
            List<DoubleScoreComponentOuter> thresholdSlice =
                    Slicer.filter( slice,
                                   next -> next.getPoolMetadata()
                                               .getThresholds()
                                               .equals( nextThreshold ) );


            // Slice by main/baseline and sort with baseline last
            Map<DatasetOrientation, List<DoubleScoreComponentOuter>> sorted =
                    ChartDataFactory.sliceByDatasetOrientation( thresholdSlice );

            for ( Map.Entry<DatasetOrientation, List<DoubleScoreComponentOuter>> nextEntry : sorted.entrySet() )
            {
                DatasetOrientation nextOrientation = nextEntry.getKey();
                List<DoubleScoreComponentOuter> nextScenario = nextEntry.getValue();

                // Create the time series with a label
                String seriesName = ChartDataFactory.getNameForPoolingWindowSeries( leadDurations,
                                                                                    poolingTimes,
                                                                                    nextThreshold,
                                                                                    durationUnits );

                if ( sorted.size() > 1
                     && nextOrientation == DatasetOrientation.BASELINE )
                {
                    seriesName += BASELINE_SCENARIO_LABEL;
                }

                YIntervalSeries series = ChartDataFactory.getIntervalSeries( nextScenario, seriesName, mapper );
                collection.addSeries( series );
            }
        }
    }

    /**
     * Returns a dataset for a verification diagram organized by lead duration (one) and then threshold (up to many).
     * @param orientation the dataset orientation
     * @param statistics the statistics
     * @param qualifiers the qualifiers
     * @param scenarioCount the number of datasets or scenarios
     * @param baseName the base name of the dataset
     * @param dataset the plotting dataset to which series will be added
     * @param dimensions the plot dimensions
     */
    private static void addDiagramSeries( DatasetOrientation orientation,
                                          List<DiagramStatisticOuter> statistics,
                                          SortedSet<String> qualifiers,
                                          int scenarioCount,
                                          String baseName,
                                          XYIntervalSeriesCollection dataset,
                                          Pair<MetricDimension, MetricDimension> dimensions )
    {
        String orientationQualifier = "";

        // Scatter plots are a special snowflake, no thresholds and fixed naming of the (up to two) datasets
        if ( statistics.stream()
                       .allMatch( s -> s.getMetricName() == MetricConstants.SCATTER_PLOT ) )
        {
            baseName = "";
            if ( orientation == DatasetOrientation.RIGHT )
            {
                orientationQualifier = "Predicted";
            }
            else
            {
                orientationQualifier = "Baseline";
            }
        }
        else if ( scenarioCount > 1
                  && orientation == DatasetOrientation.BASELINE )
        {
            orientationQualifier = BASELINE_SCENARIO_LABEL;
        }

        // No qualifiers
        if ( qualifiers.isEmpty() )
        {
            String name = baseName + orientationQualifier;
            XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( statistics,
                                                                              name,
                                                                              dimensions.getLeft(),
                                                                              dimensions.getRight() );
            dataset.addSeries( nextSeries );
        }
        // Slice by qualifier
        else
        {
            for ( String qualifier : qualifiers )
            {
                List<DiagramStatisticOuter> slicedInner = Slicer.filter( statistics,
                                                                         next -> next.getComponentNameQualifiers()
                                                                                     .stream()
                                                                                     .anyMatch( qualifier::equals ) );
                String name = qualifier + orientationQualifier;
                XYIntervalSeries nextSeries = ChartDataFactory.getIntervalSeries( slicedInner,
                                                                                  name,
                                                                                  dimensions.getLeft(),
                                                                                  dimensions.getRight() );
                dataset.addSeries( nextSeries );
            }
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
        if ( !earliest.equals( TimeWindowOuter.DURATION_MIN )
             || !latest.equals( TimeWindowOuter.DURATION_MAX ) )
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
        if ( !earliestTime.equals( Instant.MIN )
             || !latestTime.equals( Instant.MAX ) )
        {
            // Zero-width interval
            if ( earliestTime.equals( latestTime ) )
            {
                key = key + latestTime.toString()
                                      .replace( "Z", "" ) + ","; // Zone in legend title
            }
            else
            {
                key = key + "("
                      + earliestTime.toString()
                                    .replace( "Z", "" )
                      + ","
                      + latestTime.toString()
                                  .replace( "Z", "" )
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
        Objects.requireNonNull( scores );
        Objects.requireNonNull( name );
        Objects.requireNonNull( domainValue );

        ToDoubleFunction<DoubleScoreComponentOuter> toDouble = score ->
        {
            if ( !( score.isSummaryStatistic()
                    && score.getSummaryStatistic()
                            .getStatistic() == SummaryStatistic.StatisticName.QUANTILE ) )
            {
                return Double.NaN;
            }
            return score.getSummaryStatistic()
                        .getProbability();
        };

        Map<Double, List<DoubleScoreComponentOuter>> mappedByQuantile
                = scores.stream()
                        .collect( Collectors.groupingBy( toDouble::applyAsDouble ) );

        List<DoubleScoreComponentOuter> nominal = mappedByQuantile.remove( Double.NaN );

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

        // In some cases, duplicates may be present. For example, when including two instances of the same metric with
        // different threshold parameters for each metric, the "all data" statistic may be present for each instance
        BinaryOperator<DoubleScoreComponentOuter> aggregator = ( a, b ) ->
        {
            LOGGER.debug( "Encountered duplicate scores to filter with the following metadata: {}.", a );
            return a;
        };

        // Map the results by time window and threshold to correlate nominal/central and quantile values
        Map<PoolMetadata, DoubleScoreComponentOuter> centralMapped = Map.of();
        Map<PoolMetadata, DoubleScoreComponentOuter> lowerMapped = Map.of();
        Map<PoolMetadata, DoubleScoreComponentOuter> upperMapped = Map.of();

        // Get a central value, if available. Use the nominal value first, else the median
        if ( Objects.nonNull( nominal ) )
        {
            centralMapped = nominal.stream()
                                   .collect( Collectors.toMap( DoubleScoreComponentOuter::getPoolMetadata,
                                                               Function.identity(),
                                                               aggregator ) );
        }
        // Median
        else if ( mappedByQuantile.containsKey( 0.5 ) )
        {
            centralMapped = mappedByQuantile.get( 0.5 )
                                            .stream()
                                            .collect( Collectors.toMap( DoubleScoreComponentOuter::getPoolMetadata,
                                                                        Function.identity(),
                                                                        aggregator ) );
        }

        if ( Objects.nonNull( lower ) )
        {
            lowerMapped = lower.stream()
                               .collect( Collectors.toMap( DoubleScoreComponentOuter::getPoolMetadata,
                                                           Function.identity(),
                                                           aggregator ) );
        }

        if ( Objects.nonNull( upper ) )
        {
            upperMapped = upper.stream()
                               .collect( Collectors.toMap( DoubleScoreComponentOuter::getPoolMetadata,
                                                           Function.identity(),
                                                           aggregator ) );
        }

        return ChartDataFactory.getIntervalSeries( centralMapped, lowerMapped, upperMapped, name, domainValue );
    }

    /**
     * Creates an {@link YIntervalSeries} from the inputs.
     * @param centralMapped the map of central values
     * @param lowerMapped the map of lower interval values
     * @param upperMapped the map of upper interval values
     * @param name the series name
     * @param domainValue a function that produces the domain value associated with a score
     * @return the series
     * @throws NullPointerException if ay input is null
     */

    private static YIntervalSeries getIntervalSeries( Map<PoolMetadata, DoubleScoreComponentOuter> centralMapped,
                                                      Map<PoolMetadata, DoubleScoreComponentOuter> lowerMapped,
                                                      Map<PoolMetadata, DoubleScoreComponentOuter> upperMapped,
                                                      String name,
                                                      ToDoubleFunction<DoubleScoreComponentOuter> domainValue )
    {
        Objects.requireNonNull( centralMapped );
        Objects.requireNonNull( lowerMapped );
        Objects.requireNonNull( upperMapped );
        Objects.requireNonNull( name );
        Objects.requireNonNull( domainValue );

        YIntervalSeries series = new YIntervalSeries( name );

        Set<PoolMetadata> metadatas = centralMapped.keySet();

        if ( metadatas.isEmpty() )
        {
            metadatas = lowerMapped.keySet();
        }

        if ( metadatas.isEmpty() )
        {
            metadatas = upperMapped.keySet();
        }

        for ( PoolMetadata nextMeta : metadatas )
        {
            double nominalValue = Double.NaN;
            double lowerValue = Double.NaN;
            double upperValue = Double.NaN;
            double domainAxisValue = Double.NaN;

            if ( centralMapped.containsKey( nextMeta ) )
            {
                DoubleScoreComponentOuter nextNominal = centralMapped.get( nextMeta );
                nominalValue = nextNominal.getStatistic()
                                          .getValue();
                lowerValue = nominalValue;
                upperValue = nominalValue;
                domainAxisValue = domainValue.applyAsDouble( nextNominal );
            }

            if ( lowerMapped.containsKey( nextMeta ) )
            {
                DoubleScoreComponentOuter nextLower = lowerMapped.get( nextMeta );
                lowerValue = nextLower.getStatistic()
                                      .getValue();
                domainAxisValue = domainValue.applyAsDouble( nextLower );
            }

            if ( upperMapped.containsKey( nextMeta ) )
            {
                DoubleScoreComponentOuter nextUpper = upperMapped.get( nextMeta );
                upperValue = nextUpper.getStatistic()
                                      .getValue();
                domainAxisValue = domainValue.applyAsDouble( nextUpper );
            }

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
        ToDoubleFunction<DiagramStatisticOuter> toDouble = score ->
        {
            if ( !( score.isSummaryStatistic()
                    && score.getSummaryStatistic()
                            .getStatistic() == SummaryStatistic.StatisticName.QUANTILE ) )
            {
                return Double.NaN;
            }
            return score.getSummaryStatistic()
                        .getProbability();
        };

        Map<Double, List<DiagramStatisticOuter>> mappedByQuantile
                = diagram.stream()
                         .collect( Collectors.groupingBy( toDouble::applyAsDouble ) );

        List<DiagramStatisticOuter> nominal = mappedByQuantile.remove( Double.NaN );

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
        if ( max > min )
        {
            lower = mappedByQuantile.get( min );
            upper = mappedByQuantile.get( max );
        }

        // In some cases, duplicates may be present. For example, when including two instances of the same metric with
        // different threshold parameters for each metric, the "all data" statistic may be present for each instance
        BinaryOperator<DiagramStatisticOuter> aggregator = ( a, b ) ->
        {
            LOGGER.debug( "Encountered duplicate scores to filter with the following metadata: {}.", a );
            return a;
        };

        // Map the results by time window and threshold to correlate nominal/central and quantile values
        Map<PoolMetadata, DiagramStatisticOuter> centralMapped = Map.of();
        Map<PoolMetadata, DiagramStatisticOuter> lowerMapped = Map.of();
        Map<PoolMetadata, DiagramStatisticOuter> upperMapped = Map.of();

        // Get a central value, if available. Use the nominal value first, else the median
        if ( Objects.nonNull( nominal ) )
        {
            centralMapped = nominal.stream()
                                   .collect( Collectors.toMap( DiagramStatisticOuter::getPoolMetadata,
                                                               Function.identity(),
                                                               aggregator ) );
        }
        // Median
        else if ( mappedByQuantile.containsKey( 0.5 ) )
        {
            centralMapped = mappedByQuantile.get( 0.5 )
                                            .stream()
                                            .collect( Collectors.toMap( DiagramStatisticOuter::getPoolMetadata,
                                                                        Function.identity(),
                                                                        aggregator ) );
        }

        if ( Objects.nonNull( lower ) )
        {
            lowerMapped = lower.stream()
                               .collect( Collectors.toMap( DiagramStatisticOuter::getPoolMetadata,
                                                           Function.identity(),
                                                           aggregator ) );
        }

        if ( Objects.nonNull( upper ) )
        {
            upperMapped = upper.stream()
                               .collect( Collectors.toMap( DiagramStatisticOuter::getPoolMetadata,
                                                           Function.identity(),
                                                           aggregator ) );
        }

        return ChartDataFactory.getIntervalSeries( centralMapped,
                                                   lowerMapped,
                                                   upperMapped,
                                                   name,
                                                   xDimension,
                                                   yDimension );
    }

    /**
     * Creates an {@link XYIntervalSeries} from the inputs.
     * @param centralMapped the map of central values
     * @param lowerMapped the map of lower interval values
     * @param upperMapped the map of upper interval values
     * @param name the series name
     * @param xDimension the domain axis dimension
     * @param yDimension the range axis dimension
     * @return the series
     * @throws NullPointerException if ay input is null
     */

    private static XYIntervalSeries getIntervalSeries( Map<PoolMetadata, DiagramStatisticOuter> centralMapped,
                                                       Map<PoolMetadata, DiagramStatisticOuter> lowerMapped,
                                                       Map<PoolMetadata, DiagramStatisticOuter> upperMapped,
                                                       String name,
                                                       MetricDimension xDimension,
                                                       MetricDimension yDimension )
    {
        Objects.requireNonNull( centralMapped );
        Objects.requireNonNull( lowerMapped );
        Objects.requireNonNull( upperMapped );
        Objects.requireNonNull( name );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );

        XYIntervalSeries series = new XYIntervalSeries( name );

        Set<PoolMetadata> metadatas = centralMapped.keySet();

        if ( metadatas.isEmpty() )
        {
            metadatas = lowerMapped.keySet();
        }

        if ( metadatas.isEmpty() )
        {
            metadatas = upperMapped.keySet();
        }

        // Iterate the series
        for ( PoolMetadata metadata : metadatas )
        {
            DiagramStatisticOuter nextCentral = centralMapped.get( metadata );
            DiagramStatisticOuter nextLower = lowerMapped.get( metadata );
            DiagramStatisticOuter nextUpper = upperMapped.get( metadata );

            ChartDataFactory.addDiagramDatasetToSeries( nextCentral,
                                                        nextLower,
                                                        nextUpper,
                                                        xDimension,
                                                        yDimension,
                                                        series );
        }

        return series;
    }

    /**
     * Adds a new diagram dataset to the supplied series.
     * @param central the central dataset
     * @param lower the lower bound dataset
     * @param upper the upper bound dataset
     * @param xDimension the domain axis dimension
     * @param yDimension the range axis dimension
     * @param series the series to adjust
     */
    private static void addDiagramDatasetToSeries( DiagramStatisticOuter central,
                                                   DiagramStatisticOuter lower,
                                                   DiagramStatisticOuter upper,
                                                   MetricDimension xDimension,
                                                   MetricDimension yDimension,
                                                   XYIntervalSeries series )
    {
        if ( Objects.isNull( central ) )
        {
            LOGGER.debug( "Skipping a diagram because it does not have a central dataset." );
            return;
        }

        String nameQualifier = central.getComponentNameQualifiers()
                                      .first();
        DiagramStatisticComponent xCentral = central.getComponent( xDimension, nameQualifier );
        DiagramStatisticComponent yCentral = central.getComponent( yDimension, nameQualifier );
        int valueCount = xCentral.getValuesCount();
        DiagramStatisticComponent xLower = null;
        DiagramStatisticComponent xUpper = null;
        DiagramStatisticComponent yLower = null;
        DiagramStatisticComponent yUpper = null;

        if ( Objects.nonNull( lower ) )
        {
            nameQualifier = lower.getComponentNameQualifiers()
                                 .first();
            xLower = lower.getComponent( xDimension, nameQualifier );
            yLower = lower.getComponent( yDimension, nameQualifier );
            valueCount = xLower.getValuesCount();
        }

        if ( Objects.nonNull( upper ) )
        {
            nameQualifier = upper.getComponentNameQualifiers()
                                 .first();
            xUpper = upper.getComponent( xDimension, nameQualifier );
            yUpper = upper.getComponent( yDimension, nameQualifier );
            valueCount = xUpper.getValuesCount();
        }

        // Add the series data
        for ( int j = 0; j < valueCount; j++ )
        {
            double xC = Double.NaN;
            double yC = Double.NaN;
            double xL = Double.NaN;
            double xU = Double.NaN;
            double yL = Double.NaN;
            double yU = Double.NaN;

            if ( j < xCentral.getValuesCount() )
            {
                xC = xCentral.getValues( j );
                yC = yCentral.getValues( j );
            }

            if ( Objects.nonNull( lower ) )
            {
                xL = xLower.getValues( j );
                yL = yLower.getValues( j );
            }

            if ( Objects.nonNull( upper ) )
            {
                xU = xUpper.getValues( j );
                yU = yUpper.getValues( j );
            }

            // Histogram
            if ( xDimension == MetricDimension.BIN_UPPER_BOUND
                 && valueCount > 1 )
            {
                double gap = xCentral.getValues( 1 ) - xCentral.getValues( 0 );
                xL = xC - ( 0.5 * gap );
                xU = xC + ( 0.5 * gap );
            }

            series.add( xC, xL, xU, yC, yL, yU );
        }
    }

    /**
     * Returns the time label for the legend name in pooling window datasets.
     * @param currentTime the current time
     * @param uniqueTimes the unique times
     * @param unboundedTime the unbounded time
     * @return the time label to use
     */

    private static Pair<Instant, Instant> getTimeLabelForPoolingWindowsLegend( Pair<Instant, Instant> currentTime,
                                                                               Set<Pair<Instant, Instant>> uniqueTimes,
                                                                               Pair<Instant, Instant> unboundedTime )
    {
        // Only qualify the label in the legend name if there are multiple unique times
        Pair<Instant, Instant> lableTime = currentTime;
        if ( uniqueTimes.size() == 1 )
        {
            lableTime = unboundedTime;
        }

        return lableTime;
    }

    /**
     * Returns a label for the supplied duration.
     * @param duration the duration
     * @param durationUnits the duration units
     * @return the label
     */

    private static String getDurationLabelForThresholdLeadLegend( Duration duration,
                                                                  ChronoUnit durationUnits )
    {
        if ( duration.equals( TimeWindowOuter.DURATION_MAX ) )
        {
            return "Unbounded";
        }

        Number leadDuration = DataUtilities.durationToNumericUnits( duration,
                                                                    durationUnits );
        return leadDuration.toString();
    }

    /**
     * Prevent construction.
     */

    private ChartDataFactory()
    {
    }

}
