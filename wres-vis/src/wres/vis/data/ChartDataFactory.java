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
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.OutputTypeSelection;
import wres.datamodel.DataFactory;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
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
        return ScoreByLeadAndThreshold.of( statistics, durationUnits );
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
        return ScoreByThresholdAndLead.of( statistics, durationUnits );
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
                                                                         next -> Pair.of( next.getMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getEarliestLeadDuration(),
                                                                                          next.getMetadata()
                                                                                              .getTimeWindow()
                                                                                              .getLatestLeadDuration() ) );

        // Build the TimeSeriesCollection
        TimeSeriesCollection returnMe = new TimeSeriesCollection();

        // Filter by times if each series should contain issued or valid pools, else do not filter
        SortedSet<Pair<Instant, Instant>> times = new TreeSet<>();

        // Series by issued time 
        if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS )
        {
            SortedSet<Pair<Instant, Instant>> uniqueValidTimes = Slicer.discover( statistics,
                                                                                  next -> Pair.of( next.getMetadata()
                                                                                                       .getTimeWindow()
                                                                                                       .getEarliestValidTime(),
                                                                                                   next.getMetadata()
                                                                                                       .getTimeWindow()
                                                                                                       .getLatestValidTime() ) );
            times.addAll( uniqueValidTimes );
        }
        else if ( graphicShape == GraphicShape.VALID_DATE_POOLS )
        {
            SortedSet<Pair<Instant, Instant>> uniqueReferenceTimes = Slicer.discover( statistics,
                                                                                      next -> Pair.of( next.getMetadata()
                                                                                                           .getTimeWindow()
                                                                                                           .getEarliestReferenceTime(),
                                                                                                       next.getMetadata()
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
                                                                       next -> next.getMetadata()
                                                                                   .getTimeWindow()
                                                                                   .getEarliestLeadDuration()
                                                                                   .equals( nextDuration.getLeft() )
                                                                               && next.getMetadata()
                                                                                      .getTimeWindow()
                                                                                      .getLatestLeadDuration()
                                                                                      .equals( nextDuration.getRight() ) );

                // Slice the data by valid time if the series should contain issued times
                if ( graphicShape == GraphicShape.ISSUED_DATE_POOLS )
                {
                    slice = Slicer.filter( slice,
                                           next -> next.getMetadata()
                                                       .getTimeWindow()
                                                       .getEarliestValidTime()
                                                       .equals( nextTime.getLeft() )
                                                   && next.getMetadata()
                                                          .getTimeWindow()
                                                          .getLatestValidTime()
                                                          .equals( nextTime.getRight() ) );
                }
                else if ( graphicShape == GraphicShape.VALID_DATE_POOLS )
                {
                    slice = Slicer.filter( slice,
                                           next -> next.getMetadata()
                                                       .getTimeWindow()
                                                       .getEarliestReferenceTime()
                                                       .equals( nextTime.getLeft() )
                                                   && next.getMetadata()
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
            String rowKey = entry.getMetadata()
                                 .getThresholds()
                                 .toStringWithoutUnits();

            for ( MetricConstants metric : entry.getComponents() )
            {
                com.google.protobuf.Duration score = entry.getComponent( metric )
                                                          .getData()
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
                Slicer.discover( statistics, next -> next.getMetadata().getThresholds() );

        // Filter by by threshold
        for ( OneOrTwoThresholds nextSeries : thresholds )
        {
            TimeSeries next =
                    new TimeSeries( nextSeries.toStringWithoutUnits() );

            List<DurationDiagramStatisticOuter> filtered =
                    Slicer.filter( statistics,
                                   data -> data.getMetadata().getThresholds().equals( nextSeries ) );

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
    public static XYDataset ofVerificationDiagramByLeadThreshold( List<DiagramStatisticOuter> statistics,
                                                                  MetricDimension xDimension,
                                                                  MetricDimension yDimension,
                                                                  ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );

        DiagramStatisticOuter first = statistics.get( 0 );

        if ( first.getMetricName() == MetricConstants.RANK_HISTOGRAM )
        {
            return ChartDataFactory.ofRankHistogramByLeadThreshold( statistics, xDimension, yDimension, durationUnits );
        }

        return Diagram.ofLeadThreshold( statistics, xDimension, yDimension, durationUnits );
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
    public static XYDataset ofVerificationDiagramByThresholdLead( List<DiagramStatisticOuter> statistics,
                                                                  MetricDimension xDimension,
                                                                  MetricDimension yDimension,
                                                                  ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( xDimension );
        Objects.requireNonNull( yDimension );
        Objects.requireNonNull( durationUnits );

        DiagramStatisticOuter first = statistics.get( 0 );

        if ( first.getMetricName() == MetricConstants.RANK_HISTOGRAM )
        {
            return ChartDataFactory.ofRankHistogramByThresholdLead( statistics, xDimension, yDimension, durationUnits );
        }

        return Diagram.ofThresholdLead( statistics, xDimension, yDimension, durationUnits );
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
                                   next -> next.getMetadata()
                                               .getTimeWindow()
                                               .equals( keyInstance ) );
        }
        else if ( chartType == ChartType.THRESHOLD_LEAD )
        {
            inputSlice =
                    Slicer.filter( statistics,
                                   next -> next.getMetadata()
                                               .getThresholds()
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

            DiagramStatistic.Builder builder = diagram.getData().toBuilder();
            for ( String qualifier : qualifiers )
            {
                List<DiagramStatisticComponent> components = new ArrayList<>();

                for ( MetricDimension name : names )
                {
                    DiagramStatisticComponent next = diagram.getComponent( name, qualifier );
                    components.add( next );
                }

                // Add the new diagram for this qualifier
                builder = builder.clearStatistics();
                builder.addAllStatistics( components );

                DiagramStatistic newDiagram = builder.build();
                DiagramStatisticOuter newWrappedDiagram = DiagramStatisticOuter.of( newDiagram, diagram.getMetadata() );
                returnMe.add( newWrappedDiagram );
            }
        }

        LOGGER.debug( "Decomposed {} diagrams into {} diagrams for plotting.", diagrams.size(), returnMe.size() );

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns a dataset for a rank histogram organized by lead duration (one) and then threshold (up to many).
     * @param statistics the rank histogram statistics
     * @param xDimension the x-axis dimension
     * @param yDimension the y-axis dimension
     * @param durationUnits the lead duration units
     * @return a rank histogram dataset
     * @throws IllegalArgumentException if the dataset contains more than one lead duration
     */
    private static RankHistogram ofRankHistogramByLeadThreshold( List<DiagramStatisticOuter> statistics,
                                                                 MetricDimension xDimension,
                                                                 MetricDimension yDimension,
                                                                 ChronoUnit durationUnits )
    {
        return RankHistogram.ofLeadThreshold( statistics, xDimension, yDimension, durationUnits );
    }

    /**
     * Returns a dataset for a rank histogram organized by threshold (one) and then lead duration (up to many).
     * @param statistics the rank histogram statistics
     * @param xDimension the x-axis dimension
     * @param yDimension the y-axis dimension
     * @param durationUnits the lead duration units
     * @return a rank histogram dataset
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */
    private static RankHistogram ofRankHistogramByThresholdLead( List<DiagramStatisticOuter> statistics,
                                                                 MetricDimension xDimension,
                                                                 MetricDimension yDimension,
                                                                 ChronoUnit durationUnits )
    {
        return RankHistogram.ofThresholdLead( statistics, xDimension, yDimension, durationUnits );
    }

    /**
     * Adds one or more series to a collection of time-based pools based on their lead durations and pooling times.
     * 
     * @param collection the collection to expand
     * @param slice the slice of statistics from which to generate series
     * @param leadDurations the lead durations by which to filter series
     * @param poolingTimes the times by which to filter series
     * @param referenceTimes the reference times
     * @param GraphicShape the graphic shape
     * @param durationUnits the duration units
     */

    private static void addSeriesForPoolingWindow( TimeSeriesCollection collection,
                                                   List<DoubleScoreComponentOuter> slice,
                                                   Pair<Duration, Duration> leadDurations,
                                                   Pair<Instant, Instant> poolingTimes,
                                                   GraphicShape graphicShape,
                                                   ChronoUnit durationUnits )
    {
        // Filter by threshold
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( slice, next -> next.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            // Slice the data by threshold.  The resulting data will still contain potentially
            // multiple issued time pooling windows and/or valid time pooling windows.
            List<DoubleScoreComponentOuter> finalSlice =
                    Slicer.filter( slice,
                                   next -> next.getMetadata()
                                               .getThresholds()
                                               .equals( nextThreshold ) );

            // Create the time series with a label
            String seriesName = ChartDataFactory.getNameForPoolingWindowSeries( leadDurations,
                                                                                poolingTimes,
                                                                                nextThreshold,
                                                                                durationUnits );
            TimeSeries next = new TimeSeries( seriesName );

            // Loop through the slice, forming a time series from the issued time or valid time pooling 
            // windows and corresponding values.
            for ( DoubleScoreComponentOuter nextDouble : finalSlice )
            {
                Instant midpoint =
                        ChartDataFactory.getMidpointBetweenTimes( nextDouble.getMetadata()
                                                                            .getTimeWindow(),
                                                                  graphicShape == GraphicShape.ISSUED_DATE_POOLS );

                FixedMillisecond time = new FixedMillisecond( midpoint.toEpochMilli() );
                Double value = nextDouble.getData().getValue();
                next.add( time, value );
            }

            collection.addSeries( next );
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
                Number duration = DataFactory.durationToNumericUnits( latest, durationUnits );
                key = key + duration + ", ";
            }
            else
            {
                key = key + "("
                      + DataFactory.durationToNumericUnits( earliest, durationUnits )
                      + ","
                      + DataFactory.durationToNumericUnits( latest, durationUnits )
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

        return key + thresholds.toStringWithoutUnits();
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
     * Prevent construction.
     */

    private ChartDataFactory()
    {
    }

}
