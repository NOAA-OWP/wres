package wres.io.retrieving;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Utilities for retrieving time-series data.
 * @author James Brown
 */

public class RetrieverUtilities
{
    /** A logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( RetrieverUtilities.class );

    /**
     * Creates a stream of time-series in an analysis shape.
     *
     * @param <T> the time-series event value type
     * @param timeSeries the input series to transform, required
     * @param earliestAnalysisDuration the earliest analysis duration, required
     * @param latestAnalysisDuration the latest analysis duration, required
     * @param duplicatePolicy the duplicate policy, required
     * @param timeWindow the time window, optional
     * @return one series for each analysis duration
     * @throws NullPointerException if any required input is null
     */

    public static <T> Stream<TimeSeries<T>> createAnalysisTimeSeries( Stream<TimeSeries<T>> timeSeries,
                                                                      Duration earliestAnalysisDuration,
                                                                      Duration latestAnalysisDuration,
                                                                      DuplicatePolicy duplicatePolicy,
                                                                      TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( earliestAnalysisDuration );
        Objects.requireNonNull( latestAnalysisDuration );
        Objects.requireNonNull( duplicatePolicy );

        // All events required?
        if ( !RetrieverUtilities.addOneTimeSeriesForEachAnalysisDuration( earliestAnalysisDuration,
                                                                          latestAnalysisDuration ) )
        {
            LOGGER.debug( "No discrete analysis times defined. Built a multi-event timeseries from the analysis." );

            // Apply a duplicate policy and return
            return RetrieverUtilities.applyDuplicatePolicy( timeSeries, duplicatePolicy, timeWindow );
        }
        else
        {
            LOGGER.debug( "Building a single-event timeseries for each analysis duration between {} and {}.",
                          earliestAnalysisDuration,
                          latestAnalysisDuration );

            // No duplicate policy here, because we composed each time-series from common durations
            return RetrieverUtilities.createSeriesPerAnalysisDuration( timeSeries,
                                                                       earliestAnalysisDuration,
                                                                       latestAnalysisDuration );
        }
    }

    /**
     *
     * @param timeWindow the time window, required
     * @param dataType the data type, required
     * @param earliestAnalysisDuration the earliest analysis duration, optional
     * @param latestAnalysisDuration the latest analysis duration, optional
     * @return an adjusted time window, if required
     * @throws NullPointerException if any required input is null
     */

    public static TimeWindowOuter adjustForAnalysisTypeIfRequired( TimeWindowOuter timeWindow,
                                                                   DataType dataType,
                                                                   Duration earliestAnalysisDuration,
                                                                   Duration latestAnalysisDuration )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( dataType );

        TimeWindowOuter adjustedWindow = timeWindow;

        // Analysis data? If so, needs special handling on retrieval
        if ( dataType == DataType.ANALYSES )
        {
            TimeWindowOuter analysisWindow = RetrieverUtilities.getAnalysisTimeWindow( adjustedWindow,
                                                                                       earliestAnalysisDuration,
                                                                                       latestAnalysisDuration );

            LOGGER.debug( "Adjusted the time window during retrieval for time-series data that were identified as "
                          + "\"analyses\". The original time window was: {}. The adjusted time window is: {}.",
                          adjustedWindow,
                          analysisWindow );

            adjustedWindow = analysisWindow;
        }

        return adjustedWindow;
    }

    /**
     * Augments the timescale of the input series, as needed.
     * @param <T> the time-series event value type
     * @param timeSeries the time-series
     * @param orientation the orientation of the time-series
     * @param dataset the dataset declaration
     * @return the augmented time-series
     */
    public static <T> TimeSeries<T> augmentTimeScale( TimeSeries<T> timeSeries,
                                                      DatasetOrientation orientation,
                                                      Dataset dataset )
    {
        // Declared existing scale, which can be used to augment a source
        wres.config.yaml.components.TimeScale declaredExistingTimeScale = dataset.timeScale();

        if ( Objects.nonNull( declaredExistingTimeScale ) )
        {
            TimeScaleOuter existingTimeScaleOuter = TimeScaleOuter.of( declaredExistingTimeScale.timeScale() );
            LOGGER.debug( "Discovered a declared existing time-scale of {} for the {} time-series data. Using this "
                          + "to augment the ingested time time-series.",
                          existingTimeScaleOuter,
                          orientation );

            return TimeSeriesSlicer.augmentTimeSeriesWithTimeScale( timeSeries, existingTimeScaleOuter );
        }

        return timeSeries;
    }

    /**
     * Generates a time window from the supplied time window and adjusts to use unconditional lead durations. In
     * addition, extends the time window to account for the timescale period when the timescale is supplied.
     * @param timeWindow the time window
     * @param timeScale the timescale
     * @return the adjusted time window with unconditional lead durations
     * @throws NullPointerException if the time window is null
     */
    public static TimeWindowOuter getTimeWindowWithUnconditionalLeadTimes( TimeWindowOuter timeWindow,
                                                                           TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( timeWindow );

        // Consider all possible lead durations
        com.google.protobuf.Duration lower =
                com.google.protobuf.Duration.newBuilder()
                                            .setSeconds( TimeWindowOuter.DURATION_MIN.getSeconds() )
                                            .setNanos( TimeWindowOuter.DURATION_MIN.getNano() )
                                            .build();

        com.google.protobuf.Duration upper =
                com.google.protobuf.Duration.newBuilder()
                                            .setSeconds( TimeWindowOuter.DURATION_MAX.getSeconds() )
                                            .setNanos( TimeWindowOuter.DURATION_MAX.getNano() )
                                            .build();

        TimeWindow inner = timeWindow.getTimeWindow()
                                     .toBuilder()
                                     .setEarliestLeadDuration( lower )
                                     .setLatestLeadDuration( upper )
                                     .build();

        return TimeSeriesSlicer.adjustByTimeScalePeriod( TimeWindowOuter.of( inner ),
                                                         timeScale );
    }

    /**
     * Creates an analysis time window from the inputs.
     * @param timeWindow the time window, optional
     * @param earliestAnalysisDuration the earliest analysis duration
     * @param latestAnalysisDuration the latest analysis duration
     * @return the analysis time window
     */

    private static TimeWindowOuter getAnalysisTimeWindow( TimeWindowOuter timeWindow,
                                                          Duration earliestAnalysisDuration,
                                                          Duration latestAnalysisDuration )
    {
        // Change the lead duration to the analysis step set by the user,
        // also set the reference datetime to an infinitely wide range so
        // that we do not restrict the analyses incorrectly.
        if ( Objects.nonNull( earliestAnalysisDuration )
             || Objects.nonNull( latestAnalysisDuration ) )
        {
            LOGGER.debug( "Creating an analysis time window for each analysis duration between {} and {}.",
                          earliestAnalysisDuration,
                          latestAnalysisDuration );

            // See discussion around #74987-174. Ignoring reference times when forming this selection is arbitrary. At 
            // the same time, the declaration does not provide a mechanism to clarify how specific types of reference 
            // time should be treated when that declaration is concerned with filtering or pooling by reference time.
            // TODO: be explicit about the connection between reference times/types and declaration options. 
            // For now, reference times are not used to filter here
            Instant earliestValidTime = Instant.MIN;
            Instant latestValidTime = Instant.MAX;

            if ( Objects.nonNull( timeWindow ) )
            {
                earliestValidTime = timeWindow.getEarliestValidTime();
                latestValidTime = timeWindow.getLatestValidTime();
            }

            TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                             Instant.MAX,
                                                                             earliestValidTime,
                                                                             latestValidTime,
                                                                             earliestAnalysisDuration,
                                                                             latestAnalysisDuration );

            return TimeWindowOuter.of( inner );
        }
        else
        {
            LOGGER.debug( "Building a multi-event analysis retriever." );

            // See discussion around #74987-174. Ignoring reference times when forming this selection is arbitrary. At 
            // the same time, the declaration does not provide a mechanism to clarify how specific types of reference 
            // time should be treated when that declaration is concerned with filtering or pooling by reference time.
            // TODO: be explicit about the connection between reference times/types and declaration options.
            // For now, reference times are not used to filter here
            TimeWindow inner = MessageFactory.getTimeWindow( Instant.MIN,
                                                             Instant.MAX,
                                                             timeWindow.getEarliestValidTime(),
                                                             timeWindow.getLatestValidTime() );
            return TimeWindowOuter.of( inner );
        }
    }

    /**
     * Transforms the input series to create one series for each required analysis duration.
     *
     * @param <T> the time-series event value type
     * @param timeSeries the input series to transform
     * @param earliestAnalysisDuration the earliest analysis duration
     * @param latestAnalysisDuration the latest analysis duration
     * @return one series for each analysis duration
     */

    private static <T> Stream<TimeSeries<T>> createSeriesPerAnalysisDuration( Stream<TimeSeries<T>> timeSeries,
                                                                              Duration earliestAnalysisDuration,
                                                                              Duration latestAnalysisDuration )
    {
        // Filter the time-series. Create one new time-series for each event-by-duration within an existing 
        // time-series whose duration falls within the constraints
        List<TimeSeries<T>> toStream = new ArrayList<>();

        List<TimeSeries<T>> collection = timeSeries.toList();
        for ( TimeSeries<T> next : collection )
        {
            Map<Duration, Event<T>> eventsByDuration =
                    TimeSeriesSlicer.mapEventsByDuration( next, ReferenceTimeType.ANALYSIS_START_TIME );

            for ( Entry<Duration, Event<T>> nextSeries : eventsByDuration.entrySet() )
            {
                Duration duration = nextSeries.getKey();
                Event<T> event = nextSeries.getValue();

                if ( duration.compareTo( earliestAnalysisDuration ) >= 0
                     && duration.compareTo( latestAnalysisDuration ) <= 0 )
                {
                    toStream.add( TimeSeries.of( next.getMetadata(),
                                                 new TreeSet<>( Collections.singleton( event ) ) ) );
                }
            }
        }

        // Warn if no events
        if ( toStream.isEmpty() )
        {
            LOGGER.warn( "While attempting to build a single-event timeseries for each analysis duration between {} "
                         + "and {}, failed to discover any events between those analysis durations.",
                         earliestAnalysisDuration,
                         latestAnalysisDuration );

        }
        else
        {
            LOGGER.debug( "Built {} single-event timeseries for each analysis duration between {} and {}: {}",
                          toStream.size(),
                          earliestAnalysisDuration,
                          latestAnalysisDuration,
                          toStream );
        }

        return toStream.stream();
    }

    /**
     * Applies a duplicate policy to analysis time-series. One of {@link DuplicatePolicy@}.
     *
     * @param <T> the time-series event value type
     * @param timeSeries the input series whose duplicates, if any, should be treated
     * @param duplicatePolicy the duplicate policy
     * @param timeWindow the time window
     * @return the time-series with duplicates treated
     */

    private static <T> Stream<TimeSeries<T>> applyDuplicatePolicy( Stream<TimeSeries<T>> timeSeries,
                                                                   DuplicatePolicy duplicatePolicy,
                                                                   TimeWindowOuter timeWindow )
    {
        // Retain all
        if ( duplicatePolicy == DuplicatePolicy.KEEP_ALL )
        {
            return timeSeries;
        }

        // Filter, handling absence of reference times
        Comparator<Instant> nullsFriendly = Comparator.nullsFirst( Instant::compareTo );
        Comparator<TimeSeries<T>> comparator =
                ( a, b ) -> nullsFriendly.compare( a.getReferenceTimes()
                                                    .get( ReferenceTimeType.ANALYSIS_START_TIME ),
                                                   b.getReferenceTimes()
                                                    .get( ReferenceTimeType.ANALYSIS_START_TIME ) );

        // Keep the duplicate with the earliest reference time
        if ( duplicatePolicy == DuplicatePolicy.KEEP_EARLIEST_REFERENCE_TIME )
        {
            return RetrieverUtilities.filterDuplicatesByValidTime( timeSeries,
                                                                   comparator,
                                                                   duplicatePolicy,
                                                                   timeWindow );
        }
        // Keep the duplicate with the latest reference time
        else if ( duplicatePolicy == DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME )
        {
            // Reversed: latest to earliest by ReferenceTimeType.ANALYSIS_START_TIME
            Comparator<TimeSeries<T>> reversed = comparator.reversed();
            return RetrieverUtilities.filterDuplicatesByValidTime( timeSeries, reversed, duplicatePolicy, timeWindow );
        }
        else
        {
            throw new IllegalStateException( "Encountered unexpected duplicate policy when filtering analysis "
                                             + "time-series for duplicates: "
                                             + duplicatePolicy );
        }
    }

    /**
     * Filters the input time-series for duplicates using a prescribed comparator to order the time-series prior to
     * filtering. The first encountered duplicate, after ordering, will be retained. 
     *
     * @param <T> the time-series event value type
     * @param filterMe the time-series to filter
     * @param comparator the comparator for ordering the time-series
     * @param duplicatePolicy the duplicate policy
     * @param timeWindow the time window
     * @return the filtered time-series with duplicates removed
     */

    private static <T> Stream<TimeSeries<T>> filterDuplicatesByValidTime( Stream<TimeSeries<T>> filterMe,
                                                                          Comparator<TimeSeries<T>> comparator,
                                                                          DuplicatePolicy duplicatePolicy,
                                                                          TimeWindowOuter timeWindow )
    {
        List<TimeSeries<T>> collection = filterMe.sorted( comparator )
                                                 .toList();

        // Record the valid times consumed so far
        Set<Instant> validTimesConsumed = new HashSet<>();

        List<TimeSeries<T>> toStream = new ArrayList<>();
        Comparator<Instant> nullsFriendly = Comparator.nullsFirst( Instant::compareTo );

        for ( TimeSeries<T> next : collection )
        {
            TimeSeries<T> filtered =
                    TimeSeriesSlicer.filterByEvent( next,
                                                    event -> !validTimesConsumed.contains( event.getTime() ) );

            // Add series with some events left and whose reference times fall within the reference time bounds
            // See: #74987-174. TODO: be explicit about the connection between reference times/types and declaration 
            // options. For now, reference times are not used to filter here
            Instant isGreaterThan = null;
            Instant isLessThanOrEqualTo = null;
            if ( Objects.nonNull( timeWindow ) )
            {
                isGreaterThan = timeWindow.getEarliestReferenceTime();
                isLessThanOrEqualTo = timeWindow.getLatestReferenceTime();
            }

            Instant referenceTime = filtered.getReferenceTimes().get( ReferenceTimeType.ANALYSIS_START_TIME );

            // Some events left after filter and either no reference time bounds or reference times are within 
            // bounds
            if ( !filtered.getEvents().isEmpty() && Objects.isNull( timeWindow )
                 || ( nullsFriendly.compare( referenceTime, isGreaterThan ) > 0
                      && nullsFriendly.compare( referenceTime, isLessThanOrEqualTo ) <= 0 ) )
            {
                toStream.add( filtered );

                // Get the valid times to ignore in subsequent series
                Set<Instant> nextValidTimes = next.getEvents()
                                                  .stream()
                                                  .map( Event::getTime )
                                                  .collect( Collectors.toSet() );

                validTimesConsumed.addAll( nextValidTimes );
            }

            if ( LOGGER.isTraceEnabled() && filtered.getEvents().size() != next.getEvents().size() )
            {
                LOGGER.trace( "While filtering analysis time-series {} according to the duplicate policy of {}, "
                              + "removed {} events that were duplicates by valid time across time-series.",
                              next.hashCode(),
                              duplicatePolicy,
                              next.getEvents().size() - filtered.getEvents().size() );
            }
        }

        return toStream.stream();
    }

    /**
     * Returns <code>true</code> if the retriever should return one time-series for each of the common analysis 
     * durations across several reference times of the type {@link ReferenceTimeType#ANALYSIS_START_TIME},
     * <code>false</code> if it should return a single time-series per {@link ReferenceTimeType#ANALYSIS_START_TIME}.
     *
     * @param earliestAnalysisDuration the earliest analysis duration
     * @param latestAnalysisDuration the latest analysis duration
     * @return true if the retriever should return a separate time-series for each analysis duration, otherwise false
     */

    private static boolean addOneTimeSeriesForEachAnalysisDuration( Duration earliestAnalysisDuration,
                                                                    Duration latestAnalysisDuration )
    {
        return !earliestAnalysisDuration.equals( TimeWindowOuter.DURATION_MIN )
               || !latestAnalysisDuration.equals( TimeWindowOuter.DURATION_MAX );
    }

    /**
     * Do not construct.
     */
    private RetrieverUtilities()
    {
    }
}
