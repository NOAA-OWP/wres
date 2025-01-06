package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.TimeWindowAggregation;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;

/**
 * Utility class for manipulating time windows.
 *
 * @author James Brown
 */
public class TimeWindowSlicer
{

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeWindowSlicer.class );

    /** Re-used message. */
    private static final String CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION = "Cannot determine time "
                                                                                         + "windows from missing "
                                                                                         + "declaration.";

    /**
     * Returns a {@link TimeWindowOuter} that represents the union of the inputs, specifically where the
     * {@link TimeWindowOuter#getEarliestReferenceTime()} and {@link TimeWindowOuter#getLatestReferenceTime()} are the
     * earliest and latest instances, respectively, and likewise for the {@link TimeWindowOuter#getEarliestValidTime()}
     * and {@link TimeWindowOuter#getLatestValidTime()}, and the {@link TimeWindowOuter#getEarliestLeadDuration()} and
     * {@link TimeWindowOuter#getLatestLeadDuration()}.
     *
     * @param input the input windows
     * @return the union of the inputs with respect to dates and lead times
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter union( Set<TimeWindowOuter> input )
    {
        Objects.requireNonNull( input, "Cannot determine the union of time windows for a null input." );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for empty input." );
        }

        if ( new HashSet<>( input ).contains( null ) )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for input that contains "
                                                + "one or more null time windows." );
        }

        // Check and set time parameters
        TimeWindowOuter first = input.iterator().next();
        Instant earliestR = first.getEarliestReferenceTime();
        Instant latestR = first.getLatestReferenceTime();
        Instant earliestV = first.getEarliestValidTime();
        Instant latestV = first.getLatestValidTime();
        Duration earliestL = first.getEarliestLeadDuration();
        Duration latestL = first.getLatestLeadDuration();

        for ( TimeWindowOuter next : input )
        {
            if ( earliestR.isAfter( next.getEarliestReferenceTime() ) )
            {
                earliestR = next.getEarliestReferenceTime();
            }
            if ( latestR.isBefore( next.getLatestReferenceTime() ) )
            {
                latestR = next.getLatestReferenceTime();
            }
            if ( earliestL.compareTo( next.getEarliestLeadDuration() ) > 0 )
            {
                earliestL = next.getEarliestLeadDuration();
            }
            if ( latestL.compareTo( next.getLatestLeadDuration() ) < 0 )
            {
                latestL = next.getLatestLeadDuration();
            }
            if ( earliestV.isAfter( next.getEarliestValidTime() ) )
            {
                earliestV = next.getEarliestValidTime();
            }
            if ( latestV.isBefore( next.getLatestValidTime() ) )
            {
                latestV = next.getLatestValidTime();
            }
        }

        TimeWindow unionWindow = wres.statistics.MessageFactory.getTimeWindow( earliestR,
                                                                               latestR,
                                                                               earliestV,
                                                                               latestV,
                                                                               earliestL,
                                                                               latestL );
        return TimeWindowOuter.of( unionWindow );
    }

    /**
     * Returns the intersection of the two sets of {@link TimeWindowOuter}. One {@link TimeWindowOuter} intersects
     * another if it overlaps across all the non-default time dimensions.
     * @param first the first set
     * @param second the second set
     * @return the intersection of the two sets
     * @throws NullPointerException if any input is null
     */
    public static Set<TimeWindowOuter> intersection( Set<TimeWindowOuter> first,
                                                     Set<TimeWindowOuter> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // Short circuit
        if ( first.isEmpty()
             || second.isEmpty() )
        {
            LOGGER.debug( "One or both of the sets to intersect was empty." );

            return Set.of();
        }

        Set<TimeWindowOuter> intersected = new HashSet<>();

        for ( TimeWindowOuter outer : first )
        {
            for ( TimeWindowOuter inner : second )
            {
                if ( TimeWindowSlicer.intersects( outer, inner ) )
                {
                    intersected.add( outer );
                    intersected.add( inner );
                }
            }
        }

        return Collections.unmodifiableSet( intersected );
    }

    /**
     * @param first the first time window
     * @param second the second time window
     * @return true if the two windows intersect in all defined time dimensions, false otherwise
     */
    public static boolean intersects( TimeWindowOuter first, TimeWindowOuter second )
    {
        if ( first.equals( second ) )
        {
            return true;
        }

        boolean leadIntersects = first.bothLeadDurationsAreUnbounded()
                                 || second.bothLeadDurationsAreUnbounded()
                                 // Start of first window is within second window
                                 || ( first.getEarliestLeadDuration()
                                           .compareTo( second.getEarliestLeadDuration() ) >= 0
                                      && first.getEarliestLeadDuration()
                                              .compareTo( second.getLatestLeadDuration() ) <= 0 )
                                 || // Start of second window is within first window
                                 ( second.getEarliestLeadDuration()
                                         .compareTo( first.getEarliestLeadDuration() ) >= 0
                                   && second.getEarliestLeadDuration()
                                            .compareTo( first.getLatestLeadDuration() ) <= 0 )
                                 || // End of first window is within second window
                                 ( first.getLatestLeadDuration()
                                        .compareTo( second.getEarliestLeadDuration() ) >= 0
                                   && first.getLatestLeadDuration()
                                           .compareTo( second.getLatestLeadDuration() ) <= 0 )
                                 || // End of second window is within first window
                                 ( second.getLatestLeadDuration()
                                         .compareTo( first.getEarliestLeadDuration() ) >= 0
                                   && second.getLatestLeadDuration()
                                            .compareTo( first.getLatestLeadDuration() ) <= 0 );

        boolean validIntersects = TimeWindowSlicer.intersects( first.getEarliestValidTime(),
                                                               first.getLatestValidTime(),
                                                               second.getEarliestValidTime(),
                                                               second.getLatestValidTime() );

        boolean referenceIntersects = TimeWindowSlicer.intersects( first.getEarliestReferenceTime(),
                                                                   first.getLatestReferenceTime(),
                                                                   second.getEarliestReferenceTime(),
                                                                   second.getLatestReferenceTime() );

        return leadIntersects
               && validIntersects
               && referenceIntersects;
    }

    /**
     * Aggregates the supplied time windows using a prescribed method. Averaging is approximate.
     *
     * @param timeWindows the time windows
     * @param method the aggregation method.
     * @return the aggregated time windows
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the method is unsupported
     */

    public static TimeWindowOuter aggregate( Set<TimeWindowOuter> timeWindows,
                                             TimeWindowAggregation method )
    {
        Objects.requireNonNull( timeWindows );
        Objects.requireNonNull( method );

        Set<Instant> validStarts = new HashSet<>();
        Set<Instant> validEnds = new HashSet<>();
        Set<Instant> referenceStarts = new HashSet<>();
        Set<Instant> referenceEnds = new HashSet<>();
        Set<Duration> leadStarts = new HashSet<>();
        Set<Duration> leadEnds = new HashSet<>();

        for ( TimeWindowOuter next : timeWindows )
        {
            validStarts.add( next.getEarliestValidTime() );
            validEnds.add( next.getLatestValidTime() );
            referenceStarts.add( next.getEarliestReferenceTime() );
            referenceEnds.add( next.getLatestReferenceTime() );
            leadStarts.add( next.getEarliestLeadDuration() );
            leadEnds.add( next.getLatestLeadDuration() );
        }

        if ( method == TimeWindowAggregation.MINIMUM )
        {
            Instant validStart = validStarts.stream()
                                            .max( Instant::compareTo )
                                            .orElse( Instant.MIN );
            Instant validEnd = validEnds.stream()
                                        .min( Instant::compareTo )
                                        .orElse( Instant.MAX );
            Instant referenceStart = referenceStarts.stream()
                                                    .max( Instant::compareTo )
                                                    .orElse( Instant.MIN );
            Instant referenceEnd = referenceEnds.stream()
                                                .min( Instant::compareTo )
                                                .orElse( Instant.MAX );
            Duration leadStart = leadStarts.stream()
                                           .max( Duration::compareTo )
                                           .orElse( TimeWindowOuter.DURATION_MIN );
            Duration leadEnd = leadEnds.stream()
                                       .min( Duration::compareTo )
                                       .orElse( TimeWindowOuter.DURATION_MAX );

            if ( validEnd.isBefore( validStart ) )
            {
                validEnd = validStart;
            }

            if ( referenceEnd.isBefore( referenceStart ) )
            {
                referenceEnd = referenceStart;
            }

            if ( leadEnd.compareTo( leadStart ) < 0 )
            {
                leadEnd = leadStart;
            }

            TimeWindow window = MessageFactory.getTimeWindow( referenceStart,
                                                              referenceEnd,
                                                              validStart,
                                                              validEnd,
                                                              leadStart,
                                                              leadEnd );
            return TimeWindowOuter.of( window );
        }
        else if ( method == TimeWindowAggregation.MAXIMUM )
        {
            Instant validStart = validStarts.stream()
                                            .min( Instant::compareTo )
                                            .orElse( Instant.MIN );
            Instant validEnd = validEnds.stream()
                                        .max( Instant::compareTo )
                                        .orElse( Instant.MAX );
            Instant referenceStart = referenceStarts.stream()
                                                    .min( Instant::compareTo )
                                                    .orElse( Instant.MIN );
            Instant referenceEnd = referenceEnds.stream()
                                                .max( Instant::compareTo )
                                                .orElse( Instant.MAX );
            Duration leadStart = leadStarts.stream()
                                           .min( Duration::compareTo )
                                           .orElse( TimeWindowOuter.DURATION_MIN );
            Duration leadEnd = leadEnds.stream()
                                       .max( Duration::compareTo )
                                       .orElse( TimeWindowOuter.DURATION_MAX );
            TimeWindow window = MessageFactory.getTimeWindow( referenceStart,
                                                              referenceEnd,
                                                              validStart,
                                                              validEnd,
                                                              leadStart,
                                                              leadEnd );
            return TimeWindowOuter.of( window );
        }
        else if ( method == TimeWindowAggregation.AVERAGE )
        {
            Instant validStart = TimeWindowSlicer.getAverage( validStarts, Instant.MIN );
            Instant validEnd = TimeWindowSlicer.getAverage( validEnds, Instant.MAX );
            Instant referenceStart = TimeWindowSlicer.getAverage( referenceStarts, Instant.MIN );
            Instant referenceEnd = TimeWindowSlicer.getAverage( referenceEnds, Instant.MAX );

            OptionalDouble leadStartMillis = leadStarts.stream()
                                                       .mapToLong( Duration::toMillis )
                                                       .average();
            Duration leadStart = leadStartMillis.isPresent() ?
                    Duration.ofMillis( ( long ) leadStartMillis.getAsDouble() ) :
                    TimeWindowOuter.DURATION_MIN;
            OptionalDouble leadEndMillis = leadEnds.stream()
                                                   .mapToLong( Duration::toMillis )
                                                   .average();
            Duration leadEnd = leadEndMillis.isPresent() ?
                    Duration.ofMillis( ( long ) leadEndMillis.getAsDouble() ) :
                    TimeWindowOuter.DURATION_MAX;
            TimeWindow window = MessageFactory.getTimeWindow( referenceStart,
                                                              referenceEnd,
                                                              validStart,
                                                              validEnd,
                                                              leadStart,
                                                              leadEnd );
            return TimeWindowOuter.of( window );
        }

        throw new IllegalArgumentException( "The time window aggregation method is unsupported: " + method );
    }

    /**
     * Subtracts any non-instantaneous desired timescale from the earliest lead duration and the earliest valid time to
     * ensure that sufficient data is retrieved for upscaling.
     *
     * @param timeWindow the time window to adjust
     * @param timeScale the timescale to use
     * @return the adjusted time window
     */

    public static TimeWindowOuter adjustTimeWindowForTimeScale( TimeWindowOuter timeWindow,
                                                                TimeScaleOuter timeScale )
    {
        TimeWindow.Builder adjusted = timeWindow.getTimeWindow()
                                                .toBuilder();

        // Earliest lead duration
        if ( !timeWindow.getEarliestLeadDuration()
                        .equals( TimeWindowOuter.DURATION_MIN ) )
        {
            Duration period = Duration.ZERO;

            // Adjust the lower bound of the lead duration window by the non-instantaneous desired timescale
            if ( Objects.nonNull( timeScale )
                 && !timeScale.isInstantaneous() )
            {
                period = TimeScaleOuter.getOrInferPeriodFromTimeScale( timeScale );
            }

            Duration lowered = timeWindow.getEarliestLeadDuration()
                                         .minus( period );

            if ( Objects.nonNull( timeScale )
                 && LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Adjusting the lower lead duration of time window {} from {} to {} "
                              + "in order to acquire data at the desired timescale of {}.",
                              timeWindow,
                              timeWindow.getEarliestLeadDuration(),
                              lowered,
                              timeScale );
            }

            adjusted.setEarliestLeadDuration( MessageFactory.getDuration( lowered ) );
        }

        // Earliest valid time
        if ( !timeWindow.getEarliestValidTime()
                        .equals( Instant.MIN ) )
        {
            Duration period = Duration.ZERO;

            // Adjust the lower bound of the lead duration window by the non-instantaneous desired timescale
            if ( Objects.nonNull( timeScale )
                 && !timeScale.isInstantaneous() )
            {
                period = TimeScaleOuter.getOrInferPeriodFromTimeScale( timeScale );
            }

            Instant lowered = timeWindow.getEarliestValidTime()
                                        .minus( period );

            if ( Objects.nonNull( timeScale )
                 && LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Adjusting the lower valid datetime of time window {} from {} to {} "
                              + "in order to acquire data at the desired timescale of {}.",
                              timeWindow,
                              timeWindow.getEarliestValidTime(),
                              lowered,
                              timeScale );
            }

            adjusted.setEarliestValidTime( MessageFactory.getTimestamp( lowered ) );
        }

        return TimeWindowOuter.of( adjusted.build() );
    }

    /**
     * Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation.
     * Only returns the time windows that can be inferred from the declaration alone and not from any ingested data
     * sources, notably those associated with event detection.
     *
     * @param declaration the declaration, cannot be null
     * @return a set of one or more time windows for evaluation
     * @throws NullPointerException if any required input is null
     */

    public static SortedSet<TimeWindowOuter> getTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        TimePools leadDurationPools = declaration.leadTimePools();
        TimePools referenceDatesPools = declaration.referenceDatePools();
        TimePools validDatesPools = declaration.validDatePools();

        Set<TimeWindowOuter> timeWindows = Set.of();  // Default to none

        // Add the time windows generated from a declared sequence
        if ( Objects.nonNull( leadDurationPools )
             || Objects.nonNull( referenceDatesPools )
             || Objects.nonNull( validDatesPools ) )
        {
            timeWindows = TimeWindowSlicer.getTimeWindowsFromPoolSequence( declaration );
        }
        // One big pool if no explicitly declared time windows and no event detection
        else if ( declaration.timePools()
                             .isEmpty()
                  && Objects.isNull( declaration.eventDetection() ) )
        {
            TimeWindowSlicer.LOGGER.debug( "Building one big time window." );

            timeWindows = Collections.singleton( getOneBigTimeWindow( declaration ) );
        }

        // Add the explicitly declared time windows
        SortedSet<TimeWindowOuter> finalWindows = new TreeSet<>( timeWindows );

        TimeWindowSlicer.LOGGER.debug( "Added {} explicitly declared time pools to the overall group of time pools.",
                                       declaration.timePools()
                                                  .size() );

        finalWindows.addAll( declaration.timePools()
                                        .stream()
                                        .map( TimeWindowOuter::of )
                                        .collect( Collectors.toSet() ) );

        return Collections.unmodifiableSortedSet( finalWindows );
    }

    /**
     * <p>Builds a {@link TimeWindowOuter} whose {@link TimeWindow#getEarliestReferenceTime()} and
     * {@link TimeWindow#getLatestReferenceTime()} return the {@code earliest} and {@code latest} bookends of the
     * {@link EvaluationDeclaration#referenceDates()}, respectively, whose {@link TimeWindow#getEarliestValidTime()}
     * and {@link TimeWindow#getLatestValidTime()} return the {@code earliest} and {@code latest} bookends of the
     * {@link EvaluationDeclaration#validDates()}, respectively, and whose {@link TimeWindow#getEarliestLeadDuration()}
     * and {@link TimeWindow#getLatestLeadDuration()} return the {@code minimum} and {@code maximum} bookends of the
     * {@link EvaluationDeclaration#leadTimes()}, respectively.
     *
     * <p>If any of these variables are missing from the input, defaults are used, which represent the
     * computationally-feasible limiting values. For example, the smallest and largest possible instant is
     * {@link Instant#MIN} and {@link Instant#MAX}, respectively. The smallest and largest possible {@link Duration} is
     * {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param declaration the declaration
     * @return a time window
     * @throws NullPointerException if the pairConfig is null
     */

    public static TimeWindowOuter getOneBigTimeWindow( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        Duration smallestLeadDuration = MessageFactory.DURATION_MIN;
        Duration largestLeadDuration = MessageFactory.DURATION_MAX;

        // Reference datetimes
        if ( Objects.nonNull( declaration.referenceDates() ) )
        {
            if ( Objects.nonNull( declaration.referenceDates()
                                             .minimum() ) )
            {
                earliestReferenceTime = declaration.referenceDates()
                                                   .minimum();
            }
            if ( Objects.nonNull( declaration.referenceDates()
                                             .maximum() ) )
            {
                latestReferenceTime = declaration.referenceDates()
                                                 .maximum();
            }
        }

        // Valid datetimes
        if ( Objects.nonNull( declaration.validDates() ) )
        {
            if ( Objects.nonNull( declaration.validDates()
                                             .minimum() ) )
            {
                earliestValidTime = declaration.validDates()
                                               .minimum();
            }
            if ( Objects.nonNull( declaration.validDates()
                                             .maximum() ) )
            {
                latestValidTime = declaration.validDates()
                                             .maximum();
            }
        }

        // Lead durations
        if ( Objects.nonNull( declaration.leadTimes() ) )
        {
            if ( Objects.nonNull( declaration.leadTimes()
                                             .minimum() ) )
            {
                smallestLeadDuration = declaration.leadTimes()
                                                  .minimum();
            }
            if ( Objects.nonNull( declaration.leadTimes()
                                             .maximum() ) )
            {
                largestLeadDuration = declaration.leadTimes()
                                                 .maximum();
            }
        }

        TimeWindow timeWindow = MessageFactory.getTimeWindow( earliestReferenceTime,
                                                              latestReferenceTime,
                                                              earliestValidTime,
                                                              latestValidTime,
                                                              smallestLeadDuration,
                                                              largestLeadDuration );

        return TimeWindowOuter.of( timeWindow );
    }


    /**
     * Generates time windows from an explicit pool sequence.
     *
     * @param declaration the declaration
     * @return the time windows
     */
    private static Set<TimeWindowOuter> getTimeWindowsFromPoolSequence( EvaluationDeclaration declaration )
    {
        TimePools leadDurationPools = declaration.leadTimePools();
        TimePools referenceDatesPools = declaration.referenceDatePools();
        TimePools validDatesPools = declaration.validDatePools();

        Set<TimeWindowOuter> timeWindows;

        // All dimensions
        if ( Objects.nonNull( referenceDatesPools )
             && Objects.nonNull( validDatesPools )
             && Objects.nonNull( leadDurationPools ) )
        {
            LOGGER.debug( "Building time windows for reference dates and valid dates and lead durations." );

            timeWindows = TimeWindowSlicer.getReferenceDatesValidDatesAndLeadDurationTimeWindows( declaration );
        }
        // Reference dates and valid dates
        else if ( Objects.nonNull( referenceDatesPools )
                  && Objects.nonNull( validDatesPools ) )
        {
            LOGGER.debug( "Building time windows for reference dates and valid dates." );

            timeWindows = TimeWindowSlicer.getReferenceDatesAndValidDatesTimeWindows( declaration );
        }
        // Reference dates and lead durations
        else if ( Objects.nonNull( referenceDatesPools )
                  && Objects.nonNull( leadDurationPools ) )
        {
            LOGGER.debug( "Building time windows for reference dates and lead durations." );

            timeWindows = TimeWindowSlicer.getReferenceDatesAndLeadDurationTimeWindows( declaration );
        }
        // Valid dates and lead durations
        else if ( Objects.nonNull( validDatesPools )
                  && Objects.nonNull( leadDurationPools ) )
        {
            LOGGER.debug( "Building time windows for valid dates and lead durations." );

            timeWindows = TimeWindowSlicer.getValidDatesAndLeadDurationTimeWindows( declaration );
        }
        // Reference dates
        else if ( Objects.nonNull( referenceDatesPools ) )
        {
            LOGGER.debug( "Building time windows for reference dates." );

            timeWindows = TimeWindowSlicer.getReferenceDatesTimeWindows( declaration );
        }
        // Lead durations
        else if ( Objects.nonNull( leadDurationPools ) )
        {
            LOGGER.debug( "Building time windows for lead durations." );

            timeWindows = TimeWindowSlicer.getLeadDurationTimeWindows( declaration );
        }
        // Valid dates
        else
        {
            LOGGER.debug( "Building time windows for valid dates." );

            timeWindows = TimeWindowSlicer.getValidDatesTimeWindows( declaration );
        }

        return timeWindows;
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#leadTimePools()} and the {@link EvaluationDeclaration#leadTimes()}.
     * Returns at least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of lead duration time windows
     * @throws NullPointerException if any required input is null
     */

    private static Set<TimeWindowOuter> getLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        String messageStart = "Cannot determine lead duration time windows ";

        Objects.requireNonNull( declaration, messageStart + "from null declaration." );

        LeadTimeInterval leadHours = declaration.leadTimes();

        Objects.requireNonNull( leadHours, "Cannot determine lead duration time windows without 'lead_times'." );
        Objects.requireNonNull( leadHours.minimum(),
                                "Cannot determine lead duration time windows without a 'minimum' value for "
                                + "'lead_times'." );
        Objects.requireNonNull( leadHours.maximum(),
                                "Cannot determine lead duration time windows without a 'maximum' value for "
                                + "'lead_times'." );

        TimePools leadTimesPoolingWindow = declaration.leadTimePools();

        Objects.requireNonNull( leadTimesPoolingWindow,
                                "Cannot determine lead duration time windows without a 'lead_time_pools'." );

        // Obtain the base window
        TimeWindowOuter baseWindow = TimeWindowSlicer.getOneBigTimeWindow( declaration );

        // Period associated with the leadTimesPoolingWindow
        Duration periodOfLeadTimesPoolingWindow = leadTimesPoolingWindow.period();

        // Exclusive lower bound: #56213-104
        Duration earliestLeadDurationExclusive = leadHours.minimum();

        // Inclusive upper bound
        Duration latestLeadDurationInclusive = leadHours.maximum();

        // Duration by which to increment. Defaults to the period associated
        // with the leadTimesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfLeadTimesPoolingWindow;
        if ( Objects.nonNull( leadTimesPoolingWindow.frequency() ) )
        {
            increment = leadTimesPoolingWindow.frequency();
        }

        // Lower bound of the current window
        Duration earliestExclusive = earliestLeadDurationExclusive;

        // Upper bound of the current window
        Duration latestInclusive = earliestExclusive.plus( periodOfLeadTimesPoolingWindow );

        // Create the time windows
        Set<TimeWindowOuter> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound extends past the
        // latestLeadDurationInclusive: #56213-104
        // Window increments are zero?
        if ( Duration.ZERO.equals( increment ) )
        {
            com.google.protobuf.Duration earliest = MessageFactory.getDuration( earliestExclusive );
            com.google.protobuf.Duration latest = MessageFactory.getDuration( latestInclusive );
            TimeWindow window = baseWindow.getTimeWindow()
                                          .toBuilder()
                                          .setEarliestLeadDuration( earliest )
                                          .setLatestLeadDuration( latest )
                                          .build();
            timeWindows.add( TimeWindowOuter.of( window ) );
        }
        // Create as many windows as required at the prescribed increment
        else
        {
            while ( latestInclusive.compareTo( latestLeadDurationInclusive ) <= 0 )
            {
                // Add the current time window
                com.google.protobuf.Duration earliest = MessageFactory.getDuration( earliestExclusive );
                com.google.protobuf.Duration latest = MessageFactory.getDuration( latestInclusive );
                TimeWindow window = baseWindow.getTimeWindow()
                                              .toBuilder()
                                              .setEarliestLeadDuration( earliest )
                                              .setLatestLeadDuration( latest )
                                              .build();
                timeWindows.add( TimeWindowOuter.of( window ) );

                // Increment from left-to-right: #56213-104
                earliestExclusive = earliestExclusive.plus( increment );
                latestInclusive = latestInclusive.plus( increment );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#referenceDatePools()} and the
     * {@link EvaluationDeclaration#referenceDates()}. Returns at least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of reference time windows
     * @throws NullPointerException if the declaration is null or any required input within it is null
     */

    private static Set<TimeWindowOuter> getReferenceDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, "Cannot determine reference time windows from missing "
                                             + "declaration." );
        Objects.requireNonNull( declaration.referenceDates(),
                                "Cannot determine reference time windows without 'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDates()
                                           .minimum(),
                                "Cannot determine reference time windows without the 'minimum' for the "
                                + "'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDates()
                                           .maximum(),
                                "Cannot determine reference time windows without the 'maximum' for the "
                                + "'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDatePools(),
                                "Cannot determine reference time windows without 'reference_date_pools'." );

        // Base window from which to generate a sequence of windows
        TimeWindowOuter baseWindow = TimeWindowSlicer.getOneBigTimeWindow( declaration );

        return TimeWindowSlicer.getTimeWindowsForDateSequence( declaration.referenceDates(),
                                                               declaration.referenceDatePools(),
                                                               baseWindow,
                                                               true );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}.
     * Returns at least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of reference time windows
     * @throws NullPointerException if the declaration is null or any required input within it is null
     */

    private static Set<TimeWindowOuter> getValidDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, "Cannot determine valid time windows from missing declaration." );
        Objects.requireNonNull( declaration.validDates(),
                                "Cannot determine valid time windows without 'valid_dates'." );
        Objects.requireNonNull( declaration.validDates()
                                           .minimum(),
                                "Cannot determine valid time windows without the 'minimum' for the "
                                + "'valid_dates'." );
        Objects.requireNonNull( declaration.validDates()
                                           .maximum(),
                                "Cannot determine valid time windows without the 'maximum' for the "
                                + "'valid_dates'." );
        Objects.requireNonNull( declaration.validDatePools(),
                                "Cannot determine valid time windows without 'valid_date_pools'." );

        // Base window from which to generate a sequence of windows
        TimeWindowOuter baseWindow = TimeWindowSlicer.getOneBigTimeWindow( declaration );

        return TimeWindowSlicer.getTimeWindowsForDateSequence( declaration.validDates(),
                                                               declaration.validDatePools(),
                                                               baseWindow,
                                                               false );
    }

    /**
     * <p>Generates a set of time windows based on a sequence of datetimes.
     *
     * @param dates the date constraints
     * @param pools the sequence of datetimes to generate
     * @param baseWindow the basic time window from which each pool in the sequence begins
     * @param areReferenceTimes is true if the dates are reference dates, false for valid dates
     * @return the set of reference time windows
     * @throws NullPointerException if any input is null
     */

    private static Set<TimeWindowOuter> getTimeWindowsForDateSequence( TimeInterval dates,
                                                                       TimePools pools,
                                                                       TimeWindowOuter baseWindow,
                                                                       boolean areReferenceTimes )
    {
        Objects.requireNonNull( dates );
        Objects.requireNonNull( pools );
        Objects.requireNonNull( baseWindow );

        // Period associated with the reference time pool
        Duration periodOfPoolingWindow = pools.period();

        // Exclusive lower bound: #56213-104
        Instant earliestInstantExclusive = dates.minimum();

        // Inclusive upper bound
        Instant latestInstantInclusive = dates.maximum();

        // Duration by which to increment. Defaults to the period associated with the reference time pools, otherwise
        // the frequency.
        Duration increment = periodOfPoolingWindow;
        if ( Objects.nonNull( pools.frequency() ) )
        {
            increment = pools.frequency();
        }

        // Lower bound of the current window
        Instant earliestExclusive = earliestInstantExclusive;

        // Upper bound of the current window
        Instant latestInclusive = earliestExclusive.plus( periodOfPoolingWindow );

        // Create the time windows
        Set<TimeWindowOuter> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound
        // extends past the latestInstantInclusive: #56213-104
        while ( latestInclusive.compareTo( latestInstantInclusive ) <= 0 )
        {
            TimeWindowOuter timeWindow = TimeWindowSlicer.getTimeWindowFromDates( earliestExclusive,
                                                                                  latestInclusive,
                                                                                  baseWindow,
                                                                                  areReferenceTimes );

            // Add the current time window
            timeWindows.add( timeWindow );

            // Increment left-to-right: #56213-104
            earliestExclusive = earliestExclusive.plus( increment );
            latestInclusive = latestInclusive.plus( increment );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * Returns a time window from the inputs.
     *
     * @param earliestExclusive the earliest exclusive time
     * @param latestInclusive the latest inclusive time
     * @param baseWindow the base window with default times
     * @param areReferenceTimes is true if the earliestExclusive and latestInclusive are reference times, false for
     *                          valid times
     * @return a time window
     */

    private static TimeWindowOuter getTimeWindowFromDates( Instant earliestExclusive,
                                                           Instant latestInclusive,
                                                           TimeWindowOuter baseWindow,
                                                           boolean areReferenceTimes )
    {
        Timestamp earliest = MessageFactory.getTimestamp( earliestExclusive );
        Timestamp latest = MessageFactory.getTimestamp( latestInclusive );

        // Reference dates
        if ( areReferenceTimes )
        {
            TimeWindow window = baseWindow.getTimeWindow()
                                          .toBuilder()
                                          .setEarliestReferenceTime( earliest )
                                          .setLatestReferenceTime( latest )
                                          .build();
            return TimeWindowOuter.of( window );
        }
        // Valid dates
        else
        {
            TimeWindow window = baseWindow.getTimeWindow()
                                          .toBuilder()
                                          .setEarliestValidTime( earliest )
                                          .setLatestValidTime( latest )
                                          .build();
            return TimeWindowOuter.of( window );
        }
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#leadTimePools()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#referenceDatePools()} and the {@link EvaluationDeclaration#referenceDates()}.
     * Returns at least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of lead duration and reference time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindowOuter> getReferenceDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowSlicer.getLeadDurationTimeWindows( declaration );

        Set<TimeWindowOuter> referenceDatesWindows = TimeWindowSlicer.getReferenceDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindowOuter> timeWindows =
                new HashSet<>( leadDurationWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindowOuter nextReferenceWindow : referenceDatesWindows )
        {
            for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
            {
                TimeWindow window = nextReferenceWindow.getTimeWindow()
                                                       .toBuilder()
                                                       .setEarliestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                                               .getEarliestLeadDuration() )
                                                       .setLatestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                                             .getLatestLeadDuration() )
                                                       .build();
                timeWindows.add( TimeWindowOuter.of( window ) );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#referenceDatePools()}, the {@link EvaluationDeclaration#referenceDates()},
     * the {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}. Returns at
     * least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of reference time and valid time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindowOuter> getReferenceDatesAndValidDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindowOuter> validDatesWindows = TimeWindowSlicer.getValidDatesTimeWindows( declaration );

        Set<TimeWindowOuter> referenceDatesWindows = TimeWindowSlicer.getReferenceDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( validDatesWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindowOuter nextValidWindow : validDatesWindows )
        {
            for ( TimeWindowOuter nextReferenceWindow : referenceDatesWindows )
            {
                TimeWindow window =
                        nextValidWindow.getTimeWindow()
                                       .toBuilder()
                                       .setEarliestReferenceTime( nextReferenceWindow.getTimeWindow()
                                                                                     .getEarliestReferenceTime() )
                                       .setLatestReferenceTime( nextReferenceWindow.getTimeWindow()
                                                                                   .getLatestReferenceTime() )
                                       .build();
                timeWindows.add( TimeWindowOuter.of( window ) );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#leadTimePools()} ()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#validDatePools()} ()} and the {@link EvaluationDeclaration#validDates()}. Returns
     * at least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of lead duration and valid dates time windows
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindowOuter> getValidDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowSlicer.getLeadDurationTimeWindows( declaration );

        Set<TimeWindowOuter> validDatesWindows = TimeWindowSlicer.getValidDatesTimeWindows( declaration );

        // Create a new window for each combination of valid dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( leadDurationWindows.size() * validDatesWindows.size() );
        for ( TimeWindowOuter nextValidWindow : validDatesWindows )
        {
            for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
            {
                TimeWindow window =
                        nextValidWindow.getTimeWindow()
                                       .toBuilder()
                                       .setEarliestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                               .getEarliestLeadDuration() )
                                       .setLatestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                             .getLatestLeadDuration() )
                                       .build();
                timeWindows.add( TimeWindowOuter.of( window ) );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindowOuter} for evaluation
     * using the {@link EvaluationDeclaration#leadTimePools()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#referenceDatePools()} the {@link EvaluationDeclaration#referenceDates()}, the
     * {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}. Returns at
     * least one {@link TimeWindowOuter}.
     *
     * @param declaration the declaration
     * @return the set of lead duration, reference time and valid time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindowOuter> getReferenceDatesValidDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowSlicer.getLeadDurationTimeWindows( declaration );
        Set<TimeWindowOuter> referenceDatesWindows = TimeWindowSlicer.getReferenceDatesTimeWindows( declaration );
        Set<TimeWindowOuter> validDatesWindows = TimeWindowSlicer.getValidDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( leadDurationWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindowOuter nextReferenceWindow : referenceDatesWindows )
        {
            for ( TimeWindowOuter nextValidWindow : validDatesWindows )
            {
                for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
                {
                    TimeWindow window =
                            nextValidWindow.getTimeWindow()
                                           .toBuilder()
                                           .setEarliestReferenceTime( nextReferenceWindow.getTimeWindow()
                                                                                         .getEarliestReferenceTime() )
                                           .setLatestReferenceTime( nextReferenceWindow.getTimeWindow()
                                                                                       .getLatestReferenceTime() )
                                           .setEarliestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                                   .getEarliestLeadDuration() )
                                           .setLatestLeadDuration( nextLeadWindow.getTimeWindow()
                                                                                 .getLatestLeadDuration() )
                                           .build();
                    timeWindows.add( TimeWindowOuter.of( window ) );
                }
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * @param firstLower the lower bound of the first window
     * @param firstUpper the upper bound of the first window
     * @param secondLower the lower bound of the second window
     * @param secondUpper the upper bound of the second window
     * @return whether the windows intersect
     */
    private static boolean intersects( Instant firstLower,
                                       Instant firstUpper,
                                       Instant secondLower,
                                       Instant secondUpper )
    {
        return // Start of first window is within second window
                ( firstLower.compareTo( secondLower ) >= 0
                  && firstLower.compareTo( secondUpper ) <= 0 )
                || // Start of second window is within first window
                ( secondLower.compareTo( firstLower ) >= 0
                  && secondLower.compareTo( firstUpper ) <= 0 )
                || // End of first window is within second window
                ( firstUpper.compareTo( secondLower ) >= 0
                  && firstUpper.compareTo( secondUpper ) <= 0 )
                || // End of second window is within first window
                ( secondUpper.compareTo( firstLower ) >= 0
                  && secondUpper.compareTo( firstUpper ) <= 0 );
    }

    /**
     * Calculates the average instant to millisecond precision, returning the
     * specified default if unavailable.
     *
     * @param instants the instants
     * @param defaultInstant the default instant
     * @return the average or default
     */
    private static Instant getAverage( Set<Instant> instants,
                                       Instant defaultInstant )
    {
        OptionalDouble averageMillis = instants.stream()
                                               .mapToLong( Instant::toEpochMilli )
                                               .average();
        return averageMillis.isPresent() ?
                Instant.ofEpochMilli( ( long ) averageMillis.getAsDouble() ) :
                defaultInstant;
    }

    /**
     * Do not construct.
     */
    private TimeWindowSlicer()
    {
    }

}