package wres.datamodel.time;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.measure.Unit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.units.Units;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;

/**
 * Helper class for supporting rescaling operations.
 *
 * @author James Brown
 */

class RescalingHelper
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( RescalingHelper.class );

    private static final EvaluationStatusMessage DID_NOT_DETECT_AN_ATTEMPT_TO_ACCUMULATE =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "Did not detect an attempt to accumulate "
                                          + "something that is not an accumulation." );

    private static final EvaluationStatusMessage NOT_ATTEMPTING_TO_ACCUMULATE_AN_INSTANTANEOUS_VALUE =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "Not attempting to accumulate an instantaneous value." );

    private static final EvaluationStatusMessage NO_ATTEMPT_WAS_MADE_TO_CHANGE_THE_TIME_SCALE_FUNCTION =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "No attempt was made to change the time scale function without "
                                          + "also changing the period." );

    private static final EvaluationStatusMessage THE_EVALUATION_PERIOD_OF_ZERO_IS_AN_INTEGER_MULTIPLE =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "The evaluation period is an integer multiple of the dataset "
                                          + "period and is, therefore, acceptable." );

    private static final EvaluationStatusMessage THE_DATASET_PERIOD_OF_ZERO_IS_NOT_LARGER_THAN_THE_EVALUATION_PERIOD =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "The dataset period is not larger than the evaluation period and "
                                          + "is, therefore, acceptable." );

    private static final EvaluationStatusMessage
            THE_EVALUATION_SCALE_FUNCTION_IS_NOT_UNKNOWN_AND_IS_THEREFORE_ACCEPTABLE =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "The function associated with the evaluation time scale is not "
                                          + "unknown and is, therefore, acceptable." );

    private static final EvaluationStatusMessage GROUPED_EVENTS_MESSAGE =
            EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                          "Computing grouped events for rescaling using the time scale period "
                                          + "only." );

    private static final String THE_FUNCTION_ASSOCIATED_WITH = "The function associated with "
                                                               + "the evaluation time scale is a ''{0}'', "
                                                               + "but the function associated with the "
                                                               + "dataset time scale is ''{1}''. "
                                                               + "Assuming that the latter is also a ''{2}''.";

    private static final String EXISTING_TIME_SCALE_IS_MISSING = "While attempting to upscale to an evaluation time "
                                                                 + "scale of ''{0}'', encountered a time-series whose "
                                                                 + "existing time-scale is undefined. This occurs when "
                                                                 + "the data source fails to identify its time scale"
                                                                 + "and the project declaration fails to clarify this "
                                                                 + "information. Please include the time scale "
                                                                 + "('time_scale') in the project declaration for each "
                                                                 + "dataset that must be rescaled and does not clarify "
                                                                 + "its own time scale, otherwise a change of scale is "
                                                                 + "impossible.";

    private static final String EXISTING_TIME_SCALE_IS_MISSING_DESIRED_INSTANTANEOUS = "Encountered a time-series "
                                                                                       + "whose time scale is "
                                                                                       + "undefined, but an evaluation "
                                                                                       + "time scale that is "
                                                                                       + "instantaneous. This occurs "
                                                                                       + "when the data source fails to "
                                                                                       + "identify its own time "
                                                                                       + "scale and the project "
                                                                                       + "declaration fails to clarify "
                                                                                       + "this information. Consider "
                                                                                       + "including the time "
                                                                                       + "scale ('time_scale') in "
                                                                                       + "the project declaration for "
                                                                                       + "each dataset whose time scale "
                                                                                       + "is undefined. In the mean "
                                                                                       + "time, assuming that the "
                                                                                       + "time-series has an "
                                                                                       + "instantaneous time scale.";

    private static final String THESE_INTERVALS_BEFORE_STOPPING = "these intervals before stopping: ";

    private static final String DISCOVERED_THAT_THE_VALUES_WERE_NOT_EVENLY_SPACED_WITHIN_THE_PERIOD_IDENTIFIED =
            ", discovered that the values were not evenly spaced within the period. Identified ";

    private static final String DISCOVERED_FEWER_THAN_TWO_EVENTS_IN_THE_COLLECTION =
            ", discovered fewer than two events in the collection, which cannot produce an upscaled value.";

    private static final String ENDING_AT = " ending at ";

    private static final String EVENTS_TO_AN_EVALUATION_TIME_SCALE_OF = " events to an evaluation time scale of ";

    private static final String WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF =
            "While attempting to upscale a collection of ";

    private static final String THREE_MEMBER_MESSAGE = "{}{}{}";

    private static final String FIVE_MEMBER_MESSAGE = "{}{}{}{}{}";

    private static final String SEVEN_MEMBER_MESSAGE = "{}{}{}{}{}{}{}";

    private static final String THE_LENIENCY_STATUS_WAS = ". The leniency status was: ";

    private static final String EVALUATION_TIME_SCALE_LENIENT = " Consider setting the option 'rescale_lenience: all' "
                                                                + "to ignore missing data, accept limited data and "
                                                                + "allow for unequally spaced values.";

    /** Default group rescaling status. */
    private static final GroupRescalingStatus DEFAULT_GROUP_RESCALING_STATUS =
            new GroupRescalingStatus( true, List.of() );

    /**
     * Conducts upscaling of a time-series.
     *
     * @param <T> the type of event value to upscale
     * @param timeSeries the time-series, required
     * @param upscaler the function that upscales the event values
     * @param desiredTimeScale the desired time scale, required
     * @param existingUnit the existing measurement unit, optional
     * @param desiredUnit the desired measurement unit, optional
     * @param endsAt the set of times at which upscaled values should end, required
     * @param lenient is true to upscale irregularly spaced data (e.g., due to missing values)
     * @return the upscaled time-series and associated validation events
     * @throws NullPointerException if any required input is null
     */

    static <T> RescaledTimeSeriesPlusValidation<T> upscale( TimeSeries<T> timeSeries,
                                                            Function<SortedSet<Event<T>>, T> upscaler,
                                                            TimeScaleOuter desiredTimeScale,
                                                            Unit<?> existingUnit,
                                                            Unit<?> desiredUnit,
                                                            SortedSet<Instant> endsAt,
                                                            boolean lenient )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( desiredTimeScale );
        Objects.requireNonNull( endsAt );

        // Validate the request
        List<EvaluationStatusMessage> validationEvents = RescalingHelper.validate( timeSeries,
                                                                                   desiredTimeScale,
                                                                                   existingUnit,
                                                                                   desiredUnit );

        // Empty time-series
        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( THREE_MEMBER_MESSAGE,
                              "No upscaling required for time-series ",
                              timeSeries.getMetadata(),
                              ": the time-series contained no events." );
            }

            // Return an empty time-series with the desired timescale: #93194
            TimeSeriesMetadata metadata =
                    new TimeSeriesMetadata.Builder( timeSeries.getMetadata() ).setTimeScale( desiredTimeScale )
                                                                              .build();

            TimeSeries<T> scaledEmpty = new TimeSeries.Builder<T>().setMetadata( metadata )
                                                                   .build();

            return RescaledTimeSeriesPlusValidation.of( scaledEmpty, validationEvents );
        }

        // Existing timescale missing and this was allowed during validation
        if ( !timeSeries.hasTimeScale() )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( FIVE_MEMBER_MESSAGE,
                              "Skipped upscaling time-series ",
                              timeSeries.getMetadata(),
                              " to the evaluation time scale of ",
                              desiredTimeScale,
                              " because the dataset time scale was missing. Assuming that the dataset and evaluation "
                              + "time scales are the same." );
            }

            // Return a time-series with the desired timescale: #93194
            TimeSeriesMetadata metadata =
                    new TimeSeriesMetadata.Builder( timeSeries.getMetadata() ).setTimeScale( desiredTimeScale )
                                                                              .build();

            TimeSeries<T> scaledSeries = new TimeSeries.Builder<T>().setMetadata( metadata )
                                                                    .setEvents( timeSeries.getEvents() )
                                                                    .build();

            return RescaledTimeSeriesPlusValidation.of( scaledSeries, validationEvents );
        }

        // Existing and desired are both instantaneous
        if ( timeSeries.getTimeScale()
                       .isInstantaneous()
             && desiredTimeScale.isInstantaneous() )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( SEVEN_MEMBER_MESSAGE,
                              "Skipped upscaling time-series ",
                              timeSeries.getMetadata(),
                              " to the evaluation time scale of ",
                              desiredTimeScale,
                              " because the dataset time scale is ",
                              timeSeries.getTimeScale(),
                              " and both are recognized as instantaneous." );
            }

            return RescaledTimeSeriesPlusValidation.of( timeSeries, validationEvents );
        }

        // If the period is the same, return the existing series with the desired scale
        // The validation of the function happens above. For example, the existing could be UNKNOWN
        if ( desiredTimeScale.hasPeriod()
             && desiredTimeScale.getPeriod()
                                .equals( timeSeries.getTimeScale()
                                                   .getPeriod() ) )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( SEVEN_MEMBER_MESSAGE,
                              "No upscaling required for time-series ",
                              timeSeries.getMetadata(),
                              ": the dataset time scale of ",
                              timeSeries.getTimeScale(),
                              " effectively matches the evaluation time scale of ",
                              desiredTimeScale,
                              "." );
            }

            TimeSeriesMetadata existingMetadata = timeSeries.getMetadata();

            // Create new series in case the function differs
            TimeSeriesMetadata metadata =
                    new TimeSeriesMetadata.Builder( existingMetadata ).setTimeScale( desiredTimeScale )
                                                                      .build();

            TimeSeries<T> returnMe = new TimeSeries.Builder<T>().setMetadata( metadata )
                                                                .setEvents( timeSeries.getEvents() )
                                                                .build();

            return RescaledTimeSeriesPlusValidation.of( returnMe, validationEvents );
        }

        // True upscaling needed
        return RescalingHelper.upscaleWithChangeOfPeriod( timeSeries,
                                                          upscaler,
                                                          desiredTimeScale,
                                                          endsAt,
                                                          validationEvents,
                                                          lenient );
    }

    /**
     * Checks the input list for rescaling errors and throws an exception if any are found.
     * @param events the events to check
     * @param context the context to use when throwing an exception
     * @throws RescalingException if any of the input events is an error
     */

    static void checkForRescalingErrorsAndThrowExceptionIfRequired( List<EvaluationStatusMessage> events,
                                                                    TimeSeriesMetadata context )
    {
        List<EvaluationStatusMessage> errors = events.stream()
                                                     .filter( a -> a.getStatusLevel() == StatusLevel.ERROR )
                                                     .toList();
        String spacer = "    ";

        if ( !errors.isEmpty() )
        {
            StringJoiner message = new StringJoiner( System.lineSeparator() );
            message.add( "Encountered "
                         + errors.size()
                         + " errors while attempting to upscale time-series "
                         + context // #93180
                         + ": " );

            errors.forEach( e -> message.add( spacer + spacer + e.toString() ) );

            throw new RescalingException( message.toString() );
        }
    }

    /**
     * Returns a metadata mapper that uses the input metadata as a platform and modifies the time scale and units to 
     * the prescribed values.
     *
     * @param existing the existing metadata
     * @param desiredTimeScale the desired time scale
     * @param desiredUnitString the desired unit string
     * @return the mapper metadata
     * @throws NullPointerException if any input is null
     */

    static UnaryOperator<TimeSeriesMetadata> getMetadataMapper( TimeSeriesMetadata existing,
                                                                TimeScaleOuter desiredTimeScale,
                                                                String desiredUnitString )
    {
        Objects.requireNonNull( existing );
        Objects.requireNonNull( desiredTimeScale );
        Objects.requireNonNull( desiredUnitString );

        // Create a converted time-series
        // Update the units and time scale function
        return metadata -> {
            Duration existingScalePeriod = desiredTimeScale.getPeriod();
            TimeScaleOuter newDesiredTimeScale = TimeScaleOuter.of( existingScalePeriod, TimeScaleFunction.TOTAL );
            return existing.toBuilder()
                           .setUnit( desiredUnitString )
                           .setTimeScale( newDesiredTimeScale )
                           .build();
        };
    }

    /**
     * Conducts upscaling of a time-series.
     *
     * @param <T> the type of event value to upscale
     * @param timeSeries the time-series
     * @param upscaler the function that upscales the event values
     * @param desiredTimeScale the desired time scale
     * @param endsAt the set of times at which upscaled values should end
     * @param validationEvents the validation events
     * @param lenient is true to upscale irregularly spaced data (e.g., due to missing values)
     * @return the upscaled time-series and associated validation events
     * @throws NullPointerException if any input is null
     */

    private static <T> RescaledTimeSeriesPlusValidation<T> upscaleWithChangeOfPeriod( TimeSeries<T> timeSeries,
                                                                                      Function<SortedSet<Event<T>>, T> upscaler,
                                                                                      TimeScaleOuter desiredTimeScale,
                                                                                      SortedSet<Instant> endsAt,
                                                                                      List<EvaluationStatusMessage> validationEvents,
                                                                                      boolean lenient )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( upscaler );
        Objects.requireNonNull( desiredTimeScale );
        Objects.requireNonNull( endsAt );
        Objects.requireNonNull( validationEvents );

        // Create a mutable copy of the validation events to add more, as needed
        validationEvents = new ArrayList<>( validationEvents );

        // Get the grouped events to upscale
        Map<Instant, SortedSet<Event<T>>> groups = RescalingHelper.getGroupedEventsToUpscale( timeSeries,
                                                                                              desiredTimeScale,
                                                                                              endsAt,
                                                                                              validationEvents );

        // Process the groups whose events are evenly-spaced and have no missing values, otherwise skip and log
        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();

        // Upscale each group, if possible
        for ( Map.Entry<Instant, SortedSet<Event<T>>> nextGroup : groups.entrySet() )
        {
            Instant endsAtTime = nextGroup.getKey();

            GroupRescalingStatus status = RescalingHelper.checkThatUpscalingIsPossible( nextGroup.getValue(),
                                                                                        endsAtTime,
                                                                                        desiredTimeScale,
                                                                                        lenient );

            validationEvents.addAll( status.events() );

            // Can rescale?
            if ( status.canRescale() )
            {
                Instant nextKey = nextGroup.getKey();
                SortedSet<Event<T>> nextValue = nextGroup.getValue();
                T result = upscaler.apply( nextValue );
                Event<T> upscaled = Event.of( nextKey, result );
                builder.addEvent( upscaled );
            }
        }

        // Set the larger scale
        TimeSeriesMetadata templateMetadata = timeSeries.getMetadata();
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( templateMetadata.getReferenceTimes(),
                                                             desiredTimeScale,
                                                             templateMetadata.getVariableName(),
                                                             templateMetadata.getFeature(),
                                                             templateMetadata.getUnit() );
        builder.setMetadata( metadata );

        return RescaledTimeSeriesPlusValidation.of( builder.build(), Collections.unmodifiableList( validationEvents ) );
    }

    /**
     * Creates the grouped events to upscale.
     *
     * @param <T> the type of event value
     * @param timeSeries the time series
     * @param desiredTimeScale the desired time scale
     * @param endsAt the time at which values should end, if applicable
     * @param validationEvents a mutable list of validation events
     * @return the grouped events
     */

    private static <T> Map<Instant, SortedSet<Event<T>>> getGroupedEventsToUpscale( TimeSeries<T> timeSeries,
                                                                                    TimeScaleOuter desiredTimeScale,
                                                                                    SortedSet<Instant> endsAt,
                                                                                    List<EvaluationStatusMessage> validationEvents )
    {
        // No month-days present?
        if ( !desiredTimeScale.hasMonthDays() )
        {
            validationEvents.add( GROUPED_EVENTS_MESSAGE );

            SortedSet<Instant> endsAtTime = endsAt;

            // No times at which values should end, so start at the beginning
            if ( endsAtTime.isEmpty() )
            {
                endsAtTime = RescalingHelper.getEndTimesFromSeries( timeSeries, desiredTimeScale );
            }

            // Group the events according to whether their valid times fall within the desired period that ends at a 
            // particular valid time
            Duration period = desiredTimeScale.getPeriod();

            // This grouping operation is expensive: see the notes attached to the method
            return TimeSeriesSlicer.groupEventsByInterval( timeSeries.getEvents(), endsAtTime, period );
        }

        // One or both month-days are present, so compute end times directly. Input end times are unexpected and will be 
        // ignored. This is developer-facing information, not user-facing information, so log
        if ( !endsAt.isEmpty() && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "When attempting to rescale a time-series whose evaluation time scale contains one or both "
                          + "month-days, discovered an explicit list of end times at which to derive rescaled values, "
                          + "which is unexpected. These end times will be ignored." );
        }

        // Create the intervals
        SortedSet<Pair<Instant, Instant>> intervals =
                TimeSeriesSlicer.getIntervalsFromTimeScaleWithMonthDays( desiredTimeScale,
                                                                         timeSeries );

        // Group the events by interval
        return TimeSeriesSlicer.groupEventsByInterval( timeSeries.getEvents(), intervals );
    }

    /**
     * <p>Inspects a time-series and returns the end times that increment from the first possible time. Since the
     * upscaling interval is right-closed, the inspection begins at one time-step before the first time in the series.
     *
     * <p>TODO: abstract this validation if other, future, implementations rely on similar inspections.
     *
     * @return the times at which upscaled intervals should end
     */

    private static <T> SortedSet<Instant> getEndTimesFromSeries( TimeSeries<T> timeSeries,
                                                                 TimeScaleOuter desiredTimeScale )
    {
        SortedSet<Instant> endsAt = new TreeSet<>();

        Instant firstTime = timeSeries.getEvents().first().getTime();
        Instant lastTime = timeSeries.getEvents().last().getTime();
        Duration period = desiredTimeScale.getPeriod();

        Duration timeStep = Duration.ZERO; // The inert amount by which to back-pedal
        if ( timeSeries.getEvents()
                       .size() > 1 )
        {
            Iterator<Instant> iterator = timeSeries.getEvents()
                                                   .stream()
                                                   .map( Event::getTime )
                                                   .iterator();
            timeStep = Duration.between( iterator.next(), iterator.next() );
        }

        // Loop until the next time is later than the last
        // Subtract the time-step so that the first value is always considered
        // because the interval is right-closed
        Instant check = firstTime.minus( timeStep );

        while ( check.isBefore( lastTime ) )
        {
            Instant next = check.plus( period );
            endsAt.add( next );
            check = next; // Increment
        }

        return Collections.unmodifiableSortedSet( endsAt );
    }

    /**
     * Validates the request to upscale and throws an exception if the request is invalid. 
     *
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired scale
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @throws RescalingException if the input cannot be rescaled to the desired scale
     * @return the validation events
     */

    private static <T> List<EvaluationStatusMessage> validate( TimeSeries<T> timeSeries,
                                                               TimeScaleOuter desiredTimeScale,
                                                               Unit<?> existingUnit,
                                                               Unit<?> desiredUnit )
    {
        List<EvaluationStatusMessage> events = RescalingHelper.validateForUpscaling( timeSeries.getTimeScale(),
                                                                                     desiredTimeScale,
                                                                                     existingUnit,
                                                                                     desiredUnit );

        // Errors to translate into exceptions?
        RescalingHelper.checkForRescalingErrorsAndThrowExceptionIfRequired( events, timeSeries.getMetadata() );

        return Collections.unmodifiableList( events );
    }

    /**
     * Returns an empty list of {@link EvaluationStatusMessage} if the inputs can produce an upscaled value, otherwise
     * one or more {@link EvaluationStatusMessage} that explain why this is not possible.
     *
     * @param events the events
     * @param endsAt the end of the interval to aggregate
     * @param desiredTimeScale the period over which to upscale
     * @param lenient is true to upscale irregularly spaced data (e.g., due to missing values)
     * @return a list of scale validation events, empty if upscaling is possible
     * @throws NullPointerException if any input is null
     */

    private static <T> GroupRescalingStatus checkThatUpscalingIsPossible( SortedSet<Event<T>> events,
                                                                          Instant endsAt,
                                                                          TimeScaleOuter desiredTimeScale,
                                                                          boolean lenient )
    {
        Objects.requireNonNull( events );
        Objects.requireNonNull( endsAt );
        Objects.requireNonNull( desiredTimeScale );

        String leniencyStatus = RescalingHelper.getLeniencyStatusString( lenient );

        if ( events.size() < 2 )
        {
            String message = WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF + events.size()
                             + EVENTS_TO_AN_EVALUATION_TIME_SCALE_OF
                             + desiredTimeScale
                             + ENDING_AT
                             + endsAt
                             + DISCOVERED_FEWER_THAN_TWO_EVENTS_IN_THE_COLLECTION;

            List<EvaluationStatusMessage> validationEvents =
                    List.of( EvaluationStatusMessage.debug( EvaluationStage.RESCALING, message ) );
            return new GroupRescalingStatus( lenient, validationEvents );
        }

        // Unpack the event times
        SortedSet<Instant> times =
                events.stream()
                      .map( Event::getTime )
                      .collect( Collectors.toCollection( TreeSet::new ) );

        // Add bookends to the list of times to check for even spacing
        RescalingHelper.addBookendsToCheckForEvenSpacing( times, desiredTimeScale, endsAt );

        // Check for even spacing if there are two or more gaps
        if ( times.size() > 2 )
        {
            String message = WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF + events.size()
                             + EVENTS_TO_AN_EVALUATION_TIME_SCALE_OF
                             + desiredTimeScale
                             + ENDING_AT
                             + endsAt
                             + DISCOVERED_THAT_THE_VALUES_WERE_NOT_EVENLY_SPACED_WITHIN_THE_PERIOD_IDENTIFIED
                             + THESE_INTERVALS_BEFORE_STOPPING;

            Instant last = null;
            Duration lastPeriod = null;

            for ( Instant next : times )
            {
                if ( Objects.nonNull( lastPeriod ) )
                {
                    Duration nextPeriod = Duration.between( last, next );

                    if ( !Objects.equals( lastPeriod, nextPeriod ) )
                    {
                        EvaluationStatusMessage nextEvent =
                                EvaluationStatusMessage.debug( EvaluationStage.RESCALING,
                                                               message + Set.of( lastPeriod, nextPeriod )
                                                               + leniencyStatus );
                        List<EvaluationStatusMessage> nextEventList = List.of( nextEvent );

                        // If lenient, the group can be rescaled
                        return new GroupRescalingStatus( lenient, nextEventList );
                    }
                }

                if ( Objects.nonNull( last ) )
                {
                    lastPeriod = Duration.between( last, next );
                }

                last = next;
            }
        }

        return DEFAULT_GROUP_RESCALING_STATUS;
    }

    /**
     * Mutates the input set of times, adding the bookends in order to consider them along with all other values when
     * determining if the values are evenly spaced for upscaling.
     * @param times the times to mutate
     * @param desiredTimeScale the desired time scale
     * @param endsAt the time at which the rescaled value ends
     */

    private static void addBookendsToCheckForEvenSpacing( SortedSet<Instant> times,
                                                          TimeScaleOuter desiredTimeScale,
                                                          Instant endsAt )
    {
        // Add the bookend times
        Duration period = desiredTimeScale.getPeriod();
        if ( !desiredTimeScale.hasMonthDays() )
        {
            Instant startsAt = endsAt.minus( period );
            times.add( startsAt );
            times.add( endsAt );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "When checking for evenly spaced values, added a lower bookend of {} and an upper "
                              + "bookend of {}.",
                              startsAt,
                              endsAt );
            }
        }
        else
        {
            // Because the bookend for month-days is one instant before the end of the day
            Instant endTime = endsAt.plusNanos( 1 );
            times.add( endTime );
            Instant startsAt = null;

            if ( desiredTimeScale.hasPeriod() )
            {
                startsAt = endTime.minus( period );
                times.add( startsAt );
            }

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "When checking for evenly spaced values, added a lower bookend of {} and an upper "
                              + "bookend of {}.",
                              startsAt,
                              endTime );
            }
        }
    }

    /**
     * @param lenient the rescaling is lenient if true, otherwise not lenient
     * @return a status string for messaging regarding the leniency status
     */

    private static String getLeniencyStatusString( boolean lenient )
    {
        String leniencyStatus = THE_LENIENCY_STATUS_WAS + lenient + ".";

        if ( !lenient )
        {
            leniencyStatus += EVALUATION_TIME_SCALE_LENIENT;
        }

        return leniencyStatus;
    }

    /**
     * <p>Validates the request to upscale and throws an exception if the request is invalid. The validation is composed
     * of many separate pieces, each of which produces a {@link EvaluationStatusMessage}. These validation events are 
     * collected together and, if any show {@link StatusLevel#ERROR}, then an exception is thrown with all such cases 
     * identified.
     *
     * <p>TODO: abstract this validation if future implementations rely on some or all of the same constraints.
     *
     * @param existingTimeScale the existing scale
     * @param desiredTimeScale the desired scale
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     */

    private static List<EvaluationStatusMessage> validateForUpscaling( TimeScaleOuter existingTimeScale,
                                                                       TimeScaleOuter desiredTimeScale,
                                                                       Unit<?> existingUnit,
                                                                       Unit<?> desiredUnit )
    {
        // Existing time-scale is unknown
        if ( Objects.isNull( existingTimeScale ) )
        {
            // The desired time scale is not instantaneous, so upscaling may be required, but cannot be determined. 
            // This is an exceptional outcome
            if ( !desiredTimeScale.isInstantaneous() )
            {
                String message = MessageFormat.format( EXISTING_TIME_SCALE_IS_MISSING, desiredTimeScale );

                return List.of( EvaluationStatusMessage.error( EvaluationStage.RESCALING, message ) );
            }

            return List.of( EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                                          EXISTING_TIME_SCALE_IS_MISSING_DESIRED_INSTANTANEOUS ) );
        }

        // The validation events encountered
        List<EvaluationStatusMessage> allEvents = new ArrayList<>();

        // Change of scale required, i.e. not absolutely equal and not instantaneous
        // (which has a more lenient interpretation)
        if ( RescalingHelper.isChangeOfScaleRequired( existingTimeScale, desiredTimeScale ) )
        {
            // Can only validate the period upfront if it is available/fixed
            if ( desiredTimeScale.hasPeriod() )
            {
                // Downscaling not currently allowed
                allEvents.add( RescalingHelper.checkIfDownscalingRequested( existingTimeScale.getPeriod(),
                                                                            desiredTimeScale.getPeriod() ) );

                // The desired time scale period must be an integer multiple of the existing time scale period
                allEvents.add( RescalingHelper.checkIfDesiredPeriodDoesNotCommute( existingTimeScale.getPeriod(),
                                                                                   desiredTimeScale.getPeriod() ) );

                // If the existing and desired periods are the same, the function cannot differ
                allEvents.add( RescalingHelper.checkIfPeriodsMatchAndFunctionsDiffer( existingTimeScale,
                                                                                      desiredTimeScale,
                                                                                      existingUnit,
                                                                                      desiredUnit ) );
            }

            // Check if upscaling is combined with an integration of time-distributed units
            allEvents.add( RescalingHelper.checkIfPerformingTimeIntegrationOfUnits( existingTimeScale,
                                                                                    desiredTimeScale,
                                                                                    existingUnit,
                                                                                    desiredUnit ) );

            // The desired time scale must be a sensible function in the context of rescaling
            allEvents.add( RescalingHelper.checkIfDesiredFunctionIsUnknown( desiredTimeScale ) );

            // If the existing time scale is instantaneous, check whether an accumulation is allowed
            allEvents.add( RescalingHelper.checkIfAccumulatingInstantaneous( existingTimeScale,
                                                                             desiredTimeScale.getFunction(),
                                                                             existingUnit,
                                                                             desiredUnit ) );

            // If the desired function is a total, check whether an accumulation is possible
            allEvents.add( RescalingHelper.checkIfAccumulatingNonAccumulation( existingTimeScale.getFunction(),
                                                                               desiredTimeScale.getFunction(),
                                                                               existingUnit,
                                                                               desiredUnit ) );

        }

        return Collections.unmodifiableList( allEvents );
    }

    /**
     * Returns <code>true</code> if a change of time scale is required, otherwise false. A change of scale is required
     * if the two inputs are different, except in one of the following two cases:
     *
     * <ol>
     * <li>The two inputs are both instantaneous according to {@link TimeScaleOuter#isInstantaneous()}</li>
     * <li>The only difference is the {@link TimeScaleOuter#getFunction()} and the existingTimeScale is
     * {@link TimeScaleFunction#UNKNOWN}</li>
     * </ol>
     *
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @return true if a change of time scale is required, otherwise false
     * @throws NullPointerException if either input is null
     */

    private static boolean isChangeOfScaleRequired( TimeScaleOuter existingTimeScale, TimeScaleOuter desiredTimeScale )
    {
        Objects.requireNonNull( existingTimeScale );

        Objects.requireNonNull( desiredTimeScale );

        boolean different = !existingTimeScale.equals( desiredTimeScale );
        boolean exceptionOne = existingTimeScale.isInstantaneous() && desiredTimeScale.isInstantaneous();
        boolean exceptionTwo = existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
                               && existingTimeScale.getFunction() == TimeScaleFunction.UNKNOWN;

        return different && !exceptionOne && !exceptionTwo;
    }

    /**
     * Checks whether the desiredFunction is {@link TimeScaleFunction#UNKNOWN}, which is not allowed. If so,
     * returns a {@link EvaluationStatusMessage} that is {@link StatusLevel#ERROR}, otherwise {@link StatusLevel#INFO}.
     *
     * @param desiredScale the desired scale
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfDesiredFunctionIsUnknown( TimeScaleOuter desiredScale )
    {
        if ( desiredScale.getFunction() == TimeScaleFunction.UNKNOWN )
        {
            String message = MessageFormat.format( "The desired time scale is ''{0}'', but the function must be known "
                                                   + "to conduct rescaling.",
                                                   desiredScale );

            return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
        }

        return THE_EVALUATION_SCALE_FUNCTION_IS_NOT_UNKNOWN_AND_IS_THEREFORE_ACCEPTABLE;
    }

    /**
     * Checks whether the existingPeriod is larger than the desiredPeriod, which is not allowed. If so, returns
     * a {@link EvaluationStatusMessage} that is {@link StatusLevel#ERROR}, otherwise {@link StatusLevel#INFO}.
     *
     * @param existingPeriod the existing period
     * @param desiredPeriod the desired period
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfDownscalingRequested( Duration existingPeriod,
                                                                        Duration desiredPeriod )
    {
        if ( existingPeriod.compareTo( desiredPeriod ) > 0 )
        {
            String message = MessageFormat.format( "Downscaling is not supported: the desired time scale of ''{0}'' "
                                                   + "cannot be smaller than the existing time scale of ''{1}''.",
                                                   desiredPeriod,
                                                   existingPeriod );

            return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
        }

        return THE_DATASET_PERIOD_OF_ZERO_IS_NOT_LARGER_THAN_THE_EVALUATION_PERIOD;
    }

    /**
     * Checks whether the desiredPeriod is an integer multiple of the existingPeriod. If not an integer multiple, 
     * returns a {@link EvaluationStatusMessage} that is {@link StatusLevel#ERROR}, otherwise {@link StatusLevel#INFO}.  
     *
     * @param inputPeriod the existing period
     * @param desiredPeriod the desired period 
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfDesiredPeriodDoesNotCommute( Duration inputPeriod,
                                                                               Duration desiredPeriod )
    {
        boolean isOneZero = Duration.ZERO.equals( inputPeriod ) || Duration.ZERO.equals( desiredPeriod );

        // Check at a resolution of milliseconds
        // BigDecimal could be used, but is comparatively expensive and 
        // such precision is unnecessary when the standard time resolution is ms
        long inPeriod = inputPeriod.toMillis();
        long desPeriod = desiredPeriod.toMillis();
        long remainder = desPeriod % inPeriod;

        if ( isOneZero || remainder > 0 )
        {
            String message = MessageFormat.format( "The period of ''{0}'' associated with the evaluation time "
                                                   + "scale is not an integer multiple of the dataset period"
                                                   + ", which is ''{1}''. If the data has multiple time-steps that "
                                                   + "vary by time or feature, it may not be possible to "
                                                   + "achieve the evaluation time scale for all of the data. "
                                                   + "In that case, consider removing the evaluation time "
                                                   + "scale and performing an evaluation at the "
                                                   + "time scale of the data, where possible.",
                                                   desiredPeriod,
                                                   inputPeriod );

            return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
        }

        return THE_EVALUATION_PERIOD_OF_ZERO_IS_AN_INTEGER_MULTIPLE;
    }


    /**
     * <p>Checks whether the {@link TimeScaleOuter#getPeriod()} of the two periods match. Returns a validation event as 
     * follows:
     *
     * <ol>
     * <li>If the {@link TimeScaleOuter#getPeriod()} match and the {@link TimeScaleOuter#getFunction()} do not match and 
     * the existingTimeScale is {@link TimeScaleFunction#UNKNOWN}, returns a {@link EvaluationStatusMessage} that is 
     * a {@link StatusLevel#DEBUG}, which assumes, leniently, that the desiredTimeScale can be achieved.</li>
     * <li>If the {@link TimeScaleOuter#getPeriod()} match and the {@link TimeScaleOuter#getFunction()} do not match and 
     * the existingTimeScale is not a {@link TimeScaleFunction#UNKNOWN}, returns a {@link EvaluationStatusMessage} that is 
     * a {@link StatusLevel#ERROR}.</li>
     * <li>Otherwise, returns a {@link EvaluationStatusMessage} that is a {@link StatusLevel#INFO}.</li>
     * </ol>
     *
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfPeriodsMatchAndFunctionsDiffer( TimeScaleOuter existingTimeScale,
                                                                                  TimeScaleOuter desiredTimeScale,
                                                                                  Unit<?> existingUnit,
                                                                                  Unit<?> desiredUnit )
    {
        if ( existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
             && existingTimeScale.getFunction() != desiredTimeScale.getFunction() )
        {
            // If the existing time scale has an unknown function, potentially warn
            if ( existingTimeScale.getFunction() == TimeScaleFunction.UNKNOWN )
            {
                // Warn if the desired time scale has a different function
                if ( desiredTimeScale.getFunction() != TimeScaleFunction.UNKNOWN )
                {
                    String message = MessageFormat.format( THE_FUNCTION_ASSOCIATED_WITH,
                                                           desiredTimeScale.getFunction(),
                                                           TimeScaleFunction.UNKNOWN,
                                                           desiredTimeScale.getFunction() );

                    return EvaluationStatusMessage.debug( EvaluationStage.RESCALING, message ); // #87288, #93220
                }
            }
            else
            {
                String message = MessageFormat.format( "The period associated with the dataset and evaluation "
                                                       + "time scales is ''{0}'', but the time scale function "
                                                       + "associated with the dataset time scale is ''{1}'', which "
                                                       + "differs from the function associated with the evaluation "
                                                       + "time scale, namely ''{2}''. This is not allowed, in general "
                                                       + "because the function cannot be changed without changing the "
                                                       + "period. If you are attempting to change unit dimensions by "
                                                       + "performing a time integration of the existing measurement "
                                                       + "unit of ''{3}'', then you should instead check that the "
                                                       + "desired measurement unit of ''{4}'' is correct.",
                                                       existingTimeScale.getPeriod(),
                                                       existingTimeScale.getFunction(),
                                                       desiredTimeScale.getFunction(),
                                                       existingUnit,
                                                       desiredUnit );

                return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
            }
        }

        return NO_ATTEMPT_WAS_MADE_TO_CHANGE_THE_TIME_SCALE_FUNCTION;
    }

    /**
     * <p>Checks whether attempting to accumulate a quantity that is instantaneous, which is only allowed when changing
     * from time-distributed units to time-integral units. If allowed, returns a {@link EvaluationStatusMessage} that is 
     * {@link StatusLevel#DEBUG}, otherwise an {@link StatusLevel#ERROR}. 
     *
     * @param existingScale the existing scale and function
     * @param desiredFunction the desired function
     * @param existingUnit the existing measurement units
     * @param desiredUnit the desired measurement units
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfAccumulatingInstantaneous( TimeScaleOuter existingScale,
                                                                             TimeScaleFunction desiredFunction,
                                                                             Unit<?> existingUnit,
                                                                             Unit<?> desiredUnit )
    {
        if ( existingScale.isInstantaneous() && desiredFunction == TimeScaleFunction.TOTAL )
        {
            if ( Objects.nonNull( existingUnit ) && Objects.nonNull( desiredUnit )
                 && Units.isSupportedTimeIntegralConversion( existingUnit, desiredUnit ) )
            {
                String message = MessageFormat.format( "When attempting to accumulate instantaneous values to a "
                                                       + "total, encountered existing units of ''{0}'' with a "
                                                       + "dimension of ''{1}'' and desired units of ''{2}'' with a "
                                                       + "dimension of ''{3}''. Accumulations are allowed because the "
                                                       + "existing dimension is a rate and the required dimension is a "
                                                       + "time-integral.",
                                                       existingUnit,
                                                       existingUnit.getDimension(),
                                                       desiredUnit,
                                                       desiredUnit.getDimension() );

                return EvaluationStatusMessage.debug( EvaluationStage.RESCALING,
                                                      message );
            }

            String message = MessageFormat.format( "Cannot accumulate instantaneous values. Change the dataset "
                                                   + "time scale or change the function associated with the evaluation "
                                                   + "time scale to something other than a ''{0}''. If you are "
                                                   + "attempting to change unit dimensions by performing a time "
                                                   + "integration of the existing measurement unit of ''{1}'', then "
                                                   + "you should instead check that the desired measurement unit of "
                                                   + "''{2}'' is correct.",
                                                   TimeScaleFunction.TOTAL,
                                                   existingUnit,
                                                   desiredUnit );

            return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
        }

        return NOT_ATTEMPTING_TO_ACCUMULATE_AN_INSTANTANEOUS_VALUE;
    }


    /**
     * <p>Checks whether attempting to calculate a desired time scale that requires a time-integration of the 
     * measurement units. In this case, the desired time scale function must be a {@link TimeScaleFunction#MEAN}
     * because the time integration to a {@link TimeScaleFunction#TOTAL} is performed as a follow-up step and this is 
     * only possible if the intermediate step is a {@link TimeScaleFunction#MEAN}. 
     *
     * @param existingScale the existing time scale
     * @param desiredScale the desired time scale
     * @param existingUnit the existing measurement units
     * @param desiredUnit the desired measurement units
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfPerformingTimeIntegrationOfUnits( TimeScaleOuter existingScale,
                                                                                    TimeScaleOuter desiredScale,
                                                                                    Unit<?> existingUnit,
                                                                                    Unit<?> desiredUnit )
    {
        // Performing a time integration of the measurement units, so notify
        if ( Objects.nonNull( existingUnit ) && Objects.nonNull( desiredUnit )
             && Units.isSupportedTimeIntegralConversion( existingUnit, desiredUnit ) )
        {
            // Time scale function is not the expected MEAN, which is an internal error if the time-integration is 
            // supported, as checked above
            if ( desiredScale.getFunction() != TimeScaleFunction.MEAN )
            {
                String message =
                        MessageFormat.format( "When attempting to upscale a time-series from ''{0}'' to ''{1}'', "
                                              + "determined that the existing measurement units of ''{2}'' must "
                                              + "be time integrated to form the desired units of ''{3}''. "
                                              + "However, the time scale function for this intermediate operation was "
                                              + "a ''{4}'', which is not expected in this context. The intermediate "
                                              + "operation should have a time scale function of ''{5}''. Please check "
                                              + "that the desired units are intended.",
                                              existingScale,
                                              desiredScale,
                                              existingUnit,
                                              desiredUnit,
                                              desiredScale.getFunction(),
                                              TimeScaleFunction.MEAN );

                return EvaluationStatusMessage.error( EvaluationStage.RESCALING,
                                                      message );
            }

            String message =
                    MessageFormat.format( "When attempting to upscale a time-series from ''{0}'' to ''{1}'', "
                                          + "determined that the existing measurement units of ''{2}'' can "
                                          + "be time integrated to form the desired units of ''{3}''.",
                                          existingScale,
                                          desiredScale,
                                          existingUnit,
                                          desiredUnit );

            return EvaluationStatusMessage.info( EvaluationStage.RESCALING,
                                                 message );
        }

        String message = "Not attempting to perform a time-integration of the measurement units when conducting "
                         + "upscaling.";

        return EvaluationStatusMessage.info( EvaluationStage.RESCALING, message );
    }

    /**
     * <p>Checks whether attempting to accumulate something that is not already an accumulation.
     *
     * <ol>
     * <li>If the desired function is a {@link TimeScaleFunction#TOTAL} and the existing function is a
     * {@link TimeScaleFunction#UNKNOWN}, returns a {@link EvaluationStatusMessage} that is
     * a {@link StatusLevel#DEBUG}, which assumes, leniently, that the existing function is a 
     * {@link TimeScaleFunction#TOTAL}.</li>
     * <li>If the desired function is a {@link TimeScaleFunction#TOTAL} and the existing function is not
     * {@link TimeScaleFunction#UNKNOWN}, returns a {@link EvaluationStatusMessage} that is
     * a {@link StatusLevel#ERROR}.</li>
     * <li>Otherwise, returns a {@link EvaluationStatusMessage} that is a {@link StatusLevel#INFO}.</li>
     * </ol>
     *
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @return a validation event
     */

    private static EvaluationStatusMessage checkIfAccumulatingNonAccumulation( TimeScaleFunction existingFunction,
                                                                               TimeScaleFunction desiredFunction,
                                                                               Unit<?> existingUnit,
                                                                               Unit<?> desiredUnit )
    {
        if ( desiredFunction == TimeScaleFunction.TOTAL && existingFunction != TimeScaleFunction.TOTAL )
        {
            if ( existingFunction == TimeScaleFunction.UNKNOWN )
            {
                String message =
                        MessageFormat.format( THE_FUNCTION_ASSOCIATED_WITH,
                                              TimeScaleFunction.TOTAL,
                                              TimeScaleFunction.UNKNOWN,
                                              TimeScaleFunction.TOTAL );

                return EvaluationStatusMessage.debug( EvaluationStage.RESCALING, message ); // #87288, #93220
            }
            else
            {
                String message;
                if ( Objects.nonNull( existingUnit ) && Objects.nonNull( desiredUnit ) )
                {
                    message = MessageFormat.format( "Cannot further accumulate values that are not already "
                                                    + "accumulations. The function associated with the dataset time "
                                                    + "scale must be a ''{0}'', rather than a ''{1}'', or the function "
                                                    + "associated with the evaluation time scale must be changed. If "
                                                    + "you are attempting to change unit dimensions by performing a "
                                                    + "time integration of the existing measurement unit of ''{2}'', "
                                                    + "then you should instead check that the desired measurement unit "
                                                    + "of ''{3}'' is correct.",
                                                    TimeScaleFunction.TOTAL,
                                                    existingFunction,
                                                    existingUnit,
                                                    desiredUnit );

                }
                else
                {
                    message = MessageFormat.format( "Cannot further accumulate values that are not already "
                                                    + "accumulations. The function associated with the dataset time "
                                                    + "scale must be a ''{0}'', rather than a ''{1}'', or the function "
                                                    + "associated with the evaluation time scale must be changed. If "
                                                    + "you are attempting to change unit dimensions by performing a "
                                                    + "time integration of the existing measurement unit, then you "
                                                    + "should instead check that the desired measurement unit is "
                                                    + "correct.",
                                                    TimeScaleFunction.TOTAL,
                                                    existingFunction,
                                                    existingUnit,
                                                    desiredUnit );

                }
                return EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
            }
        }

        return DID_NOT_DETECT_AN_ATTEMPT_TO_ACCUMULATE;
    }

    /**
     * A smaller class that wraps a collection of {@link EvaluationStatusMessage} as they relate to a particular group of
     * time-series events to rescale, plus a flag that indicates whether that group of time-series events can be
     * rescaled. A time-series may compose several groups of events to rescale and this class allows for the status to
     * be tracked at the finest possible level, i.e., of one group of events to rescale.
     *
     * @author James Brown
     * @param canRescale  Is {@code true} if the group can be rescaled, otherwise {@code false}.
     * @param events  The scale validation events.
     */
    private record GroupRescalingStatus( boolean canRescale, List<EvaluationStatusMessage> events ) {}

    /**
     * Do not construct.
     */

    private RescalingHelper()
    {
    }
}
