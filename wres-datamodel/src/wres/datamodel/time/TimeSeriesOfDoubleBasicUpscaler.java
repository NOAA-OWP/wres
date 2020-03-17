package wres.datamodel.time;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.ScaleValidationEvent;
import wres.datamodel.scale.ScaleValidationEvent.EventType;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Double} 
 * values. An upscaled value is produced from a collection of values that fall within an interval that ends at a 
 * prescribed time. The interval has the same width as the period associated with the desired {@link TimeScale}. If 
 * the events are not evenly spaced within the interval, that interval is skipped and logged. If any event value is 
 * non-finite, then the upscaled event value is {@link MissingValues#DOUBLE}. The interval is right-closed, 
 * i.e. <code>(end-period,end]</code>. Thus, for example, when upscaling a sequence of instantaneous values 
 * (0Z,6Z,12Z,18Z,0Z] to form an average that ends at 0Z and spans a period of PT24H, the four-point average is taken 
 * for the values at 6Z, 12Z, 18Z and 0Z and not the five-point average. Indeed, if these values represented an average 
 * over PT1H, rather than instantaneous values, then the five-point average would consider a PT25H period.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesOfDoubleBasicUpscaler implements TimeSeriesUpscaler<Double>
{

    /**
     * Validation events that occur frequently. These typically correspond to happy paths or warnings, which do not
     * stop the application and are, therefore, hit frequently.
     */

    private static final ScaleValidationEvent DID_NOT_DETECT_AN_ATTEMPT_TO_ACCUMULATE =
            ScaleValidationEvent.pass( "Did not detect an attempt to accumulate "
                                       + "something that is not an accumulation." );

    private static final ScaleValidationEvent NOT_ATTEMPTING_TO_ACCUMULATE_AN_INSTANTANEOUS_VALUE =
            ScaleValidationEvent.pass( "Not attempting to accumulate an instantaneous value." );

    private static final ScaleValidationEvent NO_ATTEMPT_WAS_MADE_TO_CHANGE_THE_TIME_SCALE_FUNCTION =
            ScaleValidationEvent.pass( "No attempt was made to change the time scale function without "
                                       + "also changing the period." );

    private static final ScaleValidationEvent THE_DESIRED_PERIOD_OF_ZERO_IS_AN_INTEGER_MULTIPLE =
            ScaleValidationEvent.pass( "The desired period is an integer multiple of the existing "
                                       + "period and is, therefore, acceptable." );

    private static final ScaleValidationEvent THE_EXISTING_PERIOD_OF_ZERO_IS_NOT_LARGER_THAN_THE_DESIRED_PERIOD =
            ScaleValidationEvent.pass( "The existing period is not larger than the desired period and "
                                       + "is, therefore, acceptable." );

    private static final ScaleValidationEvent THE_DESIRED_FUNCTION_IS_NOT_UNKNOWN_AND_IS_THEREFORE_ACCEPTABLE =
            ScaleValidationEvent.pass( "The desired function is not unknown and is, therefore, acceptable." );

    private static final String THE_FUNCTION_ASSOCIATED_WITH = "The function associated with "
                                                               + "the desired time scale is a ''{0}'', "
                                                               + "but the function associated with the "
                                                               + "existing time scale is ''{1}''. "
                                                               + "Assuming that the latter is also a ''{2}''.";

    private static final String EXISTING_TIME_SCALE_IS_MISSING = "While attempting to upscale to a desired time scale "
                                                                 + "of ''{0}'', encountered a time-series whose "
                                                                 + "existing time-scale is undefined. This occurs when "
                                                                 + "the data source fails to identify the existing time "
                                                                 + "scale and the project declaration fails to clarify "
                                                                 + "this information. Please include the existing time "
                                                                 + "scale (existingTimeScale) in the project "
                                                                 + "declaration, otherwise a change of scale is "
                                                                 + "impossible.";

    private static final String EXISTING_TIME_SCALE_IS_MISSING_DESIRED_INSTANTANEOUS = "Encountered a time-series "
                                                                                       + "whose existing time "
                                                                                       + "scale is undefined, but whose "
                                                                                       + "desired time scale is "
                                                                                       + "instantaneous. This occurs "
                                                                                       + "when the data source fails to "
                                                                                       + "identify the existing time "
                                                                                       + "scale and the project "
                                                                                       + "declaration fails to clarify "
                                                                                       + "this information. Consider "
                                                                                       + "including the existing time "
                                                                                       + "scale (existingTimeScale) in "
                                                                                       + "the project declaration. "
                                                                                       + "Assuming that the existing "
                                                                                       + "time scale is instantaneous.";
    
    private static final String THESE_INTERVALS_BEFORE_STOPPING = "these intervals before stopping: ";

    private static final String DISCOVERED_THAT_THE_VALUES_WERE_NOT_EVENLY_SPACED_WITHIN_THE_PERIOD_IDENTIFIED =
            ", discovered that the values were not evenly spaced within the period. Identified ";

    private static final String UPSCALING = "upscaling.";

    private static final String DISCOVERED_FEWER_THAN_TWO_EVENTS_IN_THE_COLLECTION_WHICH_IS_INSUFFICIENT_FOR =
            ", discovered fewer than two events in the collection, which is insufficient for ";

    private static final String ENDING_AT = " ending at ";

    private static final String EVENTS_TO_A_PERIOD_OF = " events to a period of ";

    private static final String WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF =
            "While attemping to upscale a collection of ";    

    private static final String THREE_MEMBER_MESSAGE = "{}{}{}";

    private static final String FIVE_MEMBER_MESSAGE = "{}{}{}{}{}";

    private static final String SEVEN_MEMBER_MESSAGE = "{}{}{}{}{}";

    /**
     * Lenient on values that match the {@link MissingValues.DOUBLE}? TODO: expose this to declaration.
     */

    private static final boolean LENIENT = false;

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesOfDoubleBasicUpscaler.class );

    /**
     * Function that returns a double value or {@link MissingValues.MISSING_DOUBLE} if the 
     * input is not finite. 
     */

    private static final DoubleUnaryOperator RETURN_DOUBLE_OR_MISSING =
            a -> Double.isFinite( a ) ? a : MissingValues.DOUBLE;

    /**
     * Creates an instance.
     * 
     * @return an instance of the upscaler
     */

    public static TimeSeriesOfDoubleBasicUpscaler of()
    {
        return new TimeSeriesOfDoubleBasicUpscaler();
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScale desiredTimeScale )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySet() );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScale desiredTimeScale,
                                                             Set<Instant> endsAt )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( desiredTimeScale );

        Objects.requireNonNull( endsAt );

        // Validate the request
        List<ScaleValidationEvent> validationEvents = this.validate( timeSeries, desiredTimeScale );

        // Empty time-series
        if ( timeSeries.getEvents().isEmpty() )
        {
            LOGGER.trace( THREE_MEMBER_MESSAGE,
                          "No upscaling required for time-series ",
                          timeSeries.hashCode(),
                          ": the time-series contained no events." );

            return RescaledTimeSeriesPlusValidation.of( timeSeries, validationEvents );
        }

        // Existing time scale missing and this was allowed during validation
        if ( !timeSeries.hasTimeScale() )
        {
            LOGGER.trace( FIVE_MEMBER_MESSAGE,
                          "Skipped upscaling time-series ",
                          timeSeries.hashCode(),
                          " to the desired time scale of ",
                          desiredTimeScale,
                          " because the existing time scale was missing. Assuming that the existing and desired scales "
                                            + "are the same." );


            return RescaledTimeSeriesPlusValidation.of( timeSeries, validationEvents );
        }

        // Existing and desired are both instantaneous
        if ( timeSeries.getTimeScale().isInstantaneous() && desiredTimeScale.isInstantaneous() )
        {
            LOGGER.trace( SEVEN_MEMBER_MESSAGE,
                          "Skipped upscaling time-series ",
                          timeSeries.hashCode(),
                          " to the desired time scale of ",
                          desiredTimeScale,
                          " because the existing time scale is ",
                          timeSeries.getTimeScale(),
                          " and both are recognized as instantaneous." );

            return RescaledTimeSeriesPlusValidation.of( timeSeries, validationEvents );
        }

        // If the period is the same, return the existing series with the desired scale
        // The validation of the function happens above. For example, the existing could be UNKNOWN
        if ( desiredTimeScale.getPeriod().equals( timeSeries.getTimeScale().getPeriod() ) )
        {
            LOGGER.trace( SEVEN_MEMBER_MESSAGE,
                          "No upscaling required for time-series ",
                          timeSeries.hashCode(),
                          ": the existing time scale of ",
                          timeSeries.getTimeScale(),
                          " effectively matches the desired time scale of ",
                          desiredTimeScale,
                          "." );

            // Create new series in case the function differs
            TimeSeries<Double> returnMe = new TimeSeriesBuilder<Double>().addEvents( timeSeries.getEvents() )
                                                                         .addReferenceTimes( timeSeries.getReferenceTimes() )
                                                                         .setTimeScale( desiredTimeScale )
                                                                         .build();

            return RescaledTimeSeriesPlusValidation.of( returnMe, validationEvents );
        }

        // No times at which values should end, so start at the beginning
        if ( endsAt.isEmpty() )
        {
            endsAt = this.getEndTimesFromSeries( timeSeries, desiredTimeScale );
        }

        // Group the events according to whether their valid times fall within the desired period that ends at a 
        // particular valid time
        Duration period = desiredTimeScale.getPeriod();

        Map<Instant, SortedSet<Event<Double>>> groups =
                TimeSeriesSlicer.groupEventsByInterval( timeSeries.getEvents(), endsAt, period );

        // Process the groups whose events are evenly-spaced and have no missing values, otherwise skip and log
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        builder.addReferenceTimes( timeSeries.getReferenceTimes() );

        // Acquire the function for the desired scale
        TimeScaleFunction desiredFunction = desiredTimeScale.getFunction();

        ToDoubleFunction<SortedSet<Event<Double>>> upscaler = this.getUpscaler( desiredFunction );

        // Create a mutable copy of the validation events to add more, as needed
        List<ScaleValidationEvent> mutableValidationEvents = new ArrayList<>( validationEvents );
        validationEvents = mutableValidationEvents;

        // Upscale each group
        for ( Map.Entry<Instant, SortedSet<Event<Double>>> nextGroup : groups.entrySet() )
        {
            List<ScaleValidationEvent> validation = this.checkThatUpscalingIsPossible( nextGroup.getValue(),
                                                                                       nextGroup.getKey(),
                                                                                       period );
            validationEvents.addAll( validation );

            // No validation events, upscaling can proceed
            if ( validation.isEmpty() )
            {
                Event<Double> upscaled = Event.of( nextGroup.getKey(), upscaler.applyAsDouble( nextGroup.getValue() ) );

                builder.addEvent( upscaled );
            }
        }

        // Set the upscaled scale
        builder.setTimeScale( desiredTimeScale );

        return RescaledTimeSeriesPlusValidation.of( builder.build(), Collections.unmodifiableList( validationEvents ) );
    }

    /**
     * Inspects a time-series and returns the end times that increment from the first possible time. Since the upscaling
     * interval is right-closed, the inspection begins at one time-step before the first time in the series.
     * 
     * <p>TODO: abstract this validation if other, future, implementations rely on similar inspections.
     * 
     * @return the times at which upscaled intervals should end
     */

    private <T> Set<Instant> getEndTimesFromSeries( TimeSeries<T> timeSeries, TimeScale desiredTimeScale )
    {
        Set<Instant> endsAt = new HashSet<>();

        Instant firstTime = timeSeries.getEvents().first().getTime();
        Instant lastTime = timeSeries.getEvents().last().getTime();
        Duration period = desiredTimeScale.getPeriod();

        Duration timeStep = Duration.ZERO; // The inert amount by which to back-pedal
        if ( timeSeries.getEvents().size() > 1 )
        {
            Iterator<Instant> iterator = timeSeries.getEvents().stream().map( Event::getTime ).iterator();
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

        return Collections.unmodifiableSet( endsAt );
    }

    /**
     * Validates the request to upscale and throws an exception if the request is invalid. 
     * 
     * @param timeSeries the time-series to upscale
     * @param desiredTimeScale the desired scale
     * @throws RescalingException if the input cannot be rescaled to the desired scale
     * @return the validation events
     */

    private <T> List<ScaleValidationEvent> validate( TimeSeries<T> timeSeries, TimeScale desiredTimeScale )
    {
        List<ScaleValidationEvent> events =
                TimeSeriesOfDoubleBasicUpscaler.validateForUpscaling( timeSeries.getTimeScale(), desiredTimeScale );

        // Errors to translate into exceptions?
        List<ScaleValidationEvent> errors = events.stream()
                                                  .filter( a -> a.getEventType() == EventType.ERROR )
                                                  .collect( Collectors.toList() );
        String spacer = "    ";

        if ( !errors.isEmpty() )
        {
            StringJoiner message = new StringJoiner( System.lineSeparator() );
            message.add( "Encountered "
                         + errors.size()
                         + " errors while attempting to upscale time-series "
                         + timeSeries
                         + ": " );

            errors.stream().forEach( e -> message.add( spacer + spacer + e.toString() ) );

            throw new RescalingException( message.toString() );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Returns an empty list of {@link ScaleValidationEvent} if the inputs can produce an upscaled value, otherwise
     * one or more {@link ScaleValidationEvent} that explain why this is not possible.
     * 
     * @param events the events
     * @param endsAt the end of the interval to aggregate, which is used for logging
     * @param the period over which to upscale
     * @return a list of scale validation events, empty if upscaling is possible
     * @throws NullPointerException if any input is null
     */

    private List<ScaleValidationEvent> checkThatUpscalingIsPossible( SortedSet<Event<Double>> events,
                                                                     Instant endsAt,
                                                                     Duration period )
    {
        Objects.requireNonNull( events );
        Objects.requireNonNull( endsAt );
        Objects.requireNonNull( period );

        if ( events.size() < 2 )
        {
            String message = WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF + events.size()
                             + EVENTS_TO_A_PERIOD_OF
                             + period
                             + ENDING_AT
                             + endsAt
                             + DISCOVERED_FEWER_THAN_TWO_EVENTS_IN_THE_COLLECTION_WHICH_IS_INSUFFICIENT_FOR
                             + UPSCALING;

            return List.of( ScaleValidationEvent.debug( message ) );
        }

        // Unpack the event times
        SortedSet<Instant> times =
                events.stream()
                      .map( Event::getTime )
                      .collect( Collectors.toCollection( TreeSet::new ) );

        // Add the lower bound, as the gap between this bound and the 
        // first time should be considered too
        times.add( endsAt.minus( period ) );

        // Check for even spacing if there are two or more gaps
        if ( times.size() > 2 )
        {

            String message = WHILE_ATTEMPING_TO_UPSCALE_A_COLLECTION_OF + events.size()
                             + EVENTS_TO_A_PERIOD_OF
                             + period
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

                        return List.of( ScaleValidationEvent.debug( message + Set.of( lastPeriod, nextPeriod ) ) );
                    }
                }

                if ( Objects.nonNull( last ) )
                {
                    lastPeriod = Duration.between( last, next );
                }

                last = next;
            }
        }

        return List.of();
    }

    /**
     * Returns a function that corresponds to a {@link TimeScaleFunction}, additionally wrapped by 
     * {@link #RETURN_DOUBLE_OR_MISSING} so that missing input produces missing output.
     * 
     * @param timeScaleFunction the nominated function
     * @return a function for upscaling
     * @throws UnsupportedOperationException if the nominated function is not recognized
     */

    private ToDoubleFunction<SortedSet<Event<Double>>> getUpscaler( TimeScaleFunction function )
    {
        return events -> {

            double upscaled;

            SortedSet<Event<Double>> eventsToUse = events;

            if ( TimeSeriesOfDoubleBasicUpscaler.LENIENT )
            {
                eventsToUse = eventsToUse.stream()
                                         .filter( next -> Double.isFinite( next.getValue() ) )
                                         .collect( Collectors.toCollection( TreeSet::new ) );
            }

            switch ( function )
            {
                case MAXIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .max()
                                          .getAsDouble();
                    break;
                case MEAN:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .average()
                                          .getAsDouble();
                    break;
                case MINIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .min()
                                          .getAsDouble();
                    break;
                case TOTAL:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .sum();
                    break;
                default:
                    throw new UnsupportedOperationException( "Could not create an upscaling function for the "
                                                             + "function identifier '"
                                                             + function
                                                             + "'." );

            }

            return RETURN_DOUBLE_OR_MISSING.applyAsDouble( upscaled );
        };
    }

    /**
     * <p>Validates the request to upscale and throws an exception if the request is invalid. The validation is composed
     * of many separate pieces, each of which produces a {@link ScaleValidationEvent}. These validation events are 
     * collected together and, if any show {@link EventType#ERROR}, then an exception is thrown with all such cases 
     * identified.
     * 
     * <p>TODO: abstract this validation if future implementations rely on some or all of the same constraints.
     * 
     * @param existingTimeScale the existing scale
     * @param desiredTimeScale the desired scale
     */

    private static List<ScaleValidationEvent> validateForUpscaling( TimeScale existingTimeScale,
                                                                    TimeScale desiredTimeScale )
    {
        // Existing time-scale is unknown
        if ( Objects.isNull( existingTimeScale ) )
        {
            // The desired time scale is not instantaneous, so upscaling may be required, but cannot be determined. 
            // This is an exceptional outcome
            if ( !desiredTimeScale.isInstantaneous() )
            {
                String message = MessageFormat.format( EXISTING_TIME_SCALE_IS_MISSING, desiredTimeScale );

                return List.of( ScaleValidationEvent.error( message ) );
            }

            return List.of( ScaleValidationEvent.info( EXISTING_TIME_SCALE_IS_MISSING_DESIRED_INSTANTANEOUS ) );
        }

        // The validation events encountered
        List<ScaleValidationEvent> allEvents = new ArrayList<>();

        // Change of scale required, i.e. not absolutely equal and not instantaneous
        // (which has a more lenient interpretation)
        if ( TimeSeriesOfDoubleBasicUpscaler.isChangeOfScaleRequired( existingTimeScale, desiredTimeScale ) )
        {

            // The desired time scale must be a sensible function in the context of rescaling
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfDesiredFunctionIsUnknown( desiredTimeScale.getFunction() ) );

            // Downscaling not currently allowed
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfDownscalingRequested( existingTimeScale.getPeriod(),
                                                                                        desiredTimeScale.getPeriod() ) );

            // The desired time scale period must be an integer multiple of the existing time scale period
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfDesiredPeriodDoesNotCommute( existingTimeScale.getPeriod(),
                                                                                               desiredTimeScale.getPeriod() ) );

            // If the existing and desired periods are the same, the function cannot differ
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfPeriodsMatchAndFunctionsDiffer( existingTimeScale,
                                                                                                  desiredTimeScale ) );

            // If the existing time scale is instantaneous, do not allow accumulations (for now)
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfAccumulatingInstantaneous( existingTimeScale,
                                                                                             desiredTimeScale.getFunction() ) );

            // If the desired function is a total, then the existing function must also be a total
            allEvents.add( TimeSeriesOfDoubleBasicUpscaler.checkIfAccumulatingNonAccumulation( existingTimeScale.getFunction(),
                                                                                               desiredTimeScale.getFunction() ) );

        }

        return Collections.unmodifiableList( allEvents );
    }

    /**
     * Returns <code>true</code> if a change of time scale is required, otherwise false. A change of scale is required
     * if the two inputs are different, except in one of the following two cases:
     *
     * <ol>
     * <li>The two inputs are both instantaneous according to {@link TimeScale#isInstantaneous()}</li>
     * <li>The only difference is the {@link TimeScale#getFunction()} and the existingTimeScale is
     * {@link TimeScaleFunction#UNKNOWN}</li>
     * </ol>
     *
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @return true if a change of time scale is required, otherwise false
     * @throws NullPointerException if either input is null
     */

    private static boolean isChangeOfScaleRequired( TimeScale existingTimeScale, TimeScale desiredTimeScale )
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
     * returns a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}.
     *
     * @param desiredFunction the desired function
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDesiredFunctionIsUnknown( TimeScaleFunction desiredFunction )
    {
        if ( desiredFunction == TimeScaleFunction.UNKNOWN )
        {
            String message = MessageFormat.format( "The desired time scale function is ''{0}''"
                                                   + ": the function must be known to conduct rescaling.",
                                                   TimeScaleFunction.UNKNOWN );

            return ScaleValidationEvent.error( message );
        }

        return THE_DESIRED_FUNCTION_IS_NOT_UNKNOWN_AND_IS_THEREFORE_ACCEPTABLE;
    }

    /**
     * Checks whether the existingPeriod is larger than the desiredPeriod, which is not allowed. If so, returns
     * a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}.
     * 
     * @param existingPeriod the existing period
     * @param desiredPeriod the desired period
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDownscalingRequested( Duration existingPeriod,
                                                                     Duration desiredPeriod )
    {
        if ( existingPeriod.compareTo( desiredPeriod ) > 0 )
        {
            String message = MessageFormat.format( "Downscaling is not supported: the desired time scale of ''{0}'' "
                                                   + "cannot be smaller than the existing time scale of ''{1}''.",
                                                   desiredPeriod,
                                                   existingPeriod );

            return ScaleValidationEvent.error( message );
        }

        return THE_EXISTING_PERIOD_OF_ZERO_IS_NOT_LARGER_THAN_THE_DESIRED_PERIOD;
    }

    /**
     * Checks whether the desiredPeriod is an integer multiple of the existingPeriod. If not an integer multiple, 
     * returns a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}.  
     * 
     * @param existingPeriod the existing period
     * @param desiredPeriod the desired period 
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDesiredPeriodDoesNotCommute( Duration inputPeriod,
                                                                            Duration desiredPeriod )
    {
        boolean isOneZero = Duration.ZERO.equals( inputPeriod ) || Duration.ZERO.equals( desiredPeriod );

        // Check at a resolution of seconds
        // BigDecimal could be used, but is comparatively expensive and 
        // such precision is unnecessary when the standard time resolution is PT1M      
        long inPeriod = inputPeriod.getSeconds();
        long desPeriod = desiredPeriod.getSeconds();
        long remainder = desPeriod % inPeriod;

        if ( isOneZero || remainder > 0 )
        {
            String message = MessageFormat.format( "The desired period of ''{0}''"
                                                   + " is not an integer multiple of the existing period"
                                                   + ", which is ''{1}''. If the data has multiple time-steps that "
                                                   + "vary by time or feature, it may not be possible to "
                                                   + "achieve the desired time scale for all of the data. "
                                                   + "In that case, consider removing the desired time "
                                                   + "scale and performing an evaluation at the "
                                                   + "existing time scale of the data, where possible.",
                                                   desiredPeriod,
                                                   inputPeriod );

            return ScaleValidationEvent.error( message );
        }

        return THE_DESIRED_PERIOD_OF_ZERO_IS_AN_INTEGER_MULTIPLE;
    }


    /**
     * <p>Checks whether the {@link TimeScale#getPeriod()} of the two periods match. Returns a validation event as 
     * follows:
     * 
     * <ol>
     * <li>If the {@link TimeScale#getPeriod()} match and the {@link TimeScale#getFunction()} do not match and the 
     * the existingTimeScale is {@link TimeScaleFunction#UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#WARN}, which assumes, leniently, that the desiredTimeScale can be achieved.</li>
     * <li>If the {@link TimeScale#getPeriod()} match and the {@link TimeScale#getFunction()} do not match and the 
     * the existingTimeScale is not a {@link TimeScaleFunction#UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#ERROR}.</li>
     * <li>Otherwise, returns a {@link ScaleValidationEvent} that is a {@link EventType#PASS}.</li>
     * </ol>
     * 
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @returns a validation event
     */

    private static ScaleValidationEvent checkIfPeriodsMatchAndFunctionsDiffer( TimeScale existingTimeScale,
                                                                               TimeScale desiredTimeScale )
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

                    return ScaleValidationEvent.warn( message );
                }
            }
            else
            {
                String message = MessageFormat.format( "The period associated with the existing and desired "
                                                       + "time scales is ''{0}'', but the time scale function "
                                                       + "associated with the existing time scale is ''{1}'', which "
                                                       + "differs from the function associated with the desired time "
                                                       + "scale, namely ''{2}''. This is not allowed. The function "
                                                       + "cannot be changed without changing the period.",
                                                       existingTimeScale.getPeriod(),
                                                       existingTimeScale.getFunction(),
                                                       desiredTimeScale.getFunction() );

                return ScaleValidationEvent.error( message );
            }
        }

        return NO_ATTEMPT_WAS_MADE_TO_CHANGE_THE_TIME_SCALE_FUNCTION;
    }

    /**
     * <p>Checks whether attempting to accumulate a quantity that is instantaneous, which is not allowed. If so, returns
     * a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}. 
     * 
     * <p>TODO: in principle, this might be supported in future, but involves both an integral
     * estimate and a change in units. For example, if the input is precipitation in mm/s
     * then the total might be estimated as the average over the interval, multiplied by 
     * the number of seconds.
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfAccumulatingInstantaneous( TimeScale existingScale,
                                                                          TimeScaleFunction desiredFunction )
    {
        if ( existingScale.isInstantaneous() && desiredFunction == TimeScaleFunction.TOTAL )
        {
            String message = MessageFormat.format( "Cannot accumulate instantaneous values. Change the existing "
                                                   + "time scale or change the function associated with the desired "
                                                   + "time scale to something other than a ''{0}''.",
                                                   TimeScaleFunction.TOTAL );

            return ScaleValidationEvent.error( message );
        }

        return NOT_ATTEMPTING_TO_ACCUMULATE_AN_INSTANTANEOUS_VALUE;

    }

    /**
     * <p>Checks whether attempting to accumulate something that is not already an accumulation.
     * 
     * <ol>
     * <li>If the desired function is a {@link TimeScaleFunction.TOTAL} and the existing function is a
     * {@link TimeScaleFunction.UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#WARN}, which assumes, leniently, that the existing function is a 
     * {@link TimeScaleFunction.TOTAL}.</li>
     * <li>If the desired function is a {@link TimeScaleFunction.TOTAL} and the existing function is not
     * {@link TimeScaleFunction.UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#ERROR}.</li>
     * <li>Otherwise, returns a {@link ScaleValidationEvent} that is a {@link EventType#PASS}.</li>
     * </ol>
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @returns a validation event
     */

    private static ScaleValidationEvent checkIfAccumulatingNonAccumulation( TimeScaleFunction existingFunction,
                                                                            TimeScaleFunction desiredFunction )
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

                return ScaleValidationEvent.warn( message );
            }
            else
            {
                String message =
                        MessageFormat.format( "Cannot accumulate values that are not already accumulations. The "
                                              + "function associated with the existing time scale must be a ''{0}'', "
                                              + "rather than a ''{1}'', or the function associated with the desired "
                                              + "time scale must be changed.",
                                              TimeScaleFunction.TOTAL,
                                              existingFunction );

                return ScaleValidationEvent.error( message );
            }
        }

        return DID_NOT_DETECT_AN_ATTEMPT_TO_ACCUMULATE;
    }

    /**
     * Hidden constructor.
     */

    private TimeSeriesOfDoubleBasicUpscaler()
    {
    }

}
