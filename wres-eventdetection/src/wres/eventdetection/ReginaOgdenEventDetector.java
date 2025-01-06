package wres.eventdetection;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EventDetectionParameters;
import wres.config.yaml.components.EventDetectionParametersBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;

/**
 * An implementation of the method described in <a href="https://onlinelibrary.wiley.com/doi/10.1002/hyp.14405">
 * Regina and Ogden (2021)</a>. Ported from the original Python code, available here:
 * <a href="https://github.com/NOAA-OWP/hydrotools/blob/main/python/events/src/hydrotools/events/event_detection/decomposition.py">HydroTools</a>.
 *
 * @author Jason Regina
 * @author James Brown
 */
public class ReginaOgdenEventDetector implements EventDetector
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ReginaOgdenEventDetector.class );

    /** Median calculator. */
    private static final Median MEDIAN = new Median().withNaNStrategy( NaNStrategy.REMOVED );

    /** The detection parameters. */
    private final EventDetectionParameters parameters;

    @Override
    public Set<TimeWindowOuter> detect( TimeSeries<Double> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        LOGGER.debug( "Performing event detection using the Regina-Ogden method, which is described here: "
                      + "https://doi.org/10.1002/hyp.14405" );

        if ( timeSeries.getEvents()
                       .size() < 2 )
        {
            LOGGER.debug( "Cannot detect events in a time-series that contains fewer than two event values: {}.",
                          timeSeries.getEvents()
                                    .size() );

            return Set.of();
        }

        return this.detectEvents( timeSeries,
                                  this.getParameters() );
    }

    /**
     * Creates an instance.
     *
     * @param parameters the event detection parameters
     * @return a detector instance
     * @throws NullPointerException if the parameters are null
     * @throws IllegalArgumentException if the parameters are invalid
     */

    static ReginaOgdenEventDetector of( EventDetectionParameters parameters )
    {
        return new ReginaOgdenEventDetector( parameters );
    }

    /**
     * @return the event detection parameters
     */
    private EventDetectionParameters getParameters()
    {
        return this.parameters;
    }

    /**
     * Performs event detection with the supplied parameter. Equivalent to the <code>list_events</code> method in the
     * original codebase. Models the trend in a time series by taking the maximum of two rolling minimum filters
     * applied in a forward and backward fashion. Remove the trend and residual components. The method aims to produce
     * a detrended time series with a median of 0.0. It assumes any residual components less than twice the detrended
     * median are random noise.
     *
     * @param timeSeries the time-series
     * @param parameters the event detection parameters
     * @return the events
     */

    private Set<TimeWindowOuter> detectEvents( TimeSeries<Double> timeSeries,
                                               EventDetectionParameters parameters )
    {
        parameters = this.setDefaultParameterValues( parameters, timeSeries );

        // Smooth the time-series
        TimeSeries<Double> smoothed = this.exponentialMovingAverage( timeSeries, parameters.halfLife() );

        // Detrend the time-series
        TimeSeries<Double> detrended = this.detrend( smoothed, parameters.halfLife(), parameters.windowSize() );

        // Mark the events based on the smoothed, detrended flow
        TimeSeries<Boolean> mapped = TimeSeriesSlicer.transform( detrended,
                                                                 t -> t > 0.0,
                                                                 m -> m );

        // Filter events by duration
        TimeSeries<Boolean> filtered = this.filterEventsByDuration( mapped, parameters.minimumEventDuration() );

        // Get the provisional events
        Set<TimeWindowOuter> provisional = this.getEventBoundaries( filtered );

        // Refine the events
        return this.refineEvents( detrended, provisional, parameters.startRadius() );
    }

    /**
     * Sets the default parameter values, inspecting the timeseries where useful.
     * @param parameters parameters
     * @param timeSeries the time-series
     */
    private EventDetectionParameters setDefaultParameterValues( EventDetectionParameters parameters,
                                                                TimeSeries<Double> timeSeries )
    {
        EventDetectionParametersBuilder builder = EventDetectionParametersBuilder.builder( parameters );
        if ( Objects.isNull( builder.minimumEventDuration() )
             && Objects.nonNull( builder.halfLife() ) )
        {
            LOGGER.debug( "When setting the event detection parameters, discovered that the half-life was defined and "
                          + "the minimum event duration was undefined. Setting the minimum event duration to the "
                          + "half-life of {}.", builder.halfLife() );
            builder.minimumEventDuration( builder.halfLife() );
        }

        if ( Objects.isNull( builder.startRadius() ) )
        {
            builder.startRadius( Duration.ZERO );
        }

        if ( Objects.isNull( builder.minimumEventDuration() ) )
        {
            builder.minimumEventDuration( Duration.ZERO );
        }

        this.setSeriesSpecificParameterDefaults( builder, timeSeries, parameters );

        return builder.build();
    }

    /**
     * Sets the time-series specific event detection parameter estimates.
     *
     * @param builder the builder
     * @param timeSeries the time-series
     * @param parameters the declared parameters
     */
    private void setSeriesSpecificParameterDefaults( EventDetectionParametersBuilder builder,
                                                     TimeSeries<Double> timeSeries,
                                                     EventDetectionParameters parameters )
    {
        // Calculate the series-specific parameter defaults
        if ( Objects.isNull( builder.halfLife() )
             || Objects.isNull( builder.windowSize() ) )
        {
            Duration averageTimestep = this.getAverageTimestep( timeSeries );

            if ( Objects.isNull( builder.halfLife() ) )
            {
                if ( Objects.nonNull( builder.windowSize() ) )
                {
                    builder.halfLife( builder.windowSize()
                                             .dividedBy( 20 ) );
                    LOGGER.debug( "When performing event detection with the Regina-Ogden method, the half-life was "
                                  + "undefined. However, the window size was defined. The default half life is {}, "
                                  + "which is one twentieth of the window size. This default may not be appropriate and "
                                  + "it is strongly recommended that you set the half life explicitly using the "
                                  + "'half_life' parameter.",
                                  builder.halfLife() );
                }
                else
                {
                    builder.halfLife( averageTimestep.multipliedBy( 20 ) );
                    LOGGER.debug( "When performing event detection with the Regina-Ogden method, the half life was "
                                  + "undefined. The default half life is {}, which is twenty times the modal time-step "
                                  + "associated with the time-series used for event detection. This default may not be "
                                  + "appropriate and it is strongly recommended that you set the half life explicitly "
                                  + "using the 'half_life' parameter.",
                                  builder.halfLife() );
                }
            }

            if ( Objects.isNull( builder.windowSize() ) )
            {
                // Check original halflife, not current estimate
                if ( Objects.nonNull( parameters.halfLife() ) )
                {
                    builder.windowSize( parameters.halfLife()
                                                  .multipliedBy( 20 ) );
                    LOGGER.debug( "When performing event detection with the Regina-Ogden method, the window size for "
                                  + "smoothing and detecting trends was undefined. However, the half life was defined. "
                                  + "The default window size is {}, which is twenty times the half-life. This default "
                                  + "may not be appropriate and it is strongly recommended that you set the window size "
                                  + "explicitly using the 'window_size' parameter.",
                                  builder.windowSize() );
                }
                else
                {
                    builder.windowSize( averageTimestep.multipliedBy( 200 ) );
                    LOGGER.debug( "When performing event detection with the Regina-Ogden method, the window size for "
                                  + "smoothing and detecting trends was undefined. The default window size is {}, which "
                                  + "is two hundred times the modal time-step associated with the time-series used for "
                                  + "event detection. This default may not be appropriate and it is strongly "
                                  + "recommended that you set the window size explicitly using the 'window_size' "
                                  + "parameter.",
                                  builder.windowSize() );
                }
            }
        }
    }

    /**
     * Derives the event boundaries from the supplied dichotomous events.
     *
     * @param dichotomousEvents the event markers
     * @return the event boundaries
     * @throws NullPointerException if the input is null
     */
    private Set<TimeWindowOuter> getEventBoundaries( TimeSeries<Boolean> dichotomousEvents )
    {
        Objects.requireNonNull( dichotomousEvents );

        // Short circuit
        if ( dichotomousEvents.getEvents()
                              .isEmpty() )
        {
            return Set.of();
        }

        Set<TimeWindowOuter> timeWindows = new TreeSet<>();

        TimeWindow.Builder currentEvent = null;

        Event<Boolean> lastEvent = null;
        for ( Event<Boolean> nextEvent : dichotomousEvents.getEvents() )
        {
            if ( Boolean.TRUE.equals( nextEvent.getValue() ) )
            {
                if ( Objects.isNull( currentEvent ) )
                {
                    Timestamp nextTime = MessageFactory.getTimestamp( nextEvent.getTime() );
                    currentEvent = MessageFactory.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( nextTime );
                }
            }
            else
            {
                if ( Objects.nonNull( currentEvent ) )
                {
                    Timestamp nextTime = MessageFactory.getTimestamp( lastEvent.getTime() );
                    currentEvent.setLatestValidTime( nextTime );
                    TimeWindow timeWindow = currentEvent.build();
                    TimeWindowOuter wrapped = TimeWindowOuter.of( timeWindow );
                    timeWindows.add( wrapped );
                    currentEvent = null;
                }
            }

            lastEvent = nextEvent;
        }

        // Mop up an ongoing event
        if ( Objects.nonNull( currentEvent ) )
        {
            Timestamp nextTime = MessageFactory.getTimestamp( lastEvent.getTime() );
            currentEvent.setLatestValidTime( nextTime );
            TimeWindow timeWindow = currentEvent.build();
            TimeWindowOuter wrapped = TimeWindowOuter.of( timeWindow );
            timeWindows.add( wrapped );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * Removes the trend component from the prescribed time-series.
     *
     * @param timeSeries the time-series.
     * @param halfLife the half-life
     * @param smoothingWindow the smoothing window duration
     * @return the detrended series
     * @throws EventDetectionException if the smoothing window spans fewer than two time-steps, on average
     */
    private TimeSeries<Double> detrend( TimeSeries<Double> timeSeries, Duration halfLife, Duration smoothingWindow )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( halfLife );

        double[] eventValues = timeSeries.getEvents()
                                         .stream()
                                         .mapToDouble( Event::getValue )
                                         .toArray();

        // Calculate the window size in integer steps
        long windowSize = this.getWindowSizeFromDuration( timeSeries, smoothingWindow );

        if ( windowSize < 2 )
        {
            throw new EventDetectionException( "The window size for event detection must be at least two time-steps, "
                                               + "as this is used to determine the trend in the time-series data. "
                                               + "However, the smoothing duration of "
                                               + parameters.windowSize()
                                               + " spans only "
                                               + windowSize
                                               + " time-steps, on average, which is insufficient. Please increase the "
                                               + "window size for smoothing and try again." );
        }

        double[] trend = this.rollingMinimum( eventValues, windowSize );

        double[] detrended = new double[eventValues.length];

        // Subtract the trend and remove the residuals
        for ( int i = 0; i < detrended.length; i++ )
        {
            detrended[i] = eventValues[i] - trend[i];
        }

        double residual = MEDIAN.evaluate( detrended ) * 2.0;

        SortedSet<Event<Double>> detrendedEvents = new TreeSet<>();
        Iterator<Event<Double>> iterator = timeSeries.getEvents()
                                                     .iterator();
        int current = 0;
        while ( iterator.hasNext() )
        {
            Event<Double> nextValue = iterator.next();

            double value = Math.max( 0.0, detrended[current] - residual );

            Event<Double> detrendedEvent = Event.of( nextValue.getTime(), value );
            detrendedEvents.add( detrendedEvent );
            current++;
        }

        return TimeSeries.of( timeSeries.getMetadata(), detrendedEvents );
    }

    /**
     * Creates an exponential moving average of a time-series, handling missing values.
     *
     * @param timeSeries the time-series
     * @param halfLife the half life
     * @return the smoothed series
     */
    private TimeSeries<Double> exponentialMovingAverage( TimeSeries<Double> timeSeries,
                                                         Duration halfLife )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( halfLife );

        if ( timeSeries.getEvents()
                       .size() < 2 )
        {
            LOGGER.debug( "Cannot exponentially smooth a time-series with fewer than two values: {}.", timeSeries );
            return timeSeries;
        }

        Iterator<Event<Double>> iterator = timeSeries.getEvents()
                                                     .iterator();

        Event<Double> firstEvent = iterator.next();
        double lastValue = firstEvent.getValue();

        double weight = 1.0;

        SortedSet<Event<Double>> smoothed = new TreeSet<>();
        smoothed.add( firstEvent ); // Initialize with first value
        Instant lastTime = firstEvent.getTime();
        long halfLifeMillis = halfLife.toMillis();
        while ( iterator.hasNext() )
        {
            Event<Double> currentValue = iterator.next();

            // Calculate alpha based on the current timestep
            Duration timestep = Duration.between( lastTime, currentValue.getTime() );
            long timestepMillis = timestep.toMillis();

            double alpha = 1.0 - Math.exp( -Math.log( 2 ) / halfLifeMillis * timestepMillis );

            // Current value is missing
            if ( Double.isNaN( currentValue.getValue() ) )
            {
                // First event, then insert NaN
                if ( currentValue == firstEvent )
                {
                    Event<Double> smoothedEvent = Event.of( currentValue.getTime(), Double.NaN );
                    smoothed.add( smoothedEvent );
                }
                // Carry forward previous value
                else
                {
                    Event<Double> smoothedEvent = Event.of( currentValue.getTime(), lastValue );
                    smoothed.add( smoothedEvent );
                }

                // Decay the weight
                weight = weight * ( 1.0 - alpha );
            }
            // Current value is present
            else
            {
                // Last value is missing
                if ( Double.isNaN( lastValue ) )
                {
                    lastValue = currentValue.getValue();
                }
                // Current and last value are both present
                else
                {
                    double smoothedValue = alpha * currentValue.getValue()
                                           + ( 1.0 - alpha ) * lastValue * weight;
                    lastValue = smoothedValue;
                    Event<Double> smoothedEvent = Event.of( currentValue.getTime(), smoothedValue );
                    smoothed.add( smoothedEvent );
                }
                weight = weight * ( 1.0 - alpha ) + alpha;
            }

            lastTime = currentValue.getTime();
        }

        return TimeSeries.of( timeSeries.getMetadata(), smoothed );
    }

    /**
     * Calculates the average duration between time-steps and returns the whole number of times this average fits inside
     * the smoothing window.
     *
     * @param timeSeries the time-series to inspect
     * @param smoothingWindow the smoothing window duration
     */

    private long getWindowSizeFromDuration( TimeSeries<Double> timeSeries, Duration smoothingWindow )
    {
        Duration averageTimestep = this.getAverageTimestep( timeSeries );

        return smoothingWindow.dividedBy( averageTimestep );
    }

    /**
     * Calculates the modal time-step in the series.
     *
     * @param timeSeries the time-series to inspect
     */

    private Duration getAverageTimestep( TimeSeries<Double> timeSeries )
    {

        List<Duration> durations = new ArrayList<>();
        Instant lastTime = null;
        for ( Event<Double> next : timeSeries.getEvents() )
        {
            Instant nextTime = next.getTime();
            if ( Objects.nonNull( lastTime ) )
            {
                Duration between = Duration.between( lastTime, nextTime );
                durations.add( between );
            }
            lastTime = nextTime;
        }

        return durations.stream()
                        .collect( Collectors.groupingBy( Function.identity(),
                                                         Collectors.counting() ) )
                        .entrySet()
                        .stream()
                        .max( Map.Entry.comparingByValue() )
                        .map( Map.Entry::getKey )
                        .orElseThrow( () -> new EventDetectionException( "Insufficient data to calculate the modal "
                                                                         + "timestep for event detection. The "
                                                                         + "time-series had the following metadata: "
                                                                         + timeSeries.getMetadata() ) );
    }

    /**
     * Calculates a rolling minimum.
     *
     * @param values the time-series values in time order
     * @param windowSize the window size
     * @return the rolling minimum
     */
    private double[] rollingMinimum( double[] values, long windowSize )
    {
        // Forward pass
        double[] forwardFiltered = this.rollingMinimumInner( values, windowSize );

        // Reverse pass (flip the time series and values)
        double[] reversedValues = new double[forwardFiltered.length];
        System.arraycopy( values, 0, reversedValues, 0, values.length );
        ArrayUtils.reverse( reversedValues );

        // Backward pass on reversed data
        double[] backwardFiltered = this.rollingMinimumInner( reversedValues, windowSize );

        // Reverse the backward result to align with the original time series
        ArrayUtils.reverse( backwardFiltered );

        // Combine forward and backward passes by taking the maximum
        double[] finalResult = new double[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            finalResult[i] = Math.max( forwardFiltered[i], backwardFiltered[i] );
        }

        return finalResult;
    }

    /**
     * Calculates a rolling minimum value from the input.
     * @param values the values
     * @param windowSize the window size
     * @return the rolling minimum
     */
    private double[] rollingMinimumInner( double[] values, long windowSize )
    {
        double[] result = new double[values.length];
        Arrays.fill( result, Double.NaN );

        for ( int i = 0; i < values.length; i++ )
        {
            long start = Math.max( 0, ( i + 1 ) - windowSize );
            double min = values[i];

            for ( int j = i; j >= start; j-- )
            {
                min = Math.min( min, values[j] );
            }
            result[i] = min;
        }
        return result;
    }

    /**
     * Finds the time associated with the minimum value in the series using a search window.
     * @param origin the origin
     * @param radius the search radius
     * @param timeSeries the time-series
     * @return the time of the local minimum
     */
    private Instant findLocalMinimum( Instant origin,
                                      Duration radius,
                                      TimeSeries<Double> timeSeries )
    {
        Objects.requireNonNull( origin );
        Objects.requireNonNull( radius );
        Objects.requireNonNull( timeSeries );

        // Convert radius to duration and subtract one instant from the left bound because the snip method is
        // right-closed
        Instant left = origin.minus( radius )
                             .minus( Duration.ofNanos( 1 ) );
        Instant right = origin.plus( radius );

        TimeWindow window = MessageFactory.getTimeWindow( left, right );
        TimeWindowOuter wrapped = TimeWindowOuter.of( window );

        // Snip the series to the bounds
        TimeSeries<Double> snipped = TimeSeriesSlicer.snip( timeSeries, wrapped );

        if ( snipped.getEvents()
                    .isEmpty() )
        {
            throw new IllegalArgumentException( "Could not find any time-series values within the search window: "
                                                + wrapped
                                                + "." );
        }

        // Sort the events by value
        SortedSet<Event<Double>> sorted = new TreeSet<>( Comparator.comparingDouble( Event::getValue ) );
        sorted.addAll( snipped.getEvents() );

        return sorted.iterator()
                     .next()
                     .getTime();
    }

    /**
     * Retains only those events that span a minimum duration.
     *
     * @param dichotomousEvents the time-series of event markers
     * @param minimumEventDuration the minimum event duration
     * @return the adjusted series with only those events included that span the minimum duration
     * @throws NullPointerException if any input is null
     */
    private TimeSeries<Boolean> filterEventsByDuration( TimeSeries<Boolean> dichotomousEvents,
                                                        Duration minimumEventDuration )
    {
        Objects.requireNonNull( dichotomousEvents );
        Objects.requireNonNull( minimumEventDuration );

        if ( minimumEventDuration == Duration.ZERO )
        {
            LOGGER.debug( "Events were not filtered by duration because the minimum duration was {}.", Duration.ZERO );

            return dichotomousEvents;
        }

        // Create a series with only events
        TimeSeries<Boolean> eventSeries = TimeSeriesSlicer.filterByEvent( dichotomousEvents, Event::getValue );

        // Short-circuit
        if ( eventSeries.getEvents()
                        .isEmpty() )
        {
            LOGGER.debug( "Events were not filtered by duration because no events were detected." );

            return dichotomousEvents;
        }

        // Determine whether the events meet the minimum span
        SortedSet<Event<Boolean>> adjustedEvents = new TreeSet<>();
        Iterator<Event<Boolean>> eventIterator = dichotomousEvents.getEvents()
                                                                  .iterator();

        // If the timescale period is non-instantaneous, then add to the event duration
        Duration scaleAdjustment = this.getTimeScaleAdjustment( dichotomousEvents.getTimeScale() );

        while ( eventIterator.hasNext() )
        {
            Event<Boolean> start = eventIterator.next();

            // Event is beginning, find the end and the span
            if ( Boolean.TRUE.equals( start.getValue() ) )
            {
                LOGGER.debug( "Event started at {}", start );

                SortedSet<Event<Boolean>> gathered = new TreeSet<>();
                gathered.add( start );
                Event<Boolean> last = start;
                while ( eventIterator.hasNext() )  // NOSONAR only one conceptual break when event ends
                {
                    Event<Boolean> next = eventIterator.next();

                    // Event is formally ending
                    if ( Boolean.FALSE.equals( next.getValue() ) )
                    {
                        SortedSet<Event<Boolean>> validEvents = this.getValidEvents( start.getTime(),
                                                                                     last.getTime(),
                                                                                     scaleAdjustment,
                                                                                     minimumEventDuration,
                                                                                     gathered );
                        // Add the new state for all values within the event period
                        adjustedEvents.addAll( validEvents );

                        // Add the current non-event
                        adjustedEvents.add( next );

                        break;
                    }
                    // Ending because no more values, so next is last
                    else if ( !eventIterator.hasNext() )
                    {
                        SortedSet<Event<Boolean>> validEvents = this.getValidEvents( start.getTime(),
                                                                                     next.getTime(),
                                                                                     scaleAdjustment,
                                                                                     minimumEventDuration,
                                                                                     gathered );
                        adjustedEvents.addAll( validEvents );

                        // Add the current event
                        adjustedEvents.add( next );

                        break;
                    }
                    else
                    {
                        gathered.add( next );
                    }

                    last = next;
                }
            }
            else
            {
                adjustedEvents.add( start );
            }
        }

        return TimeSeries.of( dichotomousEvents.getMetadata(), adjustedEvents );
    }

    /**
     * Marks events that are within the minimum duration as events, otherwise non-events.
     * @param startTime the event start time
     * @param endTime the event end time
     * @param scaleAdjustment the timescale adjustment
     * @param minimumEventDuration the minimum event duration
     * @param gathered the gathered data to adjust
     * @return the valid events
     */
    private SortedSet<Event<Boolean>> getValidEvents( Instant startTime,
                                                      Instant endTime,
                                                      Duration scaleAdjustment,
                                                      Duration minimumEventDuration,
                                                      SortedSet<Event<Boolean>> gathered )
    {
        // Are the gathered values within an event of appropriate span?
        Duration duration = Duration.between( startTime, endTime )
                                    .plus( scaleAdjustment );

        boolean valid = duration.compareTo( minimumEventDuration ) >= 0;

        return gathered.stream()
                       .map( e -> Event.of( e.getTime(), valid ) )
                       .collect( Collectors.toCollection( TreeSet::new ) );
    }

    /**
     * @param timeScale the timescale
     * @return {@link TimeScaleOuter#getPeriod()} if the input is non-instantaneous, else {@link Duration#ZERO}
     */

    private Duration getTimeScaleAdjustment( TimeScaleOuter timeScale )
    {
        // If the timescale period is non-instantaneous, then add to the event duration
        Duration scalePeriod = Duration.ZERO;

        if ( Objects.nonNull( timeScale )
             && !timeScale.isInstantaneous() )
        {
            scalePeriod = timeScale.getPeriod();
        }

        return scalePeriod;
    }

    /**
     * Refines the event points, returning a revised series of events.
     *
     * @param timeSeries the time-series
     * @param events the provisional events to refine
     * @param startRadius the start radius for searching
     * @return the refined event markers
     * @throws NullPointerException if any input is null
     */
    private Set<TimeWindowOuter> refineEvents( TimeSeries<Double> timeSeries,
                                               Set<TimeWindowOuter> events,
                                               Duration startRadius )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( events );
        Objects.requireNonNull( startRadius );

        // Short-circuit
        if ( events.isEmpty() )
        {
            return events;
        }

        Set<TimeWindowOuter> adjustedWindows = new TreeSet<>();
        for ( TimeWindowOuter nextEvent : events )
        {
            Instant eventStart = nextEvent.getEarliestValidTime();
            Instant refinedStart = this.findLocalMinimum( eventStart,
                                                          startRadius,
                                                          timeSeries );

            TimeWindow adjusted = nextEvent.getTimeWindow()
                                           .toBuilder()
                                           .setEarliestValidTime( MessageFactory.getTimestamp( refinedStart ) )
                                           .build();

            adjustedWindows.add( TimeWindowOuter.of( adjusted ) );
        }

        return Collections.unmodifiableSet( adjustedWindows );
    }

    /**
     * Hidden constructor.
     * @param parameters the event detection parameters
     */

    private ReginaOgdenEventDetector( EventDetectionParameters parameters )
    {
        Objects.requireNonNull( parameters );

        if ( !Objects.equals( parameters.halfLife(), parameters.minimumEventDuration() ) )
        {
            LOGGER.debug( "When performing event detection with the Regina-Ogden method, discovered a half-life "
                          + "of {} and a minimum event duration of {}. In general, the minimum event duration should "
                          + "match the half-life to reduce the number of false positives caused by measurement "
                          + "variability.", parameters.halfLife(), parameters.minimumEventDuration() );
        }

        if ( Objects.nonNull( parameters.minimumEventDuration() )
             && parameters.minimumEventDuration()
                          .isNegative() )
        {
            throw new IllegalArgumentException( "The minimum event duration for event detection cannot be negative: "
                                                + parameters.minimumEventDuration() );
        }

        if ( Objects.nonNull( parameters.startRadius() )
             && parameters.startRadius()
                          .isNegative() )
        {
            throw new IllegalArgumentException( "The start radius for event detection cannot be a negative duration: "
                                                + parameters.startRadius() );
        }

        this.parameters = parameters;
    }
}
