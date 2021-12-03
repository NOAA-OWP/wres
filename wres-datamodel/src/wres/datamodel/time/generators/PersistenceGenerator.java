package wres.datamodel.time.generators;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;

/**
 * <p>Generates a persistence time-series from a source of persistence data supplied on construction. The shape of the 
 * persistence series is obtained from a template time-series supplied on demand. Uses the reference time as the anchor
 * datetime for persistence when available, otherwise the valid times.
 * 
 * @author James Brown
 * @param <T> the type of persistence value to generate
 */

public class PersistenceGenerator<T> implements UnaryOperator<TimeSeries<T>>
{

    /**
     * Logging message.
     */

    private static final String WHILE_GENERATING_A_PERSISTENCE_TIME_SERIES_USING_INPUT_SERIES =
            "While generating a persistence time-series using input series {}{}";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PersistenceGenerator.class );

    /**
     * The order of persistence relative to the start of the template series from which the persistence value should 
     * be derived. Order means the number of times prior to the reference time in the template.
     */

    private final int order;

    /**
     * The source data from which the persistence values should be generated.
     */

    private final TimeSeries<T> persistenceSource;

    /**
     * An optional upscaler to use in generating a persistence value from the {@link #persistenceSource}.
     */

    private final TimeSeriesUpscaler<T> upscaler;

    /**
     * An optional constraint on admissible values. If a value is not eligible for persistence, the empty series is
     * returned.
     */

    private final Predicate<T> admissibleValue;

    /**
     * Provides an instance for persistence of order one, i.e., lag-1 persistence.
     * 
     * @param <T> the type of time-series event value
     * @param persistenceSource the persistence data source
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the 
     *            persistenceSource
     * @param admissibleValue an optional constraint on values that should be persisted
     * @return an instance
     * @throws NullPointerException if the persistenceSource is null
     * @throws IllegalArgumentException if the order is negative
     */

    public static <T> PersistenceGenerator<T> of( Supplier<Stream<TimeSeries<T>>> persistenceSource,
                                                  TimeSeriesUpscaler<T> upscaler,
                                                  Predicate<T> admissibleValue )
    {
        return new PersistenceGenerator<>( 1, persistenceSource, upscaler, admissibleValue );
    }

    /**
     * Creates a persistence time-series at a lag supplied on construction using the input time-series as a template.
     * 
     * @param template the template time-series for which persistence values will be generated
     * @return a time-series with the lagged value at every time-step
     * @throws NullPointerException if the input is null
     * @throws TimeSeriesGeneratorException if the persistence value could not be generated
     */

    @Override
    public TimeSeries<T> apply( TimeSeries<T> template )
    {
        Objects.requireNonNull( template );

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        if ( template.getEvents().isEmpty() )
        {
            LOGGER.trace( WHILE_GENERATING_A_PERSISTENCE_TIME_SERIES_USING_INPUT_SERIES,
                          template.hashCode(),
                          ", discovered that the input series had no events (i.e., was empty). Returning the same, "
                                               + "empty, series." );

            return template;
        }

        // No reference times, so not a forecast. Compute persistence with respect to valid time.
        if ( referenceTimes.isEmpty() )
        {
            LOGGER.trace( "While generating a persistence prediction, discovered a time-series without any reference "
                          + "times. Using the valid times instead." );

            return this.getPersistenceForEachValidTime( template );
        }
        else
        {
            LOGGER.trace( "While generating a persistence prediction, discovered a time-series with {} reference times."
                          + " Using the first reference time of {}.",
                          referenceTimes.size(),
                          referenceTimes.entrySet()
                                        .iterator()
                                        .next() );

            return this.getPersistenceForFirstReferenceTime( template );
        }
    }

    /**
     * Returns a persistence time-series with respect to the first reference time in the input.
     * @param template the template time-series
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForFirstReferenceTime( TimeSeries<T> template )
    {
        // Put the persistence values into a list. There are at least N+1 values in the list, established at 
        // construction
        List<Event<T>> eventsToSearch = this.persistenceSource.getEvents()
                                                              .stream()
                                                              .collect( Collectors.toList() );

        TimeSeriesMetadata templateMetadata = template.getMetadata();
        Builder<T> builder = new Builder<>();
        builder.setMetadata( templateMetadata );

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        // Take the first available reference time
        // If multiple reference times are provided in future, adapt
        if ( referenceTimes.size() > 1 && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                          + "input series has multiple reference times. Using the first time of {}.",
                          template.hashCode(),
                          referenceTimes.values()
                                        .iterator()
                                        .next() );
        }

        // Compute the instant at which the persistence value should end
        Instant referenceTime = referenceTimes.values()
                                              .iterator()
                                              .next();

        Event<T> persistenceEvent = this.getNthNearestValueInstant( eventsToSearch, referenceTime, this.order );

        if ( Objects.isNull( persistenceEvent ) )
        {
            LOGGER.trace( "While attempting to generate a persistence value for the reference time of {}, failed to "
                          + "find a corresponding time in the persistence source, which contained {} values.",
                          referenceTime,
                          eventsToSearch.size() );

            return builder.build();
        }

        T persistenceValue = null;
        Instant persistenceEventTime = persistenceEvent.getTime();

        // Upscale? 
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.persistenceSource.getTimeScale() )
             && !desiredTimeScale.equals( this.persistenceSource.getTimeScale() ) )
        {
            SortedSet<Instant> times = new TreeSet<>();
            times.add( persistenceEventTime );

            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template, times );

            if ( !upscaled.getEvents().isEmpty() )
            {
                persistenceValue = upscaled.getEvents()
                                           .first()
                                           .getValue();
            }
        }
        else
        {
            persistenceValue = persistenceEvent.getValue();
        }

        // Persistence value admissible?
        if ( ( Objects.isNull( persistenceValue ) || !this.admissibleValue.test( persistenceValue ) ) )
        {
            int seriesCode = template.hashCode();

            LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                          + "persistent value of {} was inadmissible. Unable to create a persistence time-series "
                          + "from input series {}. Returning the empty time-series instead.",
                          seriesCode,
                          persistenceValue,
                          seriesCode );
        }
        else
        {
            for ( Event<T> next : template.getEvents() )
            {
                Event<T> persistedValue = Event.of( next.getTime(), persistenceValue );
                builder.addEvent( persistedValue );
            }
        }

        return builder.build();
    }

    /**
     * Returns a persistence time-series with respect to each valid time in the input.
     * @param template the template time-series to populate with persistence values
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForEachValidTime( TimeSeries<T> template )
    {
        // Put the persistence values into a list. There are at least N+1 values in the list, established at 
        // construction
        List<Event<T>> eventsToSearch = this.persistenceSource.getEvents()
                                                              .stream()
                                                              .collect( Collectors.toList() );

        // Find a persistence event from the eventsToSearch for each valid time in the template series
        List<Instant> validTimes = template.getEvents()
                                           .stream()
                                           .map( Event::getTime )
                                           .collect( Collectors.toList() );

        List<Event<T>> persistenceEvents = this.getNthNearestValueInstant( eventsToSearch, validTimes, this.order );

        // Upscale? 
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.persistenceSource.getTimeScale() )
             && !desiredTimeScale.equals( this.persistenceSource.getTimeScale() ) )
        {
            SortedSet<Instant> persistenceEventTimes = persistenceEvents.stream()
                                                                        .filter( Objects::nonNull )
                                                                        .map( Event::getTime )
                                                                        .collect( Collectors.toCollection( TreeSet::new ) );

            // This is the upscaled time-series for values that end at the persistence event times. Now need to map
            // these back to the valid times for which persistence events are required
            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template, persistenceEventTimes );

            return this.mapUpscaledEventsToValidTimes( validTimes, persistenceEvents, upscaled );
        }
        else
        {
            TimeSeriesMetadata templateMetadata = template.getMetadata();
            Builder<T> builder = new Builder<>();
            builder.setMetadata( templateMetadata );

            for ( int i = 0; i < validTimes.size(); i++ )
            {
                Event<T> nextEvent = persistenceEvents.get( i );
                Instant nextTime = validTimes.get( i );
                if ( Objects.nonNull( nextEvent ) && this.admissibleValue.test( nextEvent.getValue() ) )
                {
                    Event<T> persistedValue = Event.of( nextTime, nextEvent.getValue() );
                    builder.addEvent( persistedValue );
                }
                else if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Failed to identify a valid persistence value for time {}. Skipping.", nextTime );
                }
            }

            return builder.build();
        }
    }

    /**
     * Returns the persistence time-series upscaled to the desired time scale of the template series at the prescribed
     * end times.
     * @param template the template series
     * @param endsAtSorted the times at which upscaled values should end
     * @return the upscaled series
     */

    private TimeSeries<T> getUpscaledPersistenceSeriesAtTheseTimes( TimeSeries<T> template,
                                                                    SortedSet<Instant> endsAtSorted )
    {
        if ( Objects.isNull( this.upscaler ) )
        {
            throw new TimeSeriesGeneratorException( "While generating a persistence time-series using input series "
                                                    + template.hashCode()
                                                    + ", discovered that the input series had a desired time scale "
                                                    + "of "
                                                    + template.getTimeScale()
                                                    + ", but the "
                                                    + "persistence source had a desired time scale of "
                                                    + this.persistenceSource.getTimeScale()
                                                    + " and no temporal upscaler was supplied on construction." );
        }

        TimeSeries<T> upscaled = this.upscaler.upscale( this.persistenceSource,
                                                        template.getTimeScale(),
                                                        Collections.unmodifiableSortedSet( endsAtSorted ) )
                                              .getTimeSeries();

        return TimeSeries.of( template.getMetadata(), upscaled.getEvents() );
    }

    /**
     * Searches the list of events and returns the event that is Nth nearest to, and earlier than, the input time, 
     * where N is the order of persistence. Repeats for each input time in the list of times. The returned list is
     * ordered according to the input list of times, with a null value in place if no event was found for a particular
     * time.
     * 
     * @param eventsToSearch the events to search for a persistence value
     * @param time the time relative to which the persistence value is needed
     * @param order the order of persistence
     * @return the persistence events
     */

    private List<Event<T>> getNthNearestValueInstant( List<Event<T>> eventsToSearch, List<Instant> times, int order )
    {
        return times.stream()
                    .map( nextTime -> this.getNthNearestValueInstant( eventsToSearch, nextTime, order ) )
                    .collect( Collectors.toList() );
    }

    /**
     * Searches the list of events and returns the event that is Nth nearest to, and earlier than, the input time, 
     * where N is the order of persistence.
     * 
     * @param eventsToSearch the events to search for a persistence value
     * @param time the time relative to which the persistence value is needed
     * @param order the order of persistence
     * @return the persistence event
     */

    private Event<T> getNthNearestValueInstant( List<Event<T>> eventsToSearch, Instant time, int order )
    {
        Instant lastTime = eventsToSearch.get( 0 )
                                         .getTime();

        // If the reference time is equal to or later than the first valid time, return null
        if ( Objects.isNull( time )
             || lastTime.equals( time )
             || lastTime.isAfter( time ) )
        {
            return null;
        }

        // Perform a binary search
        int low = 0;
        int high = eventsToSearch.size() - 1;
        int index = -1;

        // In case of no match, find the nearest value in time
        Duration gap = TimeWindowOuter.DURATION_MIN;
        int closest = -1;

        while ( low <= high )
        {
            int mid = low + ( ( high - low ) / 2 );

            Instant check = eventsToSearch.get( mid )
                                          .getTime();

            if ( check.isBefore( time ) )
            {
                // Set the nearest time that is less than the reference time
                Duration innerGap = Duration.between( time, check );
                if ( innerGap.compareTo( gap ) > 0 )
                {
                    closest = mid;
                }

                low = mid + 1;
            }
            else if ( check.isAfter( time ) )
            {
                high = mid - 1;
            }
            else if ( check.equals( time ) )
            {
                index = mid;
                break;
            }
        }

        Event<T> returnMe = null;

        // Exact match? If so, return the time that is immediately before it, immediate being the "order"
        int indexMinusOrder = index - order;
        if ( indexMinusOrder > -1 )
        {
            return eventsToSearch.get( indexMinusOrder );
        }

        // No exact match. Is there a nearest value that is earlier? If so, return it, bearing in mind that less already
        // means at least one prior, so add one to the "order"
        int closestMinusOrder = closest - order + 1;
        if ( closestMinusOrder > -1 )
        {
            return eventsToSearch.get( closestMinusOrder );
        }

        return returnMe;
    }

    /**
     * Maps the persistence event times in the upscaled time-series to their corresponding valid event times based on 
     * the mapping implied by the times at equivalent index positions in the two supplied lists.
     * 
     * @param validTimes the valid times to which the time-series should be remapped
     * @param persistenceEvents the persistence events
     * @param upscaled the upscaled time-series expressed with persistence event times
     * @return the upscaled time-series expressed with valid event times
     */

    private TimeSeries<T> mapUpscaledEventsToValidTimes( List<Instant> validTimes,
                                                         List<Event<T>> persistenceEvents,
                                                         TimeSeries<T> upscaled )
    {
        if ( validTimes.size() != persistenceEvents.size() )
        {
            throw new IllegalStateException( "Failed to map upscaled persistence events to their corresponding "
                                             + "valid times because the number of valid times and persistence events "
                                             + "was unequal, which is not expected: ["
                                             + validTimes.size()
                                             + ", "
                                             + persistenceEvents.size()
                                             + "]." );
        }
        // Map the persistence event times in which the upscaled series is expressed to the corresponding valid times.
        // This works because the two lists are indexed identically.
        Map<Instant, Instant> mappedTimes = new HashMap<>();

        for ( int i = 0; i < validTimes.size(); i++ )
        {
            Event<T> nextEvent = persistenceEvents.get( i );
            if ( Objects.nonNull( nextEvent ) )
            {
                mappedTimes.put( nextEvent.getTime(), validTimes.get( i ) );
            }
        }

        SortedSet<Event<T>> upscaledEvents = upscaled.getEvents();
        SortedSet<Event<T>> adjEvents = new TreeSet<>();

        for ( Event<T> nextEvent : upscaledEvents )
        {
            Instant validTime = mappedTimes.get( nextEvent.getTime() );
            if ( Objects.nonNull( validTime ) )
            {
                Event<T> adjusted = Event.of( validTime, nextEvent.getValue() );
                adjEvents.add( adjusted );
            }
        }

        return TimeSeries.of( upscaled.getMetadata(), adjEvents );
    }

    /**
     * Hidden constructor.
     * 
     * @param order the order of persistence
     * @param persistenceSource the source data for the persistence values
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the 
     *            persistenceSource
     * @param admissableValue an optional constrain on each admissible values to persist
     * @throws NullPointerException if the persistenceSource is null
     * @throws TimeSeriesGeneratorException if the generator could not be created
     */

    private PersistenceGenerator( int order,
                                  Supplier<Stream<TimeSeries<T>>> persistenceSource,
                                  TimeSeriesUpscaler<T> upscaler,
                                  Predicate<T> admissibleValue )
    {
        Objects.requireNonNull( persistenceSource );

        if ( order < 0 )
        {
            throw new TimeSeriesGeneratorException( "A positive order of persistence is required: " + order );
        }

        this.order = order;

        // Retrieve the time-series on construction
        List<TimeSeries<T>> source = persistenceSource.get()
                                                      .collect( Collectors.toList() );

        // Consolidate into one series
        this.persistenceSource = TimeSeriesSlicer.consolidate( source );

        if ( this.persistenceSource.getEvents().size() < order )
        {
            throw new TimeSeriesGeneratorException( "Could not create a persistence source from the time-series "
                                                    + "supplier: at least "
                                                    + order
                                                    + " time-series values are "
                                                    + "required to generate a persistence time-series of order "
                                                    + order
                                                    + " but the supplier only contained "
                                                    + this.persistenceSource.getEvents()
                                                                            .size()
                                                    + " values. " );
        }

        this.upscaler = upscaler;

        if ( Objects.isNull( admissibleValue ) )
        {
            this.admissibleValue = test -> true;
        }
        else
        {
            this.admissibleValue = admissibleValue;
        }
    }

}
