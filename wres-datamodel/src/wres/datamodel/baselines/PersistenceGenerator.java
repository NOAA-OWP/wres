package wres.datamodel.baselines;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Generates a persistence time-series from a source of persistence data supplied on construction. The shape of the 
 * persistence series is obtained from a template time-series supplied on demand. Uses the reference time as the anchor
 * datetime for persistence when available, otherwise the valid times. The template time-series must use the same 
 * feature identity as the data source from which the persistence time-series is generated.
 *
 * @author James Brown
 * @param <T> the type of persistence value to generate
 */

public class PersistenceGenerator<T> implements UnaryOperator<TimeSeries<T>>
{
    private static final String WHILE_GENERATING_A_PERSISTENCE_TIME_SERIES_USING_INPUT_SERIES =
            "While generating a persistence time-series using input series {}{}";

    private static final String MONTH_DAYS = "month-days, {}";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PersistenceGenerator.class );

    /** The order of persistence relative to the start of the template series from which the persistence value should 
     * be derived. Order means the number of times prior to the reference time in the template. */
    private final int order;

    /** The source data from which the persistence values should be generated, indexed by feature. */
    private final Map<Feature, TimeSeries<T>> persistenceSource;

    /** Representative time-series metadata from the persistence source. */
    private final TimeSeriesMetadata persistenceSourceMetadata;

    /** An optional upscaler to use in generating a persistence value from the {@link #persistenceSource}. */
    private final TimeSeriesUpscaler<T> upscaler;

    /** An optional constraint on admissible values. If a value is not eligible for persistence, the empty series is
     * returned. */
    private final Predicate<T> admissibleValue;

    /** The desired measurement unit. **/
    private final String desiredUnit;

    /** Zone ID, used several times. */
    private static final ZoneId ZONE_ID = ZoneId.of( "UTC" );

    /**
     * Provides an instance for persistence of order one, i.e., lag-1 persistence.
     *
     * @param <T> the type of time-series event value
     * @param persistenceSource the persistence data source, required
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the 
     *                 persistenceSource
     * @param admissibleValue an optional constraint on values that should be persisted
     * @param desiredUnit the desired measurement unit, required
     * @return an instance
     * @throws NullPointerException if any required input is null
     */

    public static <T> PersistenceGenerator<T> of( Supplier<Stream<TimeSeries<T>>> persistenceSource,
                                                  TimeSeriesUpscaler<T> upscaler,
                                                  Predicate<T> admissibleValue,
                                                  String desiredUnit )
    {
        return new PersistenceGenerator<>( 1,
                                           persistenceSource,
                                           upscaler,
                                           admissibleValue,
                                           desiredUnit );
    }

    /**
     * Provides an instance for persistence of order one, i.e., lag-1 persistence.
     *
     * @param <T> the type of time-series event value
     * @param persistenceSource the persistence data source, required
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the 
     *            persistenceSource, optional
     * @param admissibleValue an optional constraint on values that should be persisted
     * @param lag the lag or order, which must be non-negative
     * @param desiredUnit the desired measurement unit, required
     * @return an instance
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the order is negative
     */

    public static <T> PersistenceGenerator<T> of( Supplier<Stream<TimeSeries<T>>> persistenceSource,
                                                  TimeSeriesUpscaler<T> upscaler,
                                                  Predicate<T> admissibleValue,
                                                  int lag,
                                                  String desiredUnit )
    {
        return new PersistenceGenerator<>( lag,
                                           persistenceSource,
                                           upscaler,
                                           admissibleValue,
                                           desiredUnit );
    }

    /**
     * Creates a persistence time-series at a lag supplied on construction using the input time-series as a template.
     *
     * @param template the template time-series for which persistence values will be generated
     * @return a time-series with the lagged value at every time-step
     * @throws NullPointerException if the input is null
     * @throws BaselineGeneratorException if the persistence value could not be generated
     */

    @Override
    public TimeSeries<T> apply( TimeSeries<T> template )
    {
        Objects.requireNonNull( template );

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        if ( template.getEvents()
                     .isEmpty() )
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
        // Upscale? 
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.getSourceTimeScale() )
             && !desiredTimeScale.equals( this.getSourceTimeScale() ) )
        {
            return this.getPersistenceForFirstReferenceTimeWithUpscaling( template );
        }
        else
        {
            return this.getPersistenceForFirstReferenceTimeWithoutUpscaling( template );
        }
    }

    /**
     * Returns a persistence time-series with respect to the first reference time in the input. This method is 
     * appropriate when no upscaling is required.
     * @param template the template time-series
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForFirstReferenceTimeWithoutUpscaling( TimeSeries<T> template )
    {
        LOGGER.trace( "Generating persistence for a time-series with a reference time where upscaling is not "
                      + "required." );

        // Put the persistence values into a list. There are at least N+1 values in the list, established at 
        // construction
        TimeSeries<T> persistenceSeries = getPersistenceSeriesForTemplate( template );
        List<Event<T>> eventsToSearch = persistenceSeries.getEvents()
                                                         .stream()
                                                         .toList();

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        // Take the first available reference time
        // If multiple reference times are provided in future, adapt
        // Compute the instant at which the persistence value should end
        Instant referenceTime = referenceTimes.values()
                                              .iterator()
                                              .next();

        if ( referenceTimes.size() > 1 && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                          + "input series has multiple reference times. Using the first time of {}.",
                          template.getMetadata(),
                          referenceTime );
        }

        Event<T> persistenceEvent = this.getNthNearestEventEarlierThanInstant( eventsToSearch,
                                                                               referenceTime,
                                                                               this.order );

        if ( Objects.isNull( persistenceEvent ) )
        {
            LOGGER.trace( "While attempting to generate a persistence value for the reference time of {}, failed to "
                          + "find a corresponding time in the persistence source, which contained {} values.",
                          referenceTime,
                          eventsToSearch.size() );

            return new Builder<T>().setMetadata( template.getMetadata() )
                                   .build();
        }

        T persistenceValue = persistenceEvent.getValue();

        return this.getPersistenceTimeSeriesFromTemplateAndPersistenceValue( template, persistenceValue );
    }

    /**
     * Returns a persistence time-series with respect to the first reference time in the input. This method is 
     * appropriate when upscaling is required.
     * @param template the template time-series
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForFirstReferenceTimeWithUpscaling( TimeSeries<T> template )
    {
        LOGGER.trace( "Generating persistence for a time-series with a reference time where upscaling is required." );

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        // Take the first available reference time
        // If multiple reference times are provided in future, adapt
        Instant referenceTime = referenceTimes.values()
                                              .iterator()
                                              .next();

        if ( referenceTimes.size() > 1 && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                          + "input series has multiple reference times. Using the first time of {}.",
                          template.getMetadata(),
                          referenceTime );
        }

        TimeScaleOuter timeScale = template.getTimeScale();
        TimeSeries<T> persistenceSeries = getPersistenceSeriesForTemplate( template );

        // The rescaled periods start and/or end at a precise month-day
        if ( timeScale.hasMonthDays() )
        {
            LOGGER.trace( "Generating a persistence value for a reference time and a desired time scale with explicit "
                          + MONTH_DAYS,
                          timeScale );

            // No explicit valid times at which the upscaled values are required
            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template,
                                                                                    persistenceSeries,
                                                                                    Collections.emptyList(),
                                                                                    this.desiredUnit );

            ZonedDateTime time = referenceTime.atZone( ZONE_ID );
            time = time.minusYears( this.order );
            Instant targetLower = time.toInstant();
            Instant targetUpper = time.plusYears( 1 )
                                      .toInstant();

            // Find an upscaled event that is between N years and N-1 years earlier than the reference time, inclusive
            Optional<Event<T>> targetEvent = upscaled.getEvents()
                                                     .stream()
                                                     // The upscaled event time is between the lower and upper bounds, 
                                                     // inclusive
                                                     .filter( nextEvent -> nextEvent.getTime().equals( targetLower )
                                                                           || nextEvent.getTime().equals( targetUpper )
                                                                           || ( nextEvent.getTime()
                                                                                         .isAfter( targetLower )
                                                                                && nextEvent.getTime()
                                                                                            .isBefore( targetUpper ) ) )
                                                     .findFirst();

            if ( targetEvent.isPresent() )
            {
                T persistenceValue = targetEvent.get()
                                                .getValue();
                return this.getPersistenceTimeSeriesFromTemplateAndPersistenceValue( template, persistenceValue );
            }
            else
            {
                LOGGER.trace( "While attempting to generate a persistence value for the reference time of {}, failed "
                              + "to find a corresponding time in the upscaled persistence source, {}.",
                              referenceTime,
                              upscaled );

                return new Builder<T>().setMetadata( template.getMetadata() )
                                       .build();
            }
        }
        else
        {
            LOGGER.trace( "Generating a persistence value for a reference time and a desired time scale without "
                          + "explicit month-days, {}",
                          timeScale );

            // Put the persistence values into a list. There are at least N+1 values in the list, established at 
            // construction
            List<Event<T>> eventsToSearch = persistenceSeries.getEvents()
                                                             .stream()
                                                             .toList();

            Event<T> persistenceEvent = this.getNthNearestEventEarlierThanInstant( eventsToSearch,
                                                                                   referenceTime,
                                                                                   this.order );

            if ( Objects.isNull( persistenceEvent ) )
            {
                LOGGER.trace(
                        "While attempting to generate a persistence value for the reference time of {}, failed to "
                        + "find a corresponding time in the persistence source, which contained {} values.",
                        referenceTime,
                        eventsToSearch.size() );

                return new Builder<T>().setMetadata( template.getMetadata() )
                                       .build();
            }

            T persistenceValue = null;
            Instant persistenceEventTime = persistenceEvent.getTime();

            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template,
                                                                                    persistenceSeries,
                                                                                    List.of( persistenceEventTime ),
                                                                                    this.desiredUnit );

            if ( !upscaled.getEvents().isEmpty() )
            {
                persistenceValue = upscaled.getEvents()
                                           .first()
                                           .getValue();
            }

            return this.getPersistenceTimeSeriesFromTemplateAndPersistenceValue( template, persistenceValue );
        }
    }

    /**
     * Generates a persistence time-series from a template time-series and a persistence value.
     * @param template the template
     * @param persistenceValue the persistence value
     * @return the persistence time-series
     */

    private TimeSeries<T> getPersistenceTimeSeriesFromTemplateAndPersistenceValue( TimeSeries<T> template,
                                                                                   T persistenceValue )
    {
        // Adjust the template metadata because the persisted values are in the same measurement units as the 
        // persistence source
        TimeSeriesMetadata adjusted = template.getMetadata()
                                              .toBuilder()
                                              .setUnit( this.getSourceUnit() )
                                              .build();

        Builder<T> builder = new Builder<T>().setMetadata( adjusted );

        // Persistence value admissible?
        if ( ( Objects.isNull( persistenceValue ) || !this.admissibleValue.test( persistenceValue ) ) )
        {
            LOGGER.trace( "While generating a persistence time-series for {}, discovered that the persistent value of "
                          + "{} was inadmissible. Unable to create a persistence time-series from the input series. "
                          + "Returning the empty time-series instead.",
                          template.getMetadata(),
                          persistenceValue );
        }
        else
        {
            // Use the persistence value for every valid time in the template
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
        // Upscale? 
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.getSourceTimeScale() )
             && !desiredTimeScale.equals( this.getSourceTimeScale() ) )
        {
            return this.getPersistenceForEachValidTimeWithUpscaling( template );
        }
        else
        {
            return this.getPersistenceForEachValidTimeWithoutUpscaling( template );
        }
    }

    /**
     * Returns a persistence time-series with respect to each valid time in the input. This method is appropriate when
     * no upscaling is required.
     * @param template the template time-series to populate with persistence values
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForEachValidTimeWithoutUpscaling( TimeSeries<T> template )
    {
        LOGGER.trace( "Generating persistence for multiple valid times where upscaling is not required." );

        // Put the persistence values into a list. There are at least N+1 values in the list, established at 
        // construction
        TimeSeries<T> persistenceSeries = getPersistenceSeriesForTemplate( template );
        List<Event<T>> eventsToSearch = persistenceSeries.getEvents()
                                                         .stream()
                                                         .toList();

        // Find a persistence event from the eventsToSearch for each valid time in the template series
        List<Instant> validTimes = template.getEvents()
                                           .stream()
                                           .map( Event::getTime )
                                           .toList();

        List<Event<T>> persistenceEvents = this.getNthNearestEventEarlierThanEachInstant( eventsToSearch,
                                                                                          validTimes,
                                                                                          this.order );

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

    /**
     * Returns a persistence time-series with respect to each valid time in the input. This method is appropriate when
     * upscaling is required.
     * @param template the template time-series to populate with persistence values
     * @return a persistence time-series that uses the valid times
     */

    private TimeSeries<T> getPersistenceForEachValidTimeWithUpscaling( TimeSeries<T> template )
    {
        LOGGER.trace( "Generating persistence for multiple valid times where upscaling is required." );

        TimeScaleOuter timeScale = template.getTimeScale();
        TimeSeries<T> persistenceSeries = getPersistenceSeriesForTemplate( template );

        // The rescaled periods start and/or end at a precise month-day
        if ( timeScale.hasMonthDays() )
        {
            LOGGER.trace( "Generating a persistence value for a valid time and a desired time scale with explicit "
                          + MONTH_DAYS,
                          timeScale );

            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template,
                                                                                    persistenceSeries,
                                                                                    Collections.emptyList(),
                                                                                    this.desiredUnit );

            // Find the upscaled period whose valid time is N prior to each template valid time. Since a month-day 
            // range can occur only once per year, this is effectively N years prior to the template valid time
            TimeSeries.Builder<T> builder = new TimeSeries.Builder<T>().setMetadata( upscaled.getMetadata() );

            // Search the template events and find a corresponding upscaled event that is the Nth corresponding event
            // that is earlier than the template time
            for ( Event<T> next : template.getEvents() )
            {
                Pair<Instant, Instant> interval = this.getLaggedMonthDayInterval( next.getTime(),
                                                                                  this.order );
                Instant targetLower = interval.getLeft();
                Instant targetUpper = interval.getRight();

                // Find an upscaled event that is between the target times
                Optional<Event<T>> event = upscaled.getEvents()
                                                   .stream()
                                                   // The upscaled event time is between the lower and upper inclusive
                                                   .filter( nextEvent -> nextEvent.getTime().equals( targetLower )
                                                                         || nextEvent.getTime().equals( targetUpper )
                                                                         || ( nextEvent.getTime().isAfter( targetLower )
                                                                              && nextEvent.getTime()
                                                                                          .isBefore( targetUpper ) ) )
                                                   .findFirst();
                if ( event.isPresent() )
                {
                    Instant currentTime = next.getTime(); // Current event time
                    T laggedValue = event.get()
                                         .getValue(); // Lagged event value   
                    Event<T> nextEvent = Event.of( currentTime, laggedValue );
                    builder.addEvent( nextEvent );
                }
            }

            return builder.build();
        }
        else
        {
            LOGGER.trace( "Generating a persistence value for a valid time and a desired time scale without explicit "
                          + MONTH_DAYS,
                          timeScale );

            // The valid times from the template series at which persistence events are required
            // NOTE: if the template time-series begins after the persistence source, then the first value in the 
            // template may not acquire a persistence value, even though the persistence source has sufficient data to 
            // deliver one. This is because the upscaled values can only be acquired with respect to valid times in the
            // template and not to unknown, earlier, times. In other words, the time that is of order N prior to the
            // first valid time in the template is unknown. Extrapolating the sequence backwards is brittle. Instead, 
            // the template series should be elongated by the user to capture this value or the persistence declared as 
            // an absolute duration because the duration prior to the first valid time in the template is finite/known: 
            // see #111822
            List<Instant> templateValidTimes = template.getEvents()
                                                       .stream()
                                                       .map( Event::getTime )
                                                       .toList();

            // Rescale the persistence source to end at the valid times in the template series
            TimeSeries<T> upscaled = this.getUpscaledPersistenceSeriesAtTheseTimes( template,
                                                                                    persistenceSeries,
                                                                                    templateValidTimes,
                                                                                    this.desiredUnit );

            // The upscaled persistence values
            List<Event<T>> persistenceEvents = upscaled.getEvents()
                                                       .stream()
                                                       .toList();

            // Find a persistence event for each valid time in the template
            List<Event<T>> persistenceEventPerValidTime =
                    this.getNthNearestEventEarlierThanEachInstant( persistenceEvents,
                                                                   templateValidTimes,
                                                                   this.order );

            // Find the persistence event times at which the upscaled values must end, padding with null to retain shape
            List<Instant> persistenceEventTimes = persistenceEventPerValidTime.stream()
                                                                              .map( next -> Objects.nonNull( next ) ?
                                                                                      next.getTime()
                                                                                      :
                                                                                      null )
                                                                              .toList();

            // Get the pairs of template valid times and persistence event times based on index positions
            Map<Instant, Instant> pairedTimes = this.getPairedTimes( templateValidTimes, persistenceEventTimes );

            return this.mapUpscaledEventsToValidTimes( pairedTimes, upscaled );
        }
    }

    /**
     * Returns the paired times from the inputs based on corresponding index positions.
     *
     * @param validTimes the valid times of the template time-series for which persistence values are required
     * @param persistenceEventTimes the persistence event times
     * @return the template series valid times keyed against the persistence event times
     */

    private Map<Instant, Instant> getPairedTimes( List<Instant> validTimes, List<Instant> persistenceEventTimes )
    {
        if ( validTimes.size() != persistenceEventTimes.size() )
        {
            throw new IllegalStateException( "Failed to map upscaled persistence events to their corresponding "
                                             + "valid times because the number of valid times and persistence events "
                                             + "was unequal, which is not expected: ["
                                             + validTimes.size()
                                             + ", "
                                             + persistenceEventTimes.size()
                                             + "]." );
        }

        Map<Instant, Instant> pairs = new TreeMap<>();
        for ( int i = 0; i < validTimes.size(); i++ )
        {
            Instant left = persistenceEventTimes.get( i );
            Instant right = validTimes.get( i );
            if ( Objects.nonNull( left ) && Objects.nonNull( right ) )
            {
                pairs.put( left, right );
            }
        }

        return Collections.unmodifiableMap( pairs );
    }

    /**
     * Returns the interval within which a lagged month-day event must fall based on the prescribed lag and event time.
     *
     * @param eventTime the event time
     * @param lag the order of persistence
     * @return the interval
     */

    private Pair<Instant, Instant> getLaggedMonthDayInterval( Instant eventTime, int lag )
    {
        // The event must fall within the interval that begins N years before the event time, inclusive, and ends
        // up to N-1 years before the event time, exclusive
        ZonedDateTime time = eventTime.atZone( ZONE_ID );
        Instant targetLower = time.minusYears( lag )
                                  .toInstant();
        Instant targetUpper = time.minusYears( lag - 1L )
                                  .minusNanos( 1 ) // Do not include the event time
                                  .toInstant();

        return Pair.of( targetLower, targetUpper );
    }

    /**
     * Returns the persistence time-series upscaled to the desired time scale of the template series at the prescribed
     * end times.
     * @param template the template series
     * @param seriesToUpscale the time series to upscale
     * @param endsAtSorted the times at which upscaled values should end
     * @param desiredUnit the desired measurement unit
     * @return the upscaled series
     */

    private TimeSeries<T> getUpscaledPersistenceSeriesAtTheseTimes( TimeSeries<T> template,
                                                                    TimeSeries<T> seriesToUpscale,
                                                                    List<Instant> endsAtSorted,
                                                                    String desiredUnit )
    {
        if ( Objects.isNull( this.upscaler ) )
        {
            throw new BaselineGeneratorException( "While generating a persistence time-series using input series "
                                                  + template.hashCode()
                                                  + ", discovered that the input series had a desired time scale "
                                                  + "of "
                                                  + template.getTimeScale()
                                                  + ", but the "
                                                  + "persistence source had a desired time scale of "
                                                  + this.getSourceTimeScale()
                                                  + " and no temporal upscaler was supplied on construction." );
        }

        TimeSeries<T> upscaled = this.upscaler.upscale( seriesToUpscale,
                                                        template.getTimeScale(),
                                                        Collections.unmodifiableSortedSet( new TreeSet<>( endsAtSorted ) ),
                                                        desiredUnit )
                                              .getTimeSeries();

        // Adjust the template metadata because the persisted values are in the same measurement units as the 
        // persistence source
        TimeSeriesMetadata adjusted = template.getMetadata()
                                              .toBuilder()
                                              .setUnit( upscaled.getMetadata()
                                                                .getUnit() )
                                              .build();

        return TimeSeries.of( adjusted, upscaled.getEvents() );
    }

    /**
     * Searches the list of events and returns the event that is Nth nearest to, and earlier than, the input time, 
     * where N is the order of persistence. Repeats for each input time in the list of times. The returned list is
     * ordered according to the input list of times, with a null value in place if no event was found for a particular
     * time.
     *
     * @param eventsToSearch the events to search for a persistence value
     * @param times the times relative to which the persistence value is needed
     * @param order the order of persistence
     * @return the persistence events
     */

    private List<Event<T>> getNthNearestEventEarlierThanEachInstant( List<Event<T>> eventsToSearch,
                                                                     List<Instant> times,
                                                                     int order )
    {
        return times.stream()
                    .map( nextTime -> this.getNthNearestEventEarlierThanInstant( eventsToSearch, nextTime, order ) )
                    .toList();
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

    private Event<T> getNthNearestEventEarlierThanInstant( List<Event<T>> eventsToSearch, Instant time, int order )
    {
        // If there are no events or the reference time is equal to or later than the first valid time, return null
        if ( eventsToSearch.isEmpty() || Objects.isNull( eventsToSearch.get( 0 )
                                                                       .getTime() )
             || eventsToSearch.get( 0 )
                              .getTime()
                              .equals( time )
             || eventsToSearch.get( 0 )
                              .getTime()
                              .isAfter( time ) )
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

        return null;
    }

    /**
     * Maps the persistence event times in the upscaled time-series to their corresponding valid event times based on 
     * the mapping implied by the times at equivalent index positions in the two supplied lists.
     *
     * @param validTimePairs the pairs of valid times and corresponding persistence event times
     * @param upscaled the upscaled time-series expressed with persistence event times
     * @return the upscaled time-series expressed with valid event times
     */

    private TimeSeries<T> mapUpscaledEventsToValidTimes( Map<Instant, Instant> validTimePairs,
                                                         TimeSeries<T> upscaled )
    {
        SortedSet<Event<T>> upscaledEvents = upscaled.getEvents();
        SortedSet<Event<T>> adjEvents = new TreeSet<>();

        for ( Event<T> nextEvent : upscaledEvents )
        {
            Instant validTime = validTimePairs.get( nextEvent.getTime() );
            if ( Objects.nonNull( validTime ) )
            {
                Event<T> adjusted = Event.of( validTime, nextEvent.getValue() );
                adjEvents.add( adjusted );
            }
        }

        return TimeSeries.of( upscaled.getMetadata(), adjEvents );
    }

    /**
     * @return the time scale of the persistence source
     */

    private TimeScaleOuter getSourceTimeScale()
    {
        return this.persistenceSourceMetadata.getTimeScale();
    }

    /**
     * @return the measurement unit of the persistence source
     */

    private String getSourceUnit()
    {
        return this.persistenceSourceMetadata.getUnit();
    }

    /**
     * @return the time-series from the persistence source whose feature name matches template series
     */

    private TimeSeries<T> getPersistenceSeriesForTemplate( TimeSeries<T> template )
    {
        // Feature correlation assumes that the template feature is right-ish and the source feature is baseline-ish
        // If this is no longer a safe assumption, then the orientation should be declared on construction
        Feature templateFeature = template.getMetadata()
                                          .getFeature();

        if ( !this.persistenceSource.containsKey( templateFeature ) )
        {
            Set<Feature> sourceFeatures = this.persistenceSource.values()
                                                                .stream()
                                                                .map( next -> next.getMetadata().getFeature() )
                                                                .collect( Collectors.toSet() );

            throw new BaselineGeneratorException( "When building a persistence baseline, failed to discover a "
                                                  + "source time-series for the template time-series with feature: "
                                                  + templateFeature
                                                  + ". Source time-series were available for features: "
                                                  + sourceFeatures
                                                  + "." );
        }

        return this.persistenceSource.get( templateFeature );
    }

    /**
     * Hidden constructor.
     *
     * @param order the order of persistence
     * @param persistenceSource the source data for the persistence values, not null
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the 
     *            persistenceSource
     * @param admissibleValue an optional constrain on each admissible values to persist
     * @param desiredUnit the desired measurement unit, not null
     * @throws NullPointerException if any required input is null
     * @throws BaselineGeneratorException if the generator could not be created
     */

    private PersistenceGenerator( int order,
                                  Supplier<Stream<TimeSeries<T>>> persistenceSource,
                                  TimeSeriesUpscaler<T> upscaler,
                                  Predicate<T> admissibleValue,
                                  String desiredUnit )
    {
        Objects.requireNonNull( persistenceSource );
        Objects.requireNonNull( desiredUnit );

        if ( order < 0 )
        {
            throw new BaselineGeneratorException( "A positive order of persistence is required: " + order );
        }

        this.order = order;

        // Retrieve the time-series on construction
        List<TimeSeries<T>> source = persistenceSource.get()
                                                      .toList();

        // The persistence source cannot be empty
        if ( source.isEmpty() )
        {
            throw new BaselineGeneratorException( "Cannot generate a persistence baseline without a time-series. The "
                                                  + "persistence source was empty." );
        }

        // The persistence source cannot contain forecast-like time-series
        if ( source.stream()
                   .anyMatch( next -> next.getReferenceTimes()
                                          .containsKey( ReferenceTimeType.T0 ) ) )
        {
            throw new BaselineGeneratorException( "When attempting to generate a persistence baseline, discovered "
                                                  + "one or more time-series that contained a reference time with "
                                                  + "type 'T0', which is indicative of a forecast. Forecast-like "
                                                  + "time-series are not valid as the data source for a persistence "
                                                  + "baseline. Instead, declare observation-like time-series as the "
                                                  + "baseline data source." );
        }

        // Perform the consolidation all at once rather than for each pair of time-series. See #111801
        Map<Feature, List<TimeSeries<T>>> grouped = source.stream()
                                                          .filter( next -> next.getEvents().size() >= order )
                                                          .collect( Collectors.groupingBy( next -> next.getMetadata()
                                                                                                       .getFeature(),
                                                                                           Collectors.mapping( Function.identity(),
                                                                                                               Collectors.toList() ) ) );

        // Consolidate the time-series by feature
        Map<Feature, TimeSeries<T>> consolidated = new HashMap<>();
        for ( Map.Entry<Feature, List<TimeSeries<T>>> nextEntry : grouped.entrySet() )
        {
            Feature nextFeature = nextEntry.getKey();
            List<TimeSeries<T>> series = nextEntry.getValue();
            TimeSeries<T> nextConsolidated = this.consolidate( series, nextFeature.getName() );
            consolidated.put( nextFeature, nextConsolidated );
        }

        this.persistenceSource = Collections.unmodifiableMap( consolidated );

        if ( this.persistenceSource.isEmpty() )
        {
            throw new BaselineGeneratorException( "Could not create a persistence source from the time-series "
                                                  + "supplier: at least one time-series that contains "
                                                  + order
                                                  + " event values is "
                                                  + "required to generate a persistence time-series of order "
                                                  + order
                                                  + " but the supplier contained no time-series that match this "
                                                  + "requirement. " );
        }

        this.upscaler = upscaler;
        this.desiredUnit = desiredUnit;
        this.persistenceSourceMetadata = this.persistenceSource.values()
                                                               .stream()
                                                               .findAny()
                                                               .orElseThrow() // Already checked above that it exists
                                                               .getMetadata();

        if ( Objects.isNull( admissibleValue ) )
        {
            this.admissibleValue = test -> true;
        }
        else
        {
            this.admissibleValue = admissibleValue;
        }

        LOGGER.debug( "Created a persistence generator for lagged persistence of order {}.", this.order );
    }

    /**
     * Consolidate the time-series and emit a warning if duplicates are encountered. Unlike
     * {@link TimeSeriesSlicer#consolidate(Collection)}, this method admits duplicates.
     *
     * @param toConsolidate the time-series to consolidate
     * @param featureName the feature name to help with messaging
     * @return the consolidated time-series
     */

    private TimeSeries<T> consolidate( List<TimeSeries<T>> toConsolidate, String featureName )
    {
        SortedSet<Event<T>> events =
                new TreeSet<>( Comparator.comparing( Event::getTime ) );

        Set<Instant> duplicates = new TreeSet<>();
        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();
        for ( TimeSeries<T> nextSeries : toConsolidate )
        {
            builder.setMetadata( nextSeries.getMetadata() );
            SortedSet<Event<T>> nextEvents = nextSeries.getEvents();
            for ( Event<T> nextEvent : nextEvents )
            {
                boolean added = events.add( nextEvent );
                if ( !added )
                {
                    duplicates.add( nextEvent.getTime() );
                }
            }
        }

        TimeSeries<T> consolidated = builder.setEvents( events )
                                            .build();

        if ( !duplicates.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "While generating a persistence baseline for feature '{}', encountered duplicate events in "
                         + "the source data at {} valid times. Using only the first event encountered at each "
                         + "duplicated time. If this is unintended, please de-duplicate the persistence data before "
                         + "creating a persistence baseline. The common time-series metadata for the time-series that "
                         + "contained duplicate events was: {}. The first duplicate time was: {}.",
                         featureName,
                         duplicates.size(),
                         consolidated.getMetadata(),
                         duplicates.iterator()
                                   .next() );
        }

        return consolidated;
    }

}
