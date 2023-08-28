package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;

import wres.datamodel.scale.TimeScaleOuter;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>A time-series contains a time-ordered set of {@link Event}, together with {@link TimeSeriesMetadata}.
 *
 * @param <T> the type of time-series event value
 * @author James Brown
 * @author Jesse Bickel
 */

@Immutable
public class TimeSeries<T>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeries.class );

    /** Any non-event-related metadata that apply to the time-series as a whole. */
    private final TimeSeriesMetadata metadata;

    /** The events. */
    private final SortedSet<Event<T>> events;

    /**
     * Returns an empty {@link TimeSeries} with prescribed metadata.
     *
     * @param <T> the event type
     * @param metadata The metadata for the empty timeseries.
     * @return an empty time-series
     */

    public static <T> TimeSeries<T> of( TimeSeriesMetadata metadata )
    {
        return TimeSeries.of( metadata, Collections.emptySortedSet() );
    }

    /**
     * Returns a {@link TimeSeries} with prescribed metadata.
     *
     * @param <T> the event type
     * @param timeSeriesMetadata the metadata
     * @param events the events
     * @return the time-series
     */

    public static <T> TimeSeries<T> of( TimeSeriesMetadata timeSeriesMetadata,
                                        SortedSet<Event<T>> events )
    {
        Objects.requireNonNull( timeSeriesMetadata );
        Objects.requireNonNull( events );

        return new Builder<T>().setMetadata( timeSeriesMetadata )
                               .setEvents( events )
                               .build();
    }

    /**
     * Returns the underlying events in the time-series.
     *
     * @return the underlying events.
     */

    public SortedSet<Event<T>> getEvents()
    {
        return this.events; // Rendered immutable on construction
    }

    /**
     * Returns the {@link TimeSeriesMetadata}. 
     *
     * @return the metadata
     */

    public TimeSeriesMetadata getMetadata()
    {
        return this.metadata;
    }

    /**
     * Returns the {@link TimeScaleOuter} associated with the events or <code>null</code> if {@link #hasTimeScale()} returns
     * <code>false</code>.
     *
     * @return the time-scale or null
     */

    public TimeScaleOuter getTimeScale()
    {
        return this.getMetadata()
                   .getTimeScale();
    }

    /**
     * Returns <code>true</code> if the time-scale is known, otherwise <code>false</code>.
     *
     * @return true if the time scale is known, otherwise false
     */

    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.getMetadata() )
               && Objects.nonNull( this.getMetadata()
                                       .getTimeScale() );
    }

    /**
     * Returns the reference datetime.
     *
     * @return the reference datetime
     */

    public Map<ReferenceTimeType, Instant> getReferenceTimes()
    {
        return this.getMetadata()
                   .getReferenceTimes(); //Rendered immutable on construction
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || this.getClass() != o.getClass() )
        {
            return false;
        }

        TimeSeries<?> that = ( TimeSeries<?> ) o;
        return this.metadata.equals( that.metadata ) &&
               this.events.equals( that.events );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.metadata, this.events );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "metadata", metadata )
                .append( "events", events )
                .toString();
    }

    /**
     * Builder that allows for incremental construction and validation. 
     *
     * @param <T> the type of data
     */

    public static class Builder<T>
    {
        /**
         * Events with a prescribed comparator based on event time, since duplicates by valid time are not allowed 
         * (even if the event values are different).
         */

        private SortedSet<Event<T>> events =
                new TreeSet<>( Comparator.comparing( Event::getTime ) );

        /**
         * The time-series metadata.
         */

        private TimeSeriesMetadata metadata;

        /**
         * Adds an event. This is the preferred method to build a time-series incrementally.
         *
         * @param event the event
         * @return the builder
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the time-series already contains an event at the prescribed time
         */

        public Builder<T> addEvent( Event<T> event )
        {
            Objects.requireNonNull( event );

            boolean successfullyAdded = this.events.add( event );

            if ( !successfullyAdded )
            {
                throw new IllegalArgumentException( "While building a time-series, attempted to add an event at the "
                                                    + "same valid datetime as an existing event, which is not allowed. "
                                                    + "The duplicate event by time is '"
                                                    + event
                                                    + "'. The time-series metadata is: "
                                                    + this.metadata );
            }

            return this;
        }

        /**
         * Sets the events for the time-series. This is the preferred method to build a time-series from a pre-existing 
         * set of events. Otherwise, favor {@link #addEvent(Event)} to build incrementally. Do not build a set of 
         * events locally and then call this method, as it will be less performant than building incrementally with 
         * {@link #addEvent(Event)}, although more performant than using the same pattern with 
         * {@link #addEvents(SortedSet)} (i.e., avoid both where possible, but especially the latter).
         *
         * @param events the events
         * @return the builder
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the time-series contains more than one event at the same time
         */

        public Builder<T> setEvents( SortedSet<Event<T>> events )
        {
            Objects.requireNonNull( events );

            // Must be as many instants as events, in keeping with the comparator used when this class builds the sorted
            // set, i.e., duplicates by valid time are not allowed
            Set<Instant> instants = events.stream()
                                          .map( Event::getTime )
                                          .collect( Collectors.toSet() );

            int duplicates = events.size() - instants.size();

            if ( duplicates > 0 )
            {
                throw new IllegalArgumentException( "While building a time-series from a set of events, discovered "
                                                    + duplicates
                                                    + " duplicate events by valid time. A time-series cannot contain "
                                                    + "duplicate events." );
            }

            this.events = events;

            return this;
        }

        /**
         * Adds a collection of events to the time-series. Only use this method when building a time-series from 
         * multiple pre-existing time-series or event collections, otherwise favor {@link #addEvent(Event)} or 
         * {@link #setEvents(SortedSet)}. Do not build a set of events locally and then call this method, as it will 
         * be less performant for very large time-series (requires a set to be populated twice).
         *
         * @param events the events
         * @return the builder
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if the time-series contains more than one event at the same time
         */

        public Builder<T> addEvents( SortedSet<Event<T>> events )
        {
            Objects.requireNonNull( events );

            events.forEach( this::addEvent );

            return this;
        }

        /**
         * Sets the time-series metadata.
         *
         * @param metadata the time-series metadata
         * @return the builder
         */

        public Builder<T> setMetadata( TimeSeriesMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

        /**
         * Clears all state from the builder, returning it to its new/original state.
         *
         * @return the builder
         */

        public Builder<T> clear()
        {
            this.events.clear();
            this.metadata = null;

            return this;
        }

        /**
         * Builds the time-series.
         *
         * @return a time-series
         */

        public TimeSeries<T> build()
        {
            return new TimeSeries<>( this );
        }

    }

    /**
     * Builds with a builder.
     *
     * @param builder the builder
     */
    private TimeSeries( Builder<T> builder )
    {
        // Copy, set and then validate
        // Do not use a comparator based on valid time here because this would lead to an inconsistency with equals 
        // when comparing the set of events based on valid times alone. This guard on duplicates by valid time is only 
        // required when building the time-series
        SortedSet<Event<T>> localEvents = new TreeSet<>();
        localEvents.addAll( builder.events );
        this.events = Collections.unmodifiableSortedSet( localEvents );
        this.metadata = builder.metadata;

        if ( Objects.isNull( this.metadata ) )
        {
            throw new UnsupportedOperationException( "Use complete metadata in your TimeSeries instances." );
        }

        // All reference datetimes and types must be non-null
        for ( Map.Entry<ReferenceTimeType, Instant> nextEntry : this.getReferenceTimes()
                                                                    .entrySet() )
        {
            Objects.requireNonNull( nextEntry.getKey() );
            Objects.requireNonNull( nextEntry.getValue() );
        }

        // No null events
        this.getEvents()
            .forEach( Objects::requireNonNull );

        // Log absence of timescale
        if ( Objects.isNull( this.getMetadata().getTimeScale() ) )
        {
            LOGGER.trace( "No time-scale information was provided in builder {} for time-series {}.",
                          builder,
                          this.getMetadata() );
        }
    }

}

