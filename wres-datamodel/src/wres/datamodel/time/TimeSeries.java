package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;

/**
 * <p>A time-series contains a time-ordered set of {@link Event}, together with zero or more reference datetimes and 
 * associated {@link ReferenceTimeType}.
 * 
 * <p><b>Implementation Notes:</b>
 * 
 * <p>This class is immutable and thread-safe.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 * @author Jesse Bickel
 */

public class TimeSeries<T>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeries.class );

    /**
     * Any non-event-related metadata that apply to the time-series as a whole.
     */

    private final TimeSeriesMetadata metadata;


    /**
     * The events.
     */

    private final SortedSet<Event<T>> events;


    /**
     * Returns an empty {@link TimeSeries} with a <code>null</code> {@link TimeScale}.
     *
     * @param <T> the event type
     * @return an empty time-series
     */

    public static <T> TimeSeries<T> of()
    {
        return TimeSeries.of( Collections.emptyMap(), Collections.emptySortedSet() );
    }
    
    /**
     * Returns a {@link TimeSeries} without any reference times. Assumes a <code>null</code> {@link TimeScale}.
     *
     * @param <T> the event type
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if the events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( SortedSet<Event<T>> events )
    {
        return TimeSeries.of( Collections.emptyMap(), events );
    }

    /**
     * Returns a {@link TimeSeries} with a reference time type of {@link ReferenceTimeType#UNKNOWN} and a 
     * <code>null</code> {@link TimeScale}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                        SortedSet<Event<T>> events )
    {
        return TimeSeries.of( Collections.singletonMap( ReferenceTimeType.UNKNOWN, referenceTime ), events );
    }

    /**
     * Returns a {@link TimeSeries} with a <code>null</code> {@link TimeScale}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param referenceTimeType the type of reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                        ReferenceTimeType referenceTimeType,
                                        SortedSet<Event<T>> events )
    {
        return TimeSeries.of( Collections.singletonMap( referenceTimeType, referenceTime ), events );
    }

    /**
     * Returns a {@link TimeSeries} with a <code>null</code> {@link TimeScale}.
     * 
     * @param <T> the event type
     * @param referenceTimes the reference times
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Map<ReferenceTimeType, Instant> referenceTimes,
                                        SortedSet<Event<T>> events )
    {
        Objects.requireNonNull( referenceTimes );

        Objects.requireNonNull( events );

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();
        events.forEach( builder::addEvent );
        referenceTimes.forEach( ( type, time ) -> builder.addReferenceTime( time, type ) );

        return builder.build();
    }

    /**
     * Returns a {@link TimeSeries} with prescribed metadata.
     * 
     * @param <T> the event type
     * @param timeSeriesMetadata the metadata
     * @param events the events
     * @return the time-series
     */
    
    public static <T> TimeSeries<T> of ( TimeSeriesMetadata timeSeriesMetadata,
                                         SortedSet<Event<T>> events )
    {
        Objects.requireNonNull( timeSeriesMetadata );
        Objects.requireNonNull( events );

        // Admittedly awkward, probably should make the builder delegate all
        // this stuff to a TimeSeriesMetadataBuilder or something.
        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();
        events.forEach( builder::addEvent );
        timeSeriesMetadata.getReferenceTimes()
                          .forEach( ( type, time ) -> builder.addReferenceTime( time, type ) );
        builder.setFeatureName( timeSeriesMetadata.getFeatureName() );
        builder.setTimeScale( timeSeriesMetadata.getTimeScale() );
        builder.setUnit( timeSeriesMetadata.getUnit() );
        builder.setVariableName( timeSeriesMetadata.getVariableName() );
        return builder.build();
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

    public TimeSeriesMetadata getMetadata()
    {
        return this.metadata;
    }

    /**
     * Returns the {@link TimeScale} associated with the events or <code>null</code> if {@link #hasTimeScale()} returns
     * <code>false</code>.
     * 
     * @return the time-scale or null
     */

    public TimeScale getTimeScale()
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

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TimeSeries<?> that = ( TimeSeries<?> ) o;
        return metadata.equals( that.metadata ) &&
               events.equals( that.events );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata, events );
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
     * Builds with a builder.
     * 
     * @param builder the builder
     */
    private TimeSeries( TimeSeriesBuilder<T> builder )
    {

        // Set then validate
        // Important to place in a new set here, because the builder uses a special 
        // comparator, based on event time only
        SortedSet<Event<T>>  localEvents = new TreeSet<>();
        localEvents.addAll( builder.events );
        this.events = Collections.unmodifiableSortedSet( localEvents );       

        Map<ReferenceTimeType, Instant> localMap = new EnumMap<>( ReferenceTimeType.class );
        localMap.putAll( builder.referenceTimes );

        TimeScale localTimeScale = null;

        if ( Objects.isNull( builder.timeScale ) )
        {
            LOGGER.trace( "No time-scale information was provided in builder {} for time-series {}.",
                          builder, this );
        }
        else
        {
            localTimeScale = builder.timeScale;
        }

        localMap = Collections.unmodifiableMap( localMap );

        this.metadata = TimeSeriesMetadata.of( localMap,
                                               localTimeScale,
                                               builder.variableName,
                                               builder.featureName,
                                               builder.unit );

        // All reference datetimes and types must be non-null
        for ( Map.Entry<ReferenceTimeType, Instant> nextEntry : this.getReferenceTimes().entrySet() )
        {
            Objects.requireNonNull( nextEntry.getKey() );

            Objects.requireNonNull( nextEntry.getValue() );
        }

        // No null events
        this.getEvents().forEach( Objects::requireNonNull );

    }

    /**
     * Builder that allows for incremental construction and validation. 
     * 
     * @param <T> the type of data
     */

    public static class TimeSeriesBuilder<T>
    {
        /**
         * Events with a prescribed comparator based on event time.
         */

        private final SortedSet<Event<T>> events =
                new TreeSet<>( ( e1, e2 ) -> e1.getTime().compareTo( e2.getTime() ) );

        /**
         * The reference datetime associated with the time-series.
         */

        private final Map<ReferenceTimeType, Instant> referenceTimes = new EnumMap<>( ReferenceTimeType.class );

        /**
         * The time-scale associated with the events.
         */

        private TimeScale timeScale;


        private String variableName;
        private String featureName;
        private String unit;

        /**
         * Adds a reference time.
         * 
         * @param referenceTime the reference time
         * @param referenceTimeType the reference time type
         * @return the builder
         */

        public TimeSeriesBuilder<T> addReferenceTime( Instant referenceTime, ReferenceTimeType referenceTimeType )
        {
            this.referenceTimes.put( referenceTimeType, referenceTime );

            return this;
        }
        
        /**
         * Adds a map of reference times.
         * 
         * @param referenceTimes the reference times
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesBuilder<T> addReferenceTimes( Map<ReferenceTimeType, Instant> referenceTimes )
        {
            Objects.requireNonNull( referenceTimes );
            
            this.referenceTimes.putAll( referenceTimes );

            return this;
        }        

        /**
         * Adds several events to the time-series.
         * 
         * @param events the events
         * @return the builder
         * @throws IllegalArgumentException if the time-series already contains an event at the prescribed time
         */
        
        public TimeSeriesBuilder<T> addEvents( Set<Event<T>> events )
        {
            events.stream().forEach( this::addEvent );
            
            return this;
        }
        
        /**
         * Adds an event.
         * 
         * @param event the event
         * @return the builder
         * @throws IllegalArgumentException if the time-series already contains an event at the prescribed time
         */

        public TimeSeriesBuilder<T> addEvent( Event<T> event )
        {
            if ( this.events.contains( event ) )
            {
                throw new IllegalArgumentException( "Attemped to add an event at the same valid datetime as an "
                                                    + "existing event, which is not allowed. The duplicate event "
                                                    + "by time is '"
                                                    + event
                                                    + "'." );
            }

            this.events.add( event );

            return this;
        }

        /**
         * Adds the time-scale information.
         * 
         * @param timeScale the time scale
         * @return the builder
         */

        public TimeSeriesBuilder<T> setTimeScale( TimeScale timeScale )
        {
            this.timeScale = timeScale;

            return this;
        }

        public TimeSeriesBuilder<T> setVariableName( String variableName )
        {
            this.variableName = variableName;
            return this;
        }

        public TimeSeriesBuilder<T> setFeatureName( String featureName )
        {
            this.featureName = featureName;
            return this;
        }

        public TimeSeriesBuilder<T> setUnit( String unit )
        {
            this.unit = unit;
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


}

