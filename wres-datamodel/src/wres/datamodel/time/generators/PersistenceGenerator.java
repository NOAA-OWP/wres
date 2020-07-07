package wres.datamodel.time.generators;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;

/**
 * <p>Generates a persistence time-series from a source of persistence data supplied on construction. The shape of the 
 * persistence series is obtained from a template time-series supplied on demand.
 * 
 * <p>Other implementations of generated forecasts, such as climatology, can be represented with the same API.
 * 
 * @author james.brown@hydrosolved.com
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

        if ( referenceTimes.isEmpty() )
        {
            throw new TimeSeriesGeneratorException( "While attempting to generating a persistence time-series: the "
                                                    + "input time-series "
                                                    + template.hashCode()
                                                    + " does not contain one or more reference times, which is not "
                                                    + "allowed." );
        }

        if ( template.getEvents().isEmpty() )
        {
            LOGGER.trace( WHILE_GENERATING_A_PERSISTENCE_TIME_SERIES_USING_INPUT_SERIES,
                          template.hashCode(),
                          ", discovered that the input series had no events (i.e., was empty). Returning the same, "
                                               + "empty, series." );

            return template;
        }

        Optional<T> persist = this.getPersistence( template );

        TimeSeriesMetadata templateMetadata = template.getMetadata();
        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();
        builder.setMetadata( templateMetadata );

        // Persistence value available?
        if ( persist.isPresent() )
        {
            T value = persist.get();

            // Persistence value admissible?
            if ( Objects.nonNull( this.admissibleValue )
                 && !admissibleValue.test( value )
                 && LOGGER.isTraceEnabled() )
            {
                int seriesCode = template.hashCode();

                LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                              + "persistent value of {} was inadmissible. Unable to create a persistence time-series "
                              + "from input series {}. Returning the empty time-series instead.",
                              seriesCode,
                              value,
                              seriesCode );
            }
            else
            {
                for ( Event<T> next : template.getEvents() )
                {
                    builder.addEvent( Event.of( next.getTime(), value ) );
                }
            }
        }

        return builder.build();
    }

    /**
     * Returns a persistence value at the lag provided on construction relative to the reference time of the input
     * series.
     * 
     * @param template the template series
     * @return the persistence value at the lag supplied on construction
     * @throws TimeSeriesGeneratorException if the persistence value could not be generated
     */

    private Optional<T> getPersistence( TimeSeries<T> template )
    {

        Map<ReferenceTimeType, Instant> referenceTimes = template.getReferenceTimes();

        // Take the first available reference time
        // If multiple reference times are provided in future, adapt
        if ( referenceTimes.size() > 1 && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While generating a persistence time-series using input series {}, discovered that the "
                          + "input series has multiple reference times. Using the first time of {}.",
                          template.hashCode(),
                          referenceTimes.values().iterator().next() );
        }

        // Compute the instant at which the persistence value should end
        Instant referenceTime = referenceTimes.values().iterator().next();
        Instant endsAt = this.getNthNearestValueInstant( referenceTime, this.order );

        TimeSeries<T> persistenceSeries = this.persistenceSource;

        // Upscale? 
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.persistenceSource.getTimeScale() )
             && !desiredTimeScale.equals( this.persistenceSource.getTimeScale() ) )
        {
            if ( Objects.isNull( this.upscaler ) )
            {
                throw new TimeSeriesGeneratorException( "While generating a persistence time-series using input series "
                                                        + template.hashCode()
                                                        + ", discovered that the input series had a desired time scale "
                                                        + "of "
                                                        + desiredTimeScale
                                                        + ", but the "
                                                        + "persistence source had a desired time scale of "
                                                        + persistenceSeries.getTimeScale()
                                                        + " and no temporal upscaler was supplied on construction." );
            }

            persistenceSeries = this.upscaler.upscale( persistenceSeries,
                                                       desiredTimeScale,
                                                       Set.of( endsAt ) )
                                             .getTimeSeries();
        }

        // Finds the value that ends at the required time
        Optional<T> value = persistenceSeries.getEvents()
                                             .stream()
                                             .filter( in -> in.getTime().equals( endsAt ) )
                                             .map( Event::getValue )
                                             .findFirst();

        if ( !value.isPresent() && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While attempting to generate a persistence value that ends at time {} from the reference "
                          + "time of {}, failed to find the corresponding time in the persistence source, which "
                          + "contained {} values.",
                          endsAt,
                          referenceTime,
                          this.persistenceSource.getEvents().size() );
        }

        return value;
    }

    /**
     * Returns the time in the persistence source that is Nth nearest to, and earlier than, the reference time, where N 
     * is the order of persistence.
     * 
     * @param reference time the reference time
     * @param order the order of persistence
     * @return the time-step
     */

    private Instant getNthNearestValueInstant( Instant referenceTime, int order )
    {
        // Put the persistence values into a list. There are at least N+1 values in the list, established at 
        // construction
        List<Event<T>> events = this.persistenceSource.getEvents()
                                                      .stream()
                                                      .collect( Collectors.toList() );

        Instant returnMe = null;
        Instant lastTime = events.get( 0 ).getTime();
        for ( int i = order; i < events.size(); i++ )
        {
            Instant currentTime = events.get( i ).getTime();

            // Stop counting when the reference time has been equalled or exceeded
            if ( currentTime.equals( referenceTime ) || currentTime.isAfter( referenceTime ) )
            {
                returnMe = lastTime;
                break;
            }

            lastTime = currentTime;
        }

        return returnMe;
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

        if ( this.persistenceSource.getEvents().size() < ( order + 1 ) )
        {
            throw new TimeSeriesGeneratorException( "Could not create a persistence source from the time-series "
                                                    + "supplier: at least "
                                                    + ( order + 1 )
                                                    + " time-series values are "
                                                    + "required to generate a persistence time-series of order "
                                                    + order
                                                    + " but the supplier only contained "
                                                    + this.persistenceSource.getEvents().size()
                                                    + " values. " );
        }

        this.upscaler = upscaler;
        this.admissibleValue = admissibleValue;
    }

}
