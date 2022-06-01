package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;

import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Ensemble} 
 * values. Makes the same assumptions as the {@link TimeSeriesOfDoubleUpscaler}, but additionally requires that every
 * {@link Ensemble} contains the same number of ensemble members.
 * 
 * @author James Brown
 */

public class TimeSeriesOfEnsembleUpscaler implements TimeSeriesUpscaler<Ensemble>
{
    /**
     * Lenient means that upscaling can proceed when values that match the {@link MissingValues#DOUBLE} are encountered
     * or when the values to upscale are spaced irregularly over the interval (e.g., because data is implicitly 
     * missing).
     */

    private final boolean isLenient;

    /**
     * Function that returns a double value or {@link MissingValues#DOUBLE} if the
     * input is not finite. 
     */

    private static final DoubleUnaryOperator RETURN_DOUBLE_OR_MISSING =
            a -> Double.isFinite( a ) ? a : MissingValues.DOUBLE;

    /**
     * Creates an instance that enforces strict upscaling or no leniency.
     * 
     * @see #of(boolean)
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of()
    {
        return new TimeSeriesOfEnsembleUpscaler( false );
    }

    /**
     * Creates an instance with a prescribed leniency. Lenient upscaling means that missing data does not prevent 
     * upscaling and that irregularly spaced data (which could indicate implicitly missing values) does not prevent 
     * upscaling. Data is explicitly missing if it matches the {@link MissingValues#DOUBLE}.
     * 
     * @param isLenient is {@code true} to enforce lenient upscaling, {@code false} otherwise
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of( boolean isLenient )
    {
        return new TimeSeriesOfEnsembleUpscaler( isLenient );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                                               TimeScaleOuter desiredTimeScale )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySortedSet() );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                                               TimeScaleOuter desiredTimeScale,
                                                               SortedSet<Instant> endsAt )
    {
        Objects.requireNonNull( desiredTimeScale );

        TimeScaleFunction desiredFunction = desiredTimeScale.getFunction();
        Function<SortedSet<Event<Ensemble>>, Ensemble> upscaler = this.getEnsembleUpscaler( desiredFunction );

        return RescalingHelper.upscale( timeSeries, upscaler, desiredTimeScale, endsAt, this.isLenient() );
    }

    /**
     * Returns the ensemble upscaler used by this instance.
     * 
     * @param function the time-scale function
     * @return the ensemble upscaler
     */

    private Function<SortedSet<Event<Ensemble>>, Ensemble> getEnsembleUpscaler( TimeScaleFunction function )
    {
        ToDoubleFunction<List<Double>> upscaler = this.getTraceUpscaler( function );

        return events -> {

            int memberCount = this.getMemberCountAndValidateConstant( events );

            Labels labels = events.last()
                                  .getValue()
                                  .getLabels();

            double[] upscaled = new double[memberCount];

            for ( int i = 0; i < memberCount; i++ )
            {
                int nextIndex = i;
                List<Double> doubles = events.stream()
                                             .map( next -> next.getValue().getMembers()[nextIndex] )
                                             .collect( Collectors.toList() );

                double nextUpscaled = upscaler.applyAsDouble( doubles );
                upscaled[i] = nextUpscaled;
            }

            return Ensemble.of( upscaled, labels );
        };
    }

    /**
     * Returns the number of ensemble members associated with every ensemble in the set of events.
     * 
     * @param events the events
     * @return the number of ensemble members
     * @throws UnsupportedOperationException if the number of ensemble members is not fixed
     */
    private int getMemberCountAndValidateConstant( SortedSet<Event<Ensemble>> events )
    {
        // A map of ensemble members per valid time organized by label or index
        Set<Integer> counts =
                events.stream()
                      .map( next -> next.getValue()
                                        .size() )
                      .collect( Collectors.toSet() );

        // No labels, so check for a constant number of ensemble members
        if ( counts.size() > 1 )
        {
            throw new UnsupportedOperationException( "Encountered a collection of ensembles to upscale that contained "
                                                     + "a varying number of ensemble members, which is not supported. "
                                                     + "The ensemble member counts were: "
                                                     + counts
                                                     + "." );
        }

        return counts.stream()
                     .findAny()
                     .orElse( 0 );
    }

    /**
     * @return {@code true} if lenient upscaling is required, {@code false} otherwise.
     */

    private boolean isLenient()
    {
        return this.isLenient;
    }

    /**
     * Returns a function that corresponds to a {@link TimeScaleFunction}, additionally wrapped by 
     * {@link #RETURN_DOUBLE_OR_MISSING} so that missing input produces missing output.
     * 
     * @param function The nominated function
     * @return a function for upscaling
     * @throws UnsupportedOperationException if the nominated function is not recognized
     */

    private ToDoubleFunction<List<Double>> getTraceUpscaler( TimeScaleFunction function )
    {
        return events -> {

            double upscaled;

            List<Double> eventsToUse = events;

            if ( this.isLenient() )
            {
                eventsToUse = eventsToUse.stream()
                                         .filter( Double::isFinite )
                                         .collect( Collectors.toList() );
            }

            switch ( function )
            {
                case MAXIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .max()
                                          .getAsDouble();
                    break;
                case MEAN:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .average()
                                          .getAsDouble();
                    break;
                case MINIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .min()
                                          .getAsDouble();
                    break;
                case TOTAL:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
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
     * Hidden constructor.
     * 
     * @param isLenient is true if the lenient upscaling is required, false otherwise
     */

    private TimeSeriesOfEnsembleUpscaler( boolean isLenient )
    {
        this.isLenient = isLenient;
    }

}
