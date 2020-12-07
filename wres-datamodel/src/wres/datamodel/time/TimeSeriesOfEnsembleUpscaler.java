package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Ensemble} 
 * values. Makes the same assumptions as the {@link TimeSeriesOfDoubleUpscaler}, but additionally requires that every
 * {@link Ensemble} contains the same number of ensemble members.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesOfEnsembleUpscaler implements TimeSeriesUpscaler<Ensemble>
{

    /**
     * Lenient on values that match the {@link MissingValues#DOUBLE}? TODO: expose this to declaration.
     */

    private static final boolean LENIENT = false;

    /**
     * Function that returns a double value or {@link MissingValues#DOUBLE} if the
     * input is not finite. 
     */

    private static final DoubleUnaryOperator RETURN_DOUBLE_OR_MISSING =
            a -> Double.isFinite( a ) ? a : MissingValues.DOUBLE;

    /**
     * Creates an instance.
     * 
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of()
    {
        return new TimeSeriesOfEnsembleUpscaler();
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                                               TimeScaleOuter desiredTimeScale )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySet() );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                                               TimeScaleOuter desiredTimeScale,
                                                               Set<Instant> endsAt )
    {
        Objects.requireNonNull( desiredTimeScale );

        TimeScaleFunction desiredFunction = desiredTimeScale.getFunction();
        Function<SortedSet<Event<Ensemble>>, Ensemble> upscaler = this.getEnsembleUpscaler( desiredFunction );

        return RescalingHelper.upscale( timeSeries, upscaler, desiredTimeScale, endsAt );
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

            Optional<String[]> labels = events.last()
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

            if ( labels.isPresent() )
            {
                return Ensemble.of( upscaled, labels.get() );
            }

            return Ensemble.of( upscaled );
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

            if ( TimeSeriesOfEnsembleUpscaler.LENIENT )
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
     * @throws NullPointerException of the input is null
     */

    private TimeSeriesOfEnsembleUpscaler()
    {
    }

}
