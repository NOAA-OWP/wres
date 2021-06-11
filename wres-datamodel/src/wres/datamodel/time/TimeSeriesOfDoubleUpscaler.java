package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.MissingValues;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Double} 
 * values. An upscaled value is produced from a collection of values that fall within an interval that ends at a 
 * prescribed time. The interval has the same width as the period associated with the desired {@link TimeScaleOuter}. If 
 * the events are not evenly spaced within the interval, that interval is skipped and logged. If any event value is 
 * non-finite, then the upscaled event value is {@link MissingValues#DOUBLE}. The interval is right-closed, 
 * i.e. <code>(end-period,end]</code>. Thus, for example, when upscaling a sequence of instantaneous values 
 * (0Z,6Z,12Z,18Z,0Z] to form an average that ends at 0Z and spans a period of PT24H, the four-point average is taken 
 * for the values at 6Z, 12Z, 18Z and 0Z and not the five-point average. Indeed, if these values represented an average 
 * over PT1H, rather than instantaneous values, then the five-point average would consider a PT25H period.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesOfDoubleUpscaler implements TimeSeriesUpscaler<Double>
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
     * @return an instance of the upscaler
     */

    public static TimeSeriesOfDoubleUpscaler of()
    {
        return new TimeSeriesOfDoubleUpscaler();
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScaleOuter desiredTimeScale )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySortedSet() );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScaleOuter desiredTimeScale,
                                                             SortedSet<Instant> endsAt )
    {       
        Objects.requireNonNull( desiredTimeScale );

        TimeScaleFunction desiredFunction = desiredTimeScale.getFunction();
        ToDoubleFunction<SortedSet<Event<Double>>> upscaler = this.getUpscaler( desiredFunction );
        
        return RescalingHelper.upscale( timeSeries, upscaler::applyAsDouble, desiredTimeScale, endsAt );
    }

    /**
     * Returns a function that corresponds to a {@link TimeScaleFunction}, additionally wrapped by 
     * {@link #RETURN_DOUBLE_OR_MISSING} so that missing input produces missing output.
     * 
     * @param function The nominated function
     * @return a function for upscaling
     * @throws UnsupportedOperationException if the nominated function is not recognized
     */

    private ToDoubleFunction<SortedSet<Event<Double>>> getUpscaler( TimeScaleFunction function )
    {
        return events -> {

            double upscaled;

            SortedSet<Event<Double>> eventsToUse = events;

            if ( TimeSeriesOfDoubleUpscaler.LENIENT )
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
     * Hidden constructor.
     */

    private TimeSeriesOfDoubleUpscaler()
    {
    }

}
