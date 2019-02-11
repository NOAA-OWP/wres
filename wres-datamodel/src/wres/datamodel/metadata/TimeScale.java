package wres.datamodel.metadata;

import java.time.Duration;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.math3.exception.MathArithmeticException;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.TimeScaleConfig;

/**
 * <p>Metadata that describes the time scale associated with each value in a time-series. The time scale is described 
 * by:</p>
 * 
 * <ol>
 * <li>The period to which the value refers, which is also known as the "support" of the value.</li>
 * <li>The name of the mathematical function whose output corresponds to the value over the prescribed period.</li>
 * </ol>
 * 
 * <p>The period is represented by a {@link Duration}. The function is represented by a {@link TimeScaleFunction}.
 * </p>
 * 
 * <p>Each value in a time-series is associated with a specific datetime. The time scale does not describe the 
 * orientation of the period with respect to that datetime. Similarly, the time scale does not describe whether the 
 * earliest or latest datetime implied by the period was included or excluded in the input to the function from which 
 * the value was produced, only that the period has a specific duration. Both of these attributes are determined by 
 * convention. Additionally, the meaning of "instantaneous" is defined by convention.</p>
 * 
 * <p>As of 20 August 2018, the convention adopted by the wres is that values "end at" the specified datetime. In 
 * other words, the datetime represents the right bookened of the period to which the value refers. The treatment of 
 * the left and right bookends as including or excluding the datetime at which they begin and end, respectively, is 
 * undefined. Finally, the meaning of "instantaneous" is a period that corresponds to a duration of one second, 
 * regardless of the function.</p>
 * 
 * <p>Further information can be found in #44539.</p>
 *
 * <p>This class is immutable and thread-safe.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public final class TimeScale implements Comparable<TimeScale>
{

    /**
     * An enumeration of mathematical functions. A time-series value corresponds to the output from a function over 
     * a prescribed period.
     */

    public enum TimeScaleFunction
    {
        /**
         * The time scale function is unknown.
         */

        UNKNOWN,

        /**
         * The time scale function is a mean average over the period.
         */

        MEAN,

        /**
         * The time scale function is the minimum value recorded within the period.
         */

        MINIMUM,

        /**
         * The time scale function is the maximum value recorded within the period.
         */

        MAXIMUM,

        /**
         * The time scale function is the accumulated value over the period.
         */

        TOTAL;

    }

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScale.class );

    /**
     * The period to which each value in a time-series of values applies.
     */

    private final Duration period;

    /**
     * The name of the mathematical function whose output produced the value over the prescribed period.
     */

    private final TimeScaleFunction function;

    /**
     * Constructs a {@link TimeScale} from a period and a function that is {@link TimeScaleFunction#UNKNOWN}.
     * 
     * @param period the period
     * @return a time scale
     * @throws NullPointerException if the input is null
     */

    public static TimeScale of( Duration period )
    {
        return new TimeScale( period, TimeScaleFunction.UNKNOWN );
    }

    /**
     * Constructs a {@link TimeScale} with a period and a function.
     * 
     * @param period the period
     * @param function the function
     * @return a time scale
     * @throws NullPointerException if either input is null
     */

    public static TimeScale of( Duration period, TimeScaleFunction function )
    {
        return new TimeScale( period, function );
    }

    /**
     * Constructs a {@link TimeScale} from a {@link TimeScaleConfig}. If the {@link TimeScaleConfig#getFunction()}
     * is null, the {@link TimeScaleFunction} is set to {@link TimeScaleFunction#UNKNOWN}.
     * 
     * @param config the configuration
     * @return a time scale
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the enum that describes the function associated with the input is non-null 
     *            and its name does not match the name of a {@link TimeScaleFunction}
     */

    public static TimeScale of( TimeScaleConfig config )
    {
        Objects.requireNonNull( config, "Specify non-null time-series configuration" );

        Duration period = ProjectConfigs.getDurationFromTimeScale( config );

        TimeScaleFunction function = null;

        // TODO: NONE will be removed by #48232
        if ( Objects.isNull( config.getFunction() ) ||
             config.getFunction() == wres.config.generated.TimeScaleFunction.NONE )
        {
            function = TimeScaleFunction.UNKNOWN;
        }
        else
        {
            function = TimeScaleFunction.valueOf( config.getFunction().name() );
        }

        return new TimeScale( period, function );
    }

    /**
     * Returns the period associated with the time scale
     * 
     * @return the period
     */

    public Duration getPeriod()
    {
        return period;
    }

    /**
     * Returns the function associated with the time scale.
     * 
     * @return the function
     */

    public TimeScaleFunction getFunction()
    {
        return function;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeScale ) )
        {
            return false;
        }
        TimeScale in = (TimeScale) o;

        return in.getPeriod().equals( this.getPeriod() ) && in.getFunction() == this.getFunction();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( period, function );
    }

    @Override
    public String toString()
    {
        if ( this.isInstantaneous() )
        {
            return "[INSTANTANEOUS]";
        }
        return "[" + period + "," + function + "]";
    }

    @Override
    public int compareTo( TimeScale o )
    {
        Objects.requireNonNull( o );

        int returnMe = this.getPeriod().compareTo( o.getPeriod() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        return this.getFunction().compareTo( o.getFunction() );
    }

    /**
     * Helper that returns <code>true</code> if this time scale is effectively "instantaneous", otherwise 
     * <code>false</code>.
     * 
     * A time scale is considered "instantaneous" if the period associated with the time scale is less than or 
     * equal to sixty seconds. 
     * 
     * @return true if the period is less than or equal to 60 seconds, otherwise false.
     */

    public boolean isInstantaneous()
    {
        return this.getPeriod().compareTo( Duration.ofSeconds( 60 ) ) <= 0;
    }


    /**
     * <p>Computes the Least Common Multiple or Least Common Scale (LCS) of the inputs at a time resolution of seconds. 
     * The LCS is the integer number of seconds that is a common multiple of all of the inputs.
     * 
     * <p>When the input contains an instantaneous time scale (see {@link TimeScale#isInstantaneous()}), then this
     * method either returns the other time scale present or throws an exception if more than one additional time 
     * scale is present. However, there is no other validation of the proposed rescaling, such as the proposed 
     * {@link TimeScale#getFunction()} associated with the rescaled quantity. 
     * For that, and other validation see:
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, Duration)}.
     * 
     * @param timeScales the time scales from which to derive the LCS
     * @return the LCS for the input
     * @throws RescalingException if the input contains more than one scale function or one function plus a time scale
     *            that represents instantaneous data or if the LCS could not be calculated from the input
     * @throws NullPointerException if the inputs are null
     * @throws IllegalArgumentException if the input is empty
     */

    public static TimeScale getLeastCommonTimeScale( Set<TimeScale> timeScales )
    {
        Objects.requireNonNull( timeScales, "Cannot compute the Least Common Scale from null input." );

        if ( timeScales.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot compute the Least Common Scale from empty input." );
        }

        Set<TimeScaleFunction> functions =
                timeScales.stream().map( TimeScale::getFunction ).collect( Collectors.toCollection( TreeSet::new ) );

        // Only allowed one function unless instantaneous data is present, in which case only two
        if ( functions.size() > 2
             || ( functions.size() == 2 && timeScales.stream().noneMatch( TimeScale::isInstantaneous ) ) )
        {
            throw new RescalingException( "Could not determine the Least Common Scale from the input. Expected input "
                                          + "with only one scale function that does not correspond to an instantaneous "
                                          + "time scale. Instead found "
                                          + functions
                                          + "." );
        }

        // Only one, then that must be the LCS
        if ( timeScales.size() == 1 )
        {
            // Worth logging this situation
            LOGGER.debug( "When computing the Least Common Scale, found only one time scale in the input." );

            return timeScales.iterator().next();
        }

        // If the input contains an instantaneous time scale, then return the only non-instantaneous one present
        // The earlier validation guarantees the latter
        if ( timeScales.stream().anyMatch( TimeScale::isInstantaneous ) )
        {
            Optional<TimeScale> nonInst = timeScales.stream().filter( scale -> !scale.isInstantaneous() ).findFirst();
            if ( nonInst.isPresent() )
            {
                return nonInst.get();
            }
        }

        // Compute the LCM of the durations
        Duration lcm = TimeScale.getLeastCommonDuration( timeScales.stream()
                                                                   .map( TimeScale::getPeriod )
                                                                   .collect( Collectors.toSet() ) );

        return of( lcm, timeScales.iterator().next().getFunction() );
    }

    /**
     * <p>Computes the Least Common Multiple of the inputs at a time resolution of seconds. 
     * The LCM is the integer number of seconds that is a common multiple of all of the inputs.
     * 
     * TODO: consider moving this to a more general time-utility class, as it is independent
     * of time scale.
     * 
     * @param durations the time scales from which to derive the LCM
     * @return the LCM for the input
     * @throws NullPointerException if the inputs are null
     * @throws IllegalArgumentException if the input is empty
     */

    public static Duration getLeastCommonDuration( Set<Duration> durations )
    {
        Objects.requireNonNull( durations, "Cannot compute the Least Common Duration from null input." );

        if ( durations.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot compute the Least Common Duration from empty input." );
        }

        // Only one, then that must be the LCM
        if ( durations.size() == 1 )
        {
            // Worth logging this situation
            LOGGER.debug( "When computing the Least Common Duration, found only one duration in the input." );

            return durations.iterator().next();
        }

        // Compute the LCS from two or more time scales
        Iterator<Duration> it = durations.iterator();
        long lcs = it.next().getSeconds();
        while ( it.hasNext() )
        {
            try
            {
                lcs = ArithmeticUtils.lcm( lcs, it.next().getSeconds() );
            }
            // Decorate
            catch ( MathArithmeticException e )
            {
                throw new RescalingException( "While attempting to compute the Least Common Duration from the input:",
                                              e );
            }
        }

        return Duration.ofSeconds( lcs );
    }

    /**
     * Hidden constructor.
     * 
     * @param period the positive period
     * @param function the function
     * @throws IllegalArgumentException if the period is zero or negative
     * @throws NullPointerException if either input is null
     */

    private TimeScale( Duration period,
                       TimeScaleFunction function )
    {
        Objects.requireNonNull( period, "Specify a non-null period for the time scale." );

        Objects.requireNonNull( function, "Specify a non-null function for the time scale." );

        if ( period.isZero() )
        {
            throw new IllegalArgumentException( "Cannot build a time scale with a period of zero." );
        }

        if ( period.isNegative() )
        {
            throw new IllegalArgumentException( "Cannot build a time scale with a negative period." );
        }

        this.period = period;
        this.function = function;
    }

}
