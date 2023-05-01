package wres.datamodel.scale;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.xml.ProjectConfigs;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.messages.MessageUtilities;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * <p>Metadata that describes the timescale associated with each value in a time-series. Wraps a canonical
 * {@link TimeScale}, adding behavior
 *
 * <p>As of 20 August 2018, the convention adopted by the WRES is that time series values "end at" the specified valid 
 * time. In other words, the datetime represents the right bookened of the time scale to which the value refers. The 
 * treatment of the left and right bookends as including or excluding the datetime at which they begin and end, 
 * respectively, is undefined. Finally, the meaning of an "instantaneous" time scale is given by 
 * {@link #INSTANTANEOUS_DURATION}.
 *
 * <p>Further information can be found in #44539.
 *
 * @author James Brown
 */
@Immutable
public final class TimeScaleOuter implements Comparable<TimeScaleOuter>
{
    /** Upper bound (inclusive) for an instantaneous duration. */
    public static final Duration INSTANTANEOUS_DURATION = Duration.ofSeconds( 60 );

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleOuter.class );

    /** The canonical representation of a timescale. */
    private final TimeScale timeScale;

    /**
     * Constructs a {@link TimeScaleOuter} whose {@link TimeScaleOuter#isInstantaneous()} returns 
     * <code>true</code>.
     *
     * @return an instantaneous timescale
     */

    public static TimeScaleOuter of()
    {
        return TimeScaleOuter.of( Duration.ofMillis( 1 ) );
    }

    /**
     * Constructs a {@link TimeScaleOuter} from a period and a function that is {@link TimeScaleFunction#UNKNOWN}.
     *
     * @param period the period
     * @return a time scale
     * @throws NullPointerException if the input is null
     */

    public static TimeScaleOuter of( Duration period )
    {
        Objects.requireNonNull( period );

        TimeScale timeScaleInner =
                TimeScale.newBuilder()
                         .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                 .setSeconds( period.getSeconds() )
                                                                 .setNanos( period.getNano() ) )
                         .setFunction( wres.statistics.generated.TimeScale.TimeScaleFunction.UNKNOWN )
                         .build();

        return new TimeScaleOuter( timeScaleInner );
    }

    /**
     * Constructs a {@link TimeScaleOuter} from a {@link TimeScale}.
     *
     * @param timeScale the time scale
     * @return a time scale
     * @throws NullPointerException if the input is null
     */

    public static TimeScaleOuter of( TimeScale timeScale )
    {
        return new TimeScaleOuter( timeScale );
    }

    /**
     * Constructs a {@link TimeScaleOuter} with a period and a function.
     *
     * @param period the period
     * @param function the function
     * @return a time scale
     * @throws NullPointerException if either input is null
     */

    public static TimeScaleOuter of( Duration period, TimeScaleFunction function )
    {
        Objects.requireNonNull( period );
        Objects.requireNonNull( function );

        TimeScale timeScaleInner =
                TimeScale.newBuilder()
                         .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                 .setSeconds( period.getSeconds() )
                                                                 .setNanos( period.getNano() ) )
                         .setFunction( function )
                         .build();

        return new TimeScaleOuter( timeScaleInner );
    }

    /**
     * Constructs a {@link TimeScaleOuter} from a {@link TimeScaleConfig}. If the {@link TimeScaleConfig#getFunction()}
     * is null, the {@link TimeScaleFunction} is set to {@link TimeScaleFunction#UNKNOWN}.
     *
     * @param config the configuration
     * @return a time scale
     * @throws NullPointerException if either input is null or expected contents is null
     * @throws IllegalArgumentException if the enum that describes the function associated with the input is non-null 
     *            and its name does not match the name of a {@link TimeScaleFunction}
     */

    public static TimeScaleOuter of( TimeScaleConfig config )
    {
        Objects.requireNonNull( config, "Specify non-null time-series configuration" );

        TimeScale.Builder timeScaleInner = TimeScale.newBuilder();

        if ( Objects.nonNull( config.getPeriod() ) )
        {
            Duration period = ProjectConfigs.getDurationFromTimeScale( config );
            com.google.protobuf.Duration canonicalPeriod = MessageFactory.parse( period );
            timeScaleInner.setPeriod( canonicalPeriod );
        }

        if ( Objects.isNull( config.getFunction() ) )
        {
            timeScaleInner.setFunction( TimeScaleFunction.UNKNOWN );
        }
        else
        {
            wres.statistics.generated.TimeScale.TimeScaleFunction innerFunction =
                    wres.statistics.generated.TimeScale.TimeScaleFunction.valueOf( config.getFunction().name() );
            timeScaleInner.setFunction( innerFunction );
        }

        if ( config instanceof DesiredTimeScaleConfig desiredConfig )
        {
            if ( Objects.nonNull( desiredConfig.getEarliestDay() ) )
            {
                timeScaleInner.setStartDay( desiredConfig.getEarliestDay() );
            }

            if ( Objects.nonNull( desiredConfig.getEarliestMonth() ) )
            {
                timeScaleInner.setStartMonth( desiredConfig.getEarliestMonth() );
            }

            if ( Objects.nonNull( desiredConfig.getLatestDay() ) )
            {
                timeScaleInner.setEndDay( desiredConfig.getLatestDay() );
            }

            if ( Objects.nonNull( desiredConfig.getLatestMonth() ) )
            {
                timeScaleInner.setEndMonth( desiredConfig.getLatestMonth() );
            }
        }

        return new TimeScaleOuter( timeScaleInner.build() );
    }

    /**
     * @return true if the period associated with the time scale is defined explicitly using a combination of start and
     * end month-days, false if an explicit period is defined
     */

    public boolean hasPeriod()
    {
        return this.timeScale.hasPeriod();
    }

    /**
     * @return true if one or both month-days is present, false otherwise
     */

    public boolean hasMonthDays()
    {
        return Objects.nonNull( this.getStartMonthDay() ) || Objects.nonNull( this.getEndMonthDay() );
    }

    /**
     * Returns the explicit period associated with the time scale or null if an implicit period is defined. An implicit
     * period is a period separated by two month-days.
     *
     * @return the period
     */

    public Duration getPeriod()
    {
        Duration period = null;

        if ( this.hasPeriod() )
        {
            period = Duration.ofSeconds( this.getTimeScale().getPeriod().getSeconds(),
                                         this.getTimeScale().getPeriod().getNanos() );
        }

        return period;
    }

    /**
     * Returns the function associated with the time scale.
     *
     * @return the function
     */

    public TimeScaleFunction getFunction()
    {
        return this.getTimeScale()
                   .getFunction();
    }

    /**
     * Returns the canonical representation of a time scale.
     *
     * @return the canonical representation
     */

    public TimeScale getTimeScale()
    {
        return this.timeScale;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( !( o instanceof TimeScaleOuter in ) )
        {
            return false;
        }

        return Objects.equals( in.getTimeScale(), this.getTimeScale() );
    }

    /**
     * A lenient equals that considers {@link #isInstantaneous()} as sufficient for equality, otherwise 
     * {@link #equals(Object)}.
     *
     * @param o the object to test for equality against this instance
     * @return true if the input and existing timescale are instantaneous or content equal, otherwise false
     */

    public boolean equalsOrInstantaneous( Object o )
    {
        if ( o instanceof TimeScaleOuter timeScaleOuter && timeScaleOuter.isInstantaneous() && this.isInstantaneous() )
        {
            return true;
        }

        return this.equals( o );
    }

    @Override
    public int hashCode()
    {
        return this.getTimeScale().hashCode();
    }

    @Override
    public String toString()
    {
        if ( this.isInstantaneous() )
        {
            return "[INSTANTANEOUS]";
        }

        StringJoiner joiner = new StringJoiner( ",", "[", "]" );

        if ( this.hasPeriod() )
        {
            joiner.add( this.getPeriod() + "" );
        }

        joiner.add( this.getFunction().toString() );

        MonthDay startMonthDay = this.getStartMonthDay();

        if ( Objects.nonNull( startMonthDay ) )
        {
            joiner.add( startMonthDay.toString() );
        }

        MonthDay endMonthDay = this.getEndMonthDay();

        if ( Objects.nonNull( endMonthDay ) )
        {
            joiner.add( endMonthDay.toString() );
        }

        return joiner.toString();
    }

    @Override
    public int compareTo( TimeScaleOuter o )
    {
        return MessageUtilities.compare( this.timeScale, o.getTimeScale() );
    }

    /**
     * <p>Helper that returns <code>true</code> if this timescale is effectively "instantaneous", otherwise
     * <code>false</code>.
     *
     * <p>A timescale is considered "instantaneous" if the period associated with the timescale is less than or
     * equal to sixty seconds.
     *
     * @return true if the period is less than or equal to 60 seconds, otherwise false.
     */

    public boolean isInstantaneous()
    {
        // Explicit period
        if ( this.hasPeriod() )
        {
            return INSTANTANEOUS_DURATION.compareTo( this.getPeriod() ) >= 0;
        }

        // An implicit period with equal earliest and latest month-days is not allowed
        return false;
    }

    /**
     * @return the start month-day associated with the time scale or null if none is defined
     */

    public MonthDay getStartMonthDay()
    {
        MonthDay startMonthDay = null;

        if ( this.timeScale.getStartDay() != 0 && this.timeScale.getStartMonth() != 0 )
        {
            startMonthDay = MonthDay.of( this.timeScale.getStartMonth(), this.timeScale.getStartDay() );
        }

        return startMonthDay;
    }

    /**
     * @return the end month-day associated with the time scale or null if none is defined
     */

    public MonthDay getEndMonthDay()
    {
        MonthDay endMonthDay = null;

        if ( this.timeScale.getEndDay() != 0 && this.timeScale.getEndMonth() != 0 )
        {
            endMonthDay = MonthDay.of( this.timeScale.getEndMonth(), this.timeScale.getEndDay() );
        }

        return endMonthDay;
    }

    /**
     * <p>Computes the Least Common Multiple or Least Common Scale (LCS) of the inputs at a time resolution of 
     * milliseconds. The LCS is the integer number of milliseconds that is a common multiple of all of the inputs.
     *
     * <p>When the input contains an instantaneous time scale (see {@link TimeScaleOuter#isInstantaneous()}), then this
     * method either returns the other time scale present or throws an exception if more than one additional time 
     * scale is present. However, there is no other validation of the proposed rescaling, such as the proposed 
     * {@link TimeScaleOuter#getFunction()} associated with the rescaled quantity.
     *
     * @param timeScales the time scales from which to derive the LCS
     * @return the LCS for the input
     * @throws RescalingException if the input contains more than one scale function or one function plus a time scale
     *            that represents instantaneous data or if the LCS could not be calculated from the input
     * @throws NullPointerException if the inputs are null
     * @throws IllegalArgumentException if the input is empty or if one or more time scales have an implicit period
     */

    public static TimeScaleOuter getLeastCommonTimeScale( Set<TimeScaleOuter> timeScales )
    {
        Objects.requireNonNull( timeScales, "Cannot compute the Least Common Scale from null input." );

        if ( timeScales.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot compute the Least Common Scale from empty input." );
        }

        // All time scales must have an explicit period
        if ( timeScales.stream()
                       .anyMatch( next -> !next.hasPeriod() ) )
        {
            throw new IllegalArgumentException( "Cannot compute the Least Common Scale from one or more individual "
                                                + "time scales that do not have an explicit time scale period." );
        }

        Set<TimeScaleFunction> functions =
                timeScales.stream()
                          .map( TimeScaleOuter::getFunction )
                          .collect( Collectors.toCollection( TreeSet::new ) );

        // Only allowed one function unless instantaneous data is present, in which case only two
        if ( functions.size() > 2
             || ( functions.size() == 2 && timeScales.stream().noneMatch( TimeScaleOuter::isInstantaneous ) ) )
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
        if ( timeScales.stream().anyMatch( TimeScaleOuter::isInstantaneous ) )
        {
            Optional<TimeScaleOuter> nonInst =
                    timeScales.stream().filter( scale -> !scale.isInstantaneous() ).findFirst();
            if ( nonInst.isPresent() )
            {
                return nonInst.get();
            }
        }

        // Compute the LCM of the durations
        Duration lcm = TimeScaleOuter.getLeastCommonDuration( timeScales.stream()
                                                                        .map( TimeScaleOuter::getPeriod )
                                                                        .collect( Collectors.toSet() ) );

        return TimeScaleOuter.of( lcm, timeScales.iterator().next().getFunction() );
    }

    /**
     * <p>Computes the Least Common Multiple of the inputs at a time resolution of milliseconds.
     * The LCM is the integer number of milliseconds that is a common multiple of all of the inputs.
     *
     * <p>TODO: consider moving this to a more general time-utility class, as it is independent of time scale.
     *
     * @param durations the time scales from which to derive the LCM
     * @return the LCM for the input
     * @throws NullPointerException if the inputs are null
     * @throws IllegalArgumentException if the input is empty or contains a {@link Duration#ZERO}
     */

    public static Duration getLeastCommonDuration( Set<Duration> durations )
    {
        Objects.requireNonNull( durations, "Cannot compute the Least Common Duration from null input." );

        if ( durations.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot compute the Least Common Duration from empty input." );
        }

        // Do not allow a duration of zero: see #61703
        // Note that the LCM is delivered below with ArithmeticUtils::lcm, whose documentation allows
        // for an LCM of zero, which is not valid in this context (and, arguably, any context)
        if ( durations.contains( Duration.ZERO ) )
        {
            throw new IllegalArgumentException( "When computing the Least Common Duration, found a "
                                                + "duration of zero, which is not allowed." );
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
        long lcs = it.next().toMillis();
        while ( it.hasNext() )
        {
            try
            {
                lcs = ArithmeticUtils.lcm( lcs, it.next().toMillis() );
            }
            // Decorate
            catch ( ArithmeticException e )
            {
                throw new RescalingException( "While attempting to compute the Least Common Duration from the input:",
                                              e );
            }
        }

        return Duration.ofMillis( lcs );
    }

    /**
     * If the time scale is not instantaneous, inspects the time scale for a period. If no period is defined, attempts 
     * to infer it from the month-day bookends that must be present. For the latter, assumes a leap year to maximize 
     * the period. If {@link TimeScaleOuter#isInstantaneous()} returns {@code true}, returns 
     * {@link #INSTANTANEOUS_DURATION}.
     *
     * @param timeScale the time scale
     * @return the period
     * @throws NullPointerException if the timeScale is null
     */
    public static Duration getOrInferPeriodFromTimeScale( TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( timeScale );

        Duration period;

        if ( !timeScale.isInstantaneous() )
        {
            if ( timeScale.hasPeriod() )
            {
                period = timeScale.getPeriod();
                LOGGER.debug( "Acquired the period from the time scale of {}, which was {}.",
                              timeScale,
                              period );
            }
            else
            {
                MonthDay earliest = timeScale.getStartMonthDay();
                MonthDay latest = timeScale.getEndMonthDay();

                if ( Objects.isNull( earliest ) || Objects.isNull( latest ) )
                {
                    throw new IllegalStateException( "Start and end month-days cannot be missing in this context." );
                }

                ZoneId zoneId = ZoneId.of( "UTC" );

                // Start of day, i.e., including 0Z
                Instant start = earliest.atYear( 2020 ) // Leap year
                                        .atStartOfDay( zoneId )
                                        .minusNanos( 1 )
                                        .toInstant();

                // End of day, i.e., immediately before 0Z of the next day
                Instant end = latest.atYear( 2020 ) // Leap year
                                    .atStartOfDay( zoneId )
                                    .plusDays( 1 )
                                    .minusNanos( 1 )
                                    .toInstant();

                period = Duration.between( start, end );
                LOGGER.debug( "Inferred the period from the time scale of {}, which was {}.",
                              timeScale,
                              period );
            }
        }
        else
        {
            period = TimeScaleOuter.INSTANTANEOUS_DURATION;
        }

        return period;
    }

    /**
     * Hidden constructor.
     *
     * @param timeScale the timescale
     * @throws IllegalArgumentException if the period is zero or negative
     * @throws NullPointerException if either input is null
     */

    private TimeScaleOuter( TimeScale timeScale )
    {
        Objects.requireNonNull( timeScale, "Specify a non-null time scale." );

        // Validate the time scale
        if ( timeScale.hasPeriod() )
        {
            if ( timeScale.getPeriod().getSeconds() == 0 && timeScale.getPeriod().getNanos() == 0 )
            {
                throw new IllegalArgumentException( "Cannot build a time scale with a period of zero." );
            }

            if ( timeScale.getPeriod().getSeconds() < 0 || timeScale.getPeriod().getNanos() < 0 )
            {
                throw new IllegalArgumentException( "Cannot build a time scale with a negative period." );
            }
        }
        // Both the start and end month-days must be present
        else if ( timeScale.getStartMonth() == 0 || timeScale.getEndMonth() == 0
                  || timeScale.getStartDay() == 0
                  || timeScale.getEndDay() == 0 )
        {
            throw new IllegalArgumentException( "A time scale must have an explicit period, else an implicit period "
                                                + "that is fully-defined, which requires both the start and end "
                                                + "month-days to be present. There was no period and one or more of "
                                                + "the start and end days or months were missing. The start month was "
                                                + timeScale.getStartMonth()
                                                + ", the start day was "
                                                + timeScale.getStartDay()
                                                + ", the end month was "
                                                + timeScale.getEndMonth()
                                                + ", and the end day was "
                                                + timeScale.getEndDay()
                                                + "." );
        }

        // The monthdays must be valid
        this.validateMonthDays( timeScale );

        this.timeScale = timeScale;
    }

    /**
     * Validates the earliest and latest monthdays when available.
     * @param timeScale the timeScale, not null
     * @throws IllegalArgumentException if either monthday is invalid
     */
    private void validateMonthDays( TimeScale timeScale )
    {
        MonthDay earliestMonthDay = null;
        MonthDay latestMonthDay = null;

        if ( timeScale.getStartMonth() != 0 || timeScale.getStartDay() != 0 )
        {
            try
            {
                earliestMonthDay = MonthDay.of( timeScale.getStartMonth(), timeScale.getStartDay() );
                LOGGER.debug( "Encountered an earliest monthday of {}.", earliestMonthDay );
            }
            catch ( DateTimeException e )
            {
                throw new IllegalArgumentException( "While attempting to set the start month and day associated with "
                                                    + "the time scale, encountered invalid input.",
                                                    e );
            }
        }

        if ( timeScale.getEndMonth() != 0 || timeScale.getEndDay() != 0 )
        {
            try
            {
                latestMonthDay = MonthDay.of( timeScale.getEndMonth(), timeScale.getEndDay() );
                LOGGER.debug( "Encountered a latest monthday of {}.", latestMonthDay );
            }
            catch ( DateTimeException e )
            {
                throw new IllegalArgumentException( "While attempting to set the end month and day associated with the "
                                                    + "time scale, encountered invalid input.",
                                                    e );
            }
        }
    }

}
