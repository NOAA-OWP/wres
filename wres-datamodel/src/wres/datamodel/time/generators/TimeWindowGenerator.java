package wres.datamodel.time.generators;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DateCondition;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeWindowOuter;

/**
 * <p>Helper class whose methods generate collections of {@link TimeWindowOuter} from project declaration.
 *
 * @author james.brown@hydrosolved.com
 */

public final class TimeWindowGenerator
{

    private static final String CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION = "Cannot determine time "
                                                                                             + "windows from null pair "
                                                                                             + "configuration.";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeWindowGenerator.class );

    /**
     * Consumes a {@link ProjectConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation. Returns at least one {@link TimeWindowOuter}.
     * 
     * @param pairConfig the pair declaration, cannot be null
     * @return a set of one or more time windows for evaluation
     * @throws NullPointerException if any required input is null
     */

    public static Set<TimeWindowOuter> getTimeWindowsFromPairConfig( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION );

        PoolingWindowConfig leadDurationPools = pairConfig.getLeadTimesPoolingWindow();
        PoolingWindowConfig issuedDatesPools = pairConfig.getIssuedDatesPoolingWindow();
        PoolingWindowConfig validDatesPools = pairConfig.getValidDatesPoolingWindow();

        // Has explicit pooling windows
        if ( Objects.nonNull( leadDurationPools ) || Objects.nonNull( issuedDatesPools )
             || Objects.nonNull( validDatesPools ) )
        {
            // All dimensions
            if ( Objects.nonNull( issuedDatesPools ) && Objects.nonNull( validDatesPools )
                 && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for issued dates and valid dates and lead durations." );

                return TimeWindowGenerator.getIssuedDatesAndValidDatesLeadDurationTimeWindows( pairConfig );
            }
            // Issued dates and valid dates
            else if ( Objects.nonNull( issuedDatesPools ) && Objects.nonNull( validDatesPools ) )
            {
                LOGGER.debug( "Building time windows for issued dates and valid dates." );

                return TimeWindowGenerator.getIssuedDatesAndValidDatesTimeWindows( pairConfig );
            }
            // Issued dates and lead durations
            else if ( Objects.nonNull( issuedDatesPools ) && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for issued dates and lead durations." );

                return TimeWindowGenerator.getIssuedDatesAndLeadDurationTimeWindows( pairConfig );
            }
            // Valid dates and lead durations
            else if ( Objects.nonNull( validDatesPools ) && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for valid dates and lead durations." );

                return TimeWindowGenerator.getValidDatesAndLeadDurationTimeWindows( pairConfig );
            }
            // Issued dates
            else if ( Objects.nonNull( issuedDatesPools ) )
            {
                LOGGER.debug( "Building time windows for issued dates." );

                return TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );
            }
            // Lead durations
            else if ( Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for lead durations." );

                return TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );
            }
            // Valid dates
            else
            {
                LOGGER.debug( "Building time windows for valid dates." );

                return TimeWindowGenerator.getValidDatesTimeWindows( pairConfig );
            }
        }
        // One big pool
        else
        {
            LOGGER.debug( "Building one big time window." );

            return Collections.singleton( TimeWindowGenerator.getOneBigTimeWindow( pairConfig ) );
        }
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()} and 
     * the {@link PairConfig#getLeadHours()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration time windows 
     * @throws NullPointerException if any required input is null
     * @throws ProjectConfigException if the time windows cannot be determined
     */

    private static Set<TimeWindowOuter> getLeadDurationTimeWindows( PairConfig pairConfig )
    {
        String messageStart = "Cannot determine lead duration time windows ";

        Objects.requireNonNull( pairConfig,
                                messageStart + "from null pair configuration." );

        IntBoundsType leadHours = pairConfig.getLeadHours();

        Objects.requireNonNull( pairConfig,
                                "Cannot determine lead duration time windows from null pair configuration." );
        Objects.requireNonNull( pairConfig.getLeadHours(),
                                "Cannot determine lead duration time windows without a leadHours." );
        Objects.requireNonNull( pairConfig.getLeadHours().getMinimum(),
                                "Cannot determine lead duration time windows without a minimum leadHours." );
        Objects.requireNonNull( pairConfig.getLeadHours().getMaximum(),
                                "Cannot determine lead duration time windows without a maximum leadHours." );
        Objects.requireNonNull( pairConfig.getLeadTimesPoolingWindow(),
                                "Cannot determine lead duration time windows without a leadTimesPoolingWindow." );

        PoolingWindowConfig leadTimesPoolingWindow = pairConfig.getLeadTimesPoolingWindow();

        // Obtain the base window
        TimeWindowOuter baseWindow = TimeWindowGenerator.getOneBigTimeWindow( pairConfig );

        // Create the elements necessary to increment the windows
        ChronoUnit periodUnits = ChronoUnit.valueOf( leadTimesPoolingWindow.getUnit()
                                                                           .toString()
                                                                           .toUpperCase() );
        // Period associated with the leadTimesPoolingWindow
        Duration periodOfLeadTimesPoolingWindow = Duration.of( leadTimesPoolingWindow.getPeriod(), periodUnits );

        // Exclusive lower bound: #56213-104
        Duration earliestLeadDurationExclusive = Duration.ofHours( leadHours.getMinimum() );

        // Inclusive upper bound
        Duration latestLeadDurationInclusive = Duration.ofHours( leadHours.getMaximum() );

        // Duration by which to increment. Defaults to the period associated
        // with the leadTimesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfLeadTimesPoolingWindow;
        if ( Objects.nonNull( leadTimesPoolingWindow.getFrequency() ) )
        {
            increment = Duration.of( leadTimesPoolingWindow.getFrequency(), periodUnits );
        }

        // Lower bound of the current window
        Duration earliestExclusive = earliestLeadDurationExclusive;

        // Upper bound of the current window
        Duration latestInclusive = earliestExclusive.plus( periodOfLeadTimesPoolingWindow );

        // Create the time windows
        Set<TimeWindowOuter> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound extends past the 
        // latestLeadDurationInclusive: #56213-104
        // Window increments are zero?
        if ( Duration.ZERO.equals( increment ) )
        {
            timeWindows.add( TimeWindowOuter.of( baseWindow.getEarliestReferenceTime(),
                                                 baseWindow.getLatestReferenceTime(),
                                                 baseWindow.getEarliestValidTime(),
                                                 baseWindow.getLatestValidTime(),
                                                 earliestExclusive,
                                                 latestInclusive ) );
        }
        // Create as many windows as required at the prescribed increment
        else
        {
            while ( latestInclusive.compareTo( latestLeadDurationInclusive ) <= 0 )
            {
                // Add the current time window
                timeWindows.add( TimeWindowOuter.of( baseWindow.getEarliestReferenceTime(),
                                                     baseWindow.getLatestReferenceTime(),
                                                     baseWindow.getEarliestValidTime(),
                                                     baseWindow.getLatestValidTime(),
                                                     earliestExclusive,
                                                     latestInclusive ) );

                // Increment from left-to-right: #56213-104
                earliestExclusive = earliestExclusive.plus( increment );
                latestInclusive = latestInclusive.plus( increment );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getIssuedDatesPoolingWindow()} and 
     * the {@link PairConfig#getIssuedDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of issued dates time windows 
     * @throws NullPointerException if the pairConfig is null or any required input within it is null
     */

    private static Set<TimeWindowOuter> getIssuedDatesTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine issued date time windows from null pair configuration." );
        Objects.requireNonNull( pairConfig.getIssuedDates(),
                                "Cannot determine issued date time windows when the issuedDates configuration is "
                                                             + "missing." );
        Objects.requireNonNull( pairConfig.getIssuedDates().getEarliest(),
                                "Cannot determine issued date time windows when the earliest issuedDates is missing." );
        Objects.requireNonNull( pairConfig.getIssuedDates().getLatest(),
                                "Cannot determine issued date time windows when the latest issuedDates is missing." );
        Objects.requireNonNull( pairConfig.getIssuedDatesPoolingWindow(),
                                "Cannot determine issued date time windows without an issuedDatesPoolingWindow." );

        // Base window from which to generate a sequence of windows
        TimeWindowOuter baseWindow = TimeWindowGenerator.getOneBigTimeWindow( pairConfig );

        return TimeWindowGenerator.getTimeWindowsForDateSequence( pairConfig.getIssuedDates(),
                                                                  pairConfig.getIssuedDatesPoolingWindow(),
                                                                  baseWindow,
                                                                  true );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getValidDatesPoolingWindow()} and 
     * the {@link PairConfig#getDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of valid dates time windows 
     * @throws NullPointerException if the pairConfig is null or any required input within it is null
     */

    private static Set<TimeWindowOuter> getValidDatesTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine valid date time windows from null pair configuration." );
        Objects.requireNonNull( pairConfig.getDates(),
                                "Cannot determine valid date time windows when the dates configuration is "
                                                       + "missing." );
        Objects.requireNonNull( pairConfig.getDates().getEarliest(),
                                "Cannot determine valid date time windows when the earliest dates is missing." );
        Objects.requireNonNull( pairConfig.getDates().getLatest(),
                                "Cannot determine valid date time windows when the latest dates is missing." );
        Objects.requireNonNull( pairConfig.getValidDatesPoolingWindow(),
                                "Cannot determine valid date time windows without an validDatesPoolingWindow." );

        // Base window from which to generate a sequence of windows
        TimeWindowOuter baseWindow = TimeWindowGenerator.getOneBigTimeWindow( pairConfig );

        return TimeWindowGenerator.getTimeWindowsForDateSequence( pairConfig.getDates(),
                                                                  pairConfig.getValidDatesPoolingWindow(),
                                                                  baseWindow,
                                                                  false );
    }

    /**
     * <p>Generates a set of time windows based on a sequence of datetimes. 
     * 
     * @param dates the date constraints
     * @param pools the sequence of datetimes to generate
     * @param baseWindow the basic time window from which each pool in the sequence begins
     * @param areIssuedTimes is true if the dates are issued dates, false for valid dates
     * @return the set of issued dates time windows 
     * @throws NullPointerException if any input is null
     * @throws ProjectConfigException if the time windows cannot be determined for any reason
     */

    private static Set<TimeWindowOuter> getTimeWindowsForDateSequence( DateCondition dates,
                                                                       PoolingWindowConfig pools,
                                                                       TimeWindowOuter baseWindow,
                                                                       boolean areIssuedTimes )
    {
        Objects.requireNonNull( dates );
        Objects.requireNonNull( pools );
        Objects.requireNonNull( baseWindow );

        // Create the elements necessary to increment the windows
        ChronoUnit timeUnits = ChronoUnit.valueOf( pools.getUnit()
                                                        .toString()
                                                        .toUpperCase() );
        // Period associated with the issuedDatesPoolingWindow
        // The default period is one time unit
        Duration periodOfPoolingWindow = Duration.of( pools.getPeriod(), timeUnits );

        // Exclusive lower bound: #56213-104
        Instant earliestInstantExclusive = Instant.parse( dates.getEarliest() );

        // Inclusive upper bound
        Instant latestInstantInclusive = Instant.parse( dates.getLatest() );

        // Duration by which to increment. Defaults to the period associated
        // with the issuedDatesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfPoolingWindow;
        if ( Objects.nonNull( pools.getFrequency() ) )
        {
            increment = Duration.of( pools.getFrequency(), timeUnits );
        }

        // Lower bound of the current window
        Instant earliestExclusive = earliestInstantExclusive;

        // Upper bound of the current window
        Instant latestInclusive = earliestExclusive.plus( periodOfPoolingWindow );

        // Create the time windows
        Set<TimeWindowOuter> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound 
        // extends past the latestInstantInclusive: #56213-104
        while ( latestInclusive.compareTo( latestInstantInclusive ) <= 0 )
        {
            TimeWindowOuter timeWindow = TimeWindowGenerator.getTimeWindowFromDates( earliestExclusive,
                                                                                     latestInclusive,
                                                                                     baseWindow,
                                                                                     areIssuedTimes );

            // Add the current time window
            timeWindows.add( timeWindow );

            // Increment left-to-right: #56213-104
            earliestExclusive = earliestExclusive.plus( increment );
            latestInclusive = latestInclusive.plus( increment );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * Returns a time window from the inputs.
     * 
     * @param earliestExclusive the earliest exclusive time
     * @param latestInclusive the latest inclusive time
     * @param baseWindow the base window with default times
     * @param areIssuedTimes is true if the earliestExclusive and latestInclusive are issued times, false if valid times
     * @return a time window
     */

    private static TimeWindowOuter getTimeWindowFromDates( Instant earliestExclusive,
                                                           Instant latestInclusive,
                                                           TimeWindowOuter baseWindow,
                                                           boolean areIssuedTimes )
    {
        // Issued dates
        if ( areIssuedTimes )
        {
            return TimeWindowOuter.of( earliestExclusive,
                                       latestInclusive,
                                       baseWindow.getEarliestValidTime(),
                                       baseWindow.getLatestValidTime(),
                                       baseWindow.getEarliestLeadDuration(),
                                       baseWindow.getLatestLeadDuration() );
        }
        // Valid dates
        else
        {
            return TimeWindowOuter.of( baseWindow.getEarliestReferenceTime(),
                                       baseWindow.getLatestReferenceTime(),
                                       earliestExclusive,
                                       latestInclusive,
                                       baseWindow.getEarliestLeadDuration(),
                                       baseWindow.getLatestLeadDuration() );
        }
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()}, 
     * the {@link PairConfig#getLeadHours()}, the {@link PairConfig#getIssuedDatesPoolingWindow()}
     * and the {@link PairConfig#getIssuedDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration and issued dates time windows 
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindowOuter> getIssuedDatesAndLeadDurationTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );

        Set<TimeWindowOuter> issuedDatesWindows = TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );

        // Create a new window for each combination of issued dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( leadDurationWindows.size() * issuedDatesWindows.size() );
        for ( TimeWindowOuter nextIssuedWindow : issuedDatesWindows )
        {
            for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
            {
                TimeWindowOuter composite = TimeWindowOuter.of( nextIssuedWindow.getEarliestReferenceTime(),
                                                                nextIssuedWindow.getLatestReferenceTime(),
                                                                nextIssuedWindow.getEarliestValidTime(),
                                                                nextIssuedWindow.getLatestValidTime(),
                                                                nextLeadWindow.getEarliestLeadDuration(),
                                                                nextLeadWindow.getLatestLeadDuration() );
                timeWindows.add( composite );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getIssuedDatesPoolingWindow()}, 
     * the {@link PairConfig#getIssuedDates()}, the {@link PairConfig#getValidDatesPoolingWindow()}
     * and the {@link PairConfig#getDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of issued dates and valid dates time windows 
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindowOuter> getIssuedDatesAndValidDatesTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION );

        Set<TimeWindowOuter> validDatesWindows = TimeWindowGenerator.getValidDatesTimeWindows( pairConfig );

        Set<TimeWindowOuter> issuedDatesWindows = TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );

        // Create a new window for each combination of issued dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( validDatesWindows.size() * issuedDatesWindows.size() );
        for ( TimeWindowOuter nextValidWindow : validDatesWindows )
        {
            for ( TimeWindowOuter nextIssuedWindow : issuedDatesWindows )
            {
                TimeWindowOuter composite = TimeWindowOuter.of( nextIssuedWindow.getEarliestReferenceTime(),
                                                                nextIssuedWindow.getLatestReferenceTime(),
                                                                nextValidWindow.getEarliestValidTime(),
                                                                nextValidWindow.getLatestValidTime(),
                                                                nextValidWindow.getEarliestLeadDuration(),
                                                                nextValidWindow.getLatestLeadDuration() );
                timeWindows.add( composite );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()}, 
     * the {@link PairConfig#getLeadHours()}, the {@link PairConfig#getValidDatesPoolingWindow()}
     * and the {@link PairConfig#getDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration and valid dates time windows 
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindowOuter> getValidDatesAndLeadDurationTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );

        Set<TimeWindowOuter> validDatesWindows = TimeWindowGenerator.getValidDatesTimeWindows( pairConfig );

        // Create a new window for each combination of valid dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( leadDurationWindows.size() * validDatesWindows.size() );
        for ( TimeWindowOuter nextValidWindow : validDatesWindows )
        {
            for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
            {
                TimeWindowOuter composite = TimeWindowOuter.of( nextValidWindow.getEarliestReferenceTime(),
                                                                nextValidWindow.getLatestReferenceTime(),
                                                                nextValidWindow.getEarliestValidTime(),
                                                                nextValidWindow.getLatestValidTime(),
                                                                nextLeadWindow.getEarliestLeadDuration(),
                                                                nextLeadWindow.getLatestLeadDuration() );
                timeWindows.add( composite );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindowOuter}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()}, 
     * the {@link PairConfig#getLeadHours()}, the {@link PairConfig#getIssuedDatesPoolingWindow()}
     * the {@link PairConfig#getIssuedDates()}, the {@link PairConfig#getValidDatesPoolingWindow()} and the 
     * {@link PairConfig#getDates()}. Returns at least one {@link TimeWindowOuter}. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration and issued dates and valid dates time windows 
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindowOuter> getIssuedDatesAndValidDatesLeadDurationTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, CANNOT_DETERMINE_TIME_WINDOWS_FROM_NULL_PAIR_CONFIGURATION );

        Set<TimeWindowOuter> leadDurationWindows = TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );
        Set<TimeWindowOuter> issuedDatesWindows = TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );
        Set<TimeWindowOuter> validDatesWindows = TimeWindowGenerator.getValidDatesTimeWindows( pairConfig );

        // Create a new window for each combination of issued dates and lead duration
        Set<TimeWindowOuter> timeWindows = new HashSet<>( leadDurationWindows.size() * issuedDatesWindows.size() );
        for ( TimeWindowOuter nextIssuedWindow : issuedDatesWindows )
        {
            for ( TimeWindowOuter nextValidWindow : validDatesWindows )
            {
                for ( TimeWindowOuter nextLeadWindow : leadDurationWindows )
                {
                    TimeWindowOuter composite = TimeWindowOuter.of( nextIssuedWindow.getEarliestReferenceTime(),
                                                                    nextIssuedWindow.getLatestReferenceTime(),
                                                                    nextValidWindow.getEarliestValidTime(),
                                                                    nextValidWindow.getLatestValidTime(),
                                                                    nextLeadWindow.getEarliestLeadDuration(),
                                                                    nextLeadWindow.getLatestLeadDuration() );
                    timeWindows.add( composite );
                }
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Builds a {@link TimeWindowOuter} whose {@link TimeWindowOuter#getEarliestReferenceTime()}
     * and {@link TimeWindowOuter#getLatestReferenceTime()} return the <code>earliest</earliest> 
     * and <code>latest</earliest> bookends of the {@link PairConfig#getIssuedDates()}, 
     * respectively, whose {@link TimeWindowOuter#getEarliestValidTime()}
     * and {@link TimeWindowOuter#getLatestValidTime()} return the <code>earliest</earliest> 
     * and <code>latest</earliest> bookends of the {@link PairConfig#getDates()}, 
     * respectively, and whose {@link TimeWindowOuter#getEarliestLeadDuration()}
     * and {@link TimeWindowOuter#getLatestLeadDuration()} return the <code>minimum</earliest> 
     * and <code>maximum</earliest> bookends of the {@link PairConfig#getLeadHours()}, 
     * respectively. 
     * 
     * <p>If any of these variables are missing from the input, defaults 
     * are used, which represent the computationally-feasible limiting values. For example, 
     * the smallest and largest possible instant is {@link Instant#MIN} and {@link Instant#MAX}, 
     * respectively. The smallest and largest possible {@link Duration} is 
     * {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @param pairConfig the pair configuration
     * @return a time window
     * @throws NullPointerException if the input is null
     */

    private static TimeWindowOuter getOneBigTimeWindow( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine the time window from null pair configuration." );

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        Duration smallestLeadDuration = TimeWindowOuter.DURATION_MIN;
        Duration largestLeadDuration = TimeWindowOuter.DURATION_MAX;

        // Issued datetimes
        if ( Objects.nonNull( pairConfig.getIssuedDates() ) )
        {
            if ( Objects.nonNull( pairConfig.getIssuedDates().getEarliest() ) )
            {
                earliestReferenceTime = Instant.parse( pairConfig.getIssuedDates().getEarliest() );
            }
            if ( Objects.nonNull( pairConfig.getIssuedDates().getLatest() ) )
            {
                latestReferenceTime = Instant.parse( pairConfig.getIssuedDates().getLatest() );
            }
        }

        // Valid datetimes
        if ( Objects.nonNull( pairConfig.getDates() ) )
        {
            if ( Objects.nonNull( pairConfig.getDates().getEarliest() ) )
            {
                earliestValidTime = Instant.parse( pairConfig.getDates().getEarliest() );
            }
            if ( Objects.nonNull( pairConfig.getDates().getLatest() ) )
            {
                latestValidTime = Instant.parse( pairConfig.getDates().getLatest() );
            }
        }

        // Lead durations
        if ( Objects.nonNull( pairConfig.getLeadHours() ) )
        {
            if ( Objects.nonNull( pairConfig.getLeadHours().getMinimum() ) )
            {
                smallestLeadDuration = Duration.ofHours( pairConfig.getLeadHours().getMinimum() );
            }
            if ( Objects.nonNull( pairConfig.getLeadHours().getMaximum() ) )
            {
                largestLeadDuration = Duration.ofHours( pairConfig.getLeadHours().getMaximum() );
            }
        }

        return TimeWindowOuter.of( earliestReferenceTime,
                                   latestReferenceTime,
                                   earliestValidTime,
                                   latestValidTime,
                                   smallestLeadDuration,
                                   largestLeadDuration );
    }

    /**
     * Do not construct.
     */

    private TimeWindowGenerator()
    {
        // Do not construct
    }
}
