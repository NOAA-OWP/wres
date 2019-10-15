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
import wres.datamodel.time.TimeWindow;

/**
 * <p>Helper class whose methods generate collections of {@link TimeWindow} from project declaration.
 *
 * @author james.brown@hydrosolved.com
 */

public final class TimeWindowGenerator
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeWindowGenerator.class );

    /**
     * Consumes a {@link ProjectConfig} and returns a {@link Set} of {@link TimeWindow}
     * for evaluation. Returns at least one {@link TimeWindow}.
     * 
     * @param pairConfig the pair declaration, cannot be null
     * @return a set of one or more time windows for evaluation
     * @throws NullPointerException if the projectConfig is null
     * @throws ProjectConfigException if the time windows cannot be determined
     */

    public static Set<TimeWindow> getTimeWindowsFromPairConfig( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine time windows from null pair configuration." );

        PoolingWindowConfig leadDurationPools = pairConfig.getLeadTimesPoolingWindow();
        PoolingWindowConfig issuedDatesPools = pairConfig.getIssuedDatesPoolingWindow();

        // Has explicit pooling windows
        if ( Objects.nonNull( leadDurationPools ) || Objects.nonNull( issuedDatesPools ) )
        {
            // Lead duration pools only
            if ( Objects.isNull( issuedDatesPools ) )
            {
                LOGGER.trace( "Building time windows for lead durations." );

                return TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );
            }
            // Issued date pools only
            else if ( Objects.isNull( leadDurationPools ) )
            {
                LOGGER.trace( "Building time windows for issued dates." );

                return TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );
            }
            // Both lead duration and issued date pools
            else
            {
                LOGGER.trace( "Building time windows for issued dates and lead durations." );

                return TimeWindowGenerator.getIssuedDatesAndLeadDurationTimeWindows( pairConfig );
            }
        }
        // One big pool
        else
        {
            LOGGER.trace( "Building one big time window." );

            return Collections.singleton( TimeWindowGenerator.getOneBigTimeWindow( pairConfig ) );
        }
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindow}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()} and 
     * the {@link PairConfig#getLeadHours()}. Returns at least one {@link TimeWindow}. 
     * 
     * <p>Throws an exception if either of the <code>leadHours</code> or 
     * <code>leadTimesPoolingWindow</code> is undefined. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration time windows 
     * @throws NullPointerException if the pairConfig is null
     * @throws ProjectConfigException if the time windows cannot be determined
     */

    private static Set<TimeWindow> getLeadDurationTimeWindows( PairConfig pairConfig )
    {
        String messageStart = "Cannot determine lead duration time windows ";

        Objects.requireNonNull( pairConfig,
                                messageStart + "from null pair configuration." );

        IntBoundsType leadHours = pairConfig.getLeadHours();

        if ( Objects.isNull( leadHours ) )
        {
            throw new ProjectConfigException( leadHours, messageStart + "without a leadHours." );
        }

        if ( Objects.isNull( leadHours.getMinimum() ) )
        {
            throw new ProjectConfigException( leadHours, messageStart + "without a minimum leadHours." );
        }

        if ( Objects.isNull( leadHours.getMaximum() ) )
        {
            throw new ProjectConfigException( leadHours, messageStart + "without a maximum leadHours." );
        }

        PoolingWindowConfig leadTimesPoolingWindow = pairConfig.getLeadTimesPoolingWindow();

        // Obtain the base window
        TimeWindow baseWindow = TimeWindowGenerator.getOneBigTimeWindow( pairConfig );

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
        Set<TimeWindow> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound extends past the 
        // latestLeadDurationInclusive: #56213-104
        while ( latestInclusive.compareTo( latestLeadDurationInclusive ) <= 0 )
        {
            // Add the current time window
            timeWindows.add( TimeWindow.of( baseWindow.getEarliestReferenceTime(),
                                            baseWindow.getLatestReferenceTime(),
                                            baseWindow.getEarliestValidTime(),
                                            baseWindow.getLatestValidTime(),
                                            earliestExclusive,
                                            latestInclusive ) );

            // Increment from left-to-right: #56213-104
            earliestExclusive = earliestExclusive.plus( increment );
            latestInclusive = latestInclusive.plus( increment );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindow}
     * for evaluation using the {@link PairConfig#getIssuedDatesPoolingWindow()} and 
     * the {@link PairConfig#getIssuedDates()}. Returns at least one {@link TimeWindow}. 
     * 
     * <p>Throws an exception if either of the <code>issuedDates</code> or 
     * <code>issuedDatesPoolingWindow</code> is undefined. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of issued dates time windows 
     * @throws NullPointerException if the pairConfig is null
     * @throws ProjectConfigException if the time windows cannot be determined
     */

    private static Set<TimeWindow> getIssuedDatesTimeWindows( PairConfig pairConfig )
    {
        String messageStart = "Cannot determine issued dates time windows ";

        Objects.requireNonNull( pairConfig,
                                messageStart + "from null pair configuration." );

        DateCondition issuedDates = pairConfig.getIssuedDates();

        if ( Objects.isNull( issuedDates ) )
        {
            throw new ProjectConfigException( issuedDates, messageStart + "without an issuedDates." );
        }

        if ( Objects.isNull( issuedDates.getEarliest() ) )
        {
            throw new ProjectConfigException( issuedDates, messageStart + "without an earliest issuedDates." );
        }

        if ( Objects.isNull( issuedDates.getLatest() ) )
        {
            throw new ProjectConfigException( issuedDates, messageStart + "without a latest issuedDates." );
        }

        PoolingWindowConfig issuedDatesPoolingWindow = pairConfig.getIssuedDatesPoolingWindow();

        // Obtain the base window
        TimeWindow baseWindow = TimeWindowGenerator.getOneBigTimeWindow( pairConfig );

        // Create the elements necessary to increment the windows
        ChronoUnit timeUnits = ChronoUnit.valueOf( issuedDatesPoolingWindow.getUnit()
                                                                           .toString()
                                                                           .toUpperCase() );
        // Period associated with the issuedDatesPoolingWindow
        // The default period is one time unit
        Duration periodOfIssuedDatesPoolingWindow = Duration.of( issuedDatesPoolingWindow.getPeriod(), timeUnits );

        // Exclusive lower bound: #56213-104
        Instant earliestInstantExclusive = Instant.parse( issuedDates.getEarliest() );

        // Inclusive upper bound
        Instant latestInstantInclusive = Instant.parse( issuedDates.getLatest() );

        // Duration by which to increment. Defaults to the period associated
        // with the issuedDatesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfIssuedDatesPoolingWindow;
        if ( Objects.nonNull( issuedDatesPoolingWindow.getFrequency() ) )
        {
            increment = Duration.of( issuedDatesPoolingWindow.getFrequency(), timeUnits );
        }

        // Lower bound of the current window
        Instant earliestExclusive = earliestInstantExclusive;

        // Upper bound of the current window
        Instant latestInclusive = earliestExclusive.plus( periodOfIssuedDatesPoolingWindow );

        // Create the time windows
        Set<TimeWindow> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound 
        // extends past the latestInstantInclusive: #56213-104
        while ( latestInclusive.compareTo( latestInstantInclusive ) <= 0 )
        {
            // Add the current time window
            timeWindows.add( TimeWindow.of( earliestExclusive,
                                            latestInclusive,
                                            baseWindow.getEarliestValidTime(),
                                            baseWindow.getLatestValidTime(),
                                            baseWindow.getEarliestLeadDuration(),
                                            baseWindow.getLatestLeadDuration() ) );

            // Increment left-to-right: #56213-104
            earliestExclusive = earliestExclusive.plus( increment );
            latestInclusive = latestInclusive.plus( increment );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link PairConfig} and returns a {@link Set} of {@link TimeWindow}
     * for evaluation using the {@link PairConfig#getLeadTimesPoolingWindow()}, 
     * the {@link PairConfig#getLeadHours()}, the {@link PairConfig#getIssuedDatesPoolingWindow()}
     * and the {@link PairConfig#getIssuedDates()}. Returns at least one {@link TimeWindow}. 
     * 
     * <p>Throws an exception if any of the <code>leadHours</code>, 
     * <code>leadTimesPoolingWindow</code>, <code>issuedDates</code> or 
     * <code>issuedDatesPoolingWindow</code> is undefined. 
     * 
     * @param pairConfig the pairs configuration
     * @return the set of lead duration and issued dates time windows 
     * @throws NullPointerException if the pairConfig is null
     * @throws ProjectConfigException if the time windows cannot be determined
     */

    private static Set<TimeWindow> getIssuedDatesAndLeadDurationTimeWindows( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine time windows from null pair configuration." );

        Set<TimeWindow> leadDurationWindows = TimeWindowGenerator.getLeadDurationTimeWindows( pairConfig );

        Set<TimeWindow> issuedDatesWindows = TimeWindowGenerator.getIssuedDatesTimeWindows( pairConfig );

        // Create a new window for each combination of issued dates and lead duration
        Set<TimeWindow> timeWindows = new HashSet<>( leadDurationWindows.size() * issuedDatesWindows.size() );
        for ( TimeWindow nextIssuedWindow : issuedDatesWindows )
        {
            for ( TimeWindow nextLeadWindow : leadDurationWindows )
            {
                timeWindows.add( TimeWindow.of( nextIssuedWindow.getEarliestReferenceTime(),
                                                nextIssuedWindow.getLatestReferenceTime(),
                                                nextIssuedWindow.getEarliestValidTime(),
                                                nextIssuedWindow.getLatestValidTime(),
                                                nextLeadWindow.getEarliestLeadDuration(),
                                                nextLeadWindow.getLatestLeadDuration() ) );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Builds a {@link TimeWindow} whose {@link TimeWindow#getEarliestReferenceTime()}
     * and {@link TimeWindow#getLatestReferenceTime()} return the <code>earliest</earliest> 
     * and <code>latest</earliest> bookends of the {@link PairConfig#getIssuedDates()}, 
     * respectively, whose {@link TimeWindow#getEarliestValidTime()}
     * and {@link TimeWindow#getLatestValidTime()} return the <code>earliest</earliest> 
     * and <code>latest</earliest> bookends of the {@link PairConfig#getDates()}, 
     * respectively, and whose {@link TimeWindow#getEarliestLeadDuration()}
     * and {@link TimeWindow#getLatestLeadDuration()} return the <code>minimum</earliest> 
     * and <code>maximum</earliest> bookends of the {@link PairConfig#getLeadHours()}, 
     * respectively. 
     * 
     * <p>If any of these variables are missing from the input, defaults 
     * are used, which represent the computationally-feasible limiting values. For example, 
     * the smallest and largest possible instant is {@link Instant#MIN} and {@link Instant#MAX}, 
     * respectively. The smallest and largest possible {@link Duration} is 
     * {@link TimeWindow#DURATION_MIN} and {@link TimeWindow#DURATION_MAX}, respectively.
     * 
     * @param pairConfig the pair configuration
     * @return a time window
     * @throws NullPointerException if the input is null
     */

    private static TimeWindow getOneBigTimeWindow( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig, "Cannot determine the time window from null pair configuration." );

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        Duration smallestLeadDuration = TimeWindow.DURATION_MIN;
        Duration largestLeadDuration = TimeWindow.DURATION_MAX;

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

        return TimeWindow.of( earliestReferenceTime,
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
