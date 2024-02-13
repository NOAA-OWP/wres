package wres.pipeline.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeWindowOuter;
import wres.pipeline.WresProcessingException;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;

/**
 * Reports on the completion status of the pools associated with an evaluation. A pool is the atomic unit of work in an 
 * evaluation.
 *
 * @author James Brown
 */

public class PoolReporter implements Consumer<PoolProcessingResult>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolReporter.class );

    /** Time window stringifier. */
    private static final Function<TimeWindowOuter, String> TIME_WINDOW_STRINGIFIER =
            PoolReporter.getTimeWindowStringifier();

    /** List of successful pools.*/
    private final ConcurrentLinkedQueue<PoolRequest> successfulPools;

    /** The total number of pools to process. */
    private final int totalPools;

    /** The project configuration.*/
    private final EvaluationDeclaration declaration;

    /** Feature groups for summary statistics. The raw statistics for these feature groups are not published. */
    private final Set<FeatureGroup> featureGroupsForSummaryStatistics;

    /** The number of pools processed so far. */
    private final AtomicInteger processed;

    /** Is {@code true} to print a detailed report in {@link #report()}, {@code false} to provide a summary. */
    private final boolean printDetailedReport;

    /** The evaluation identifier. */
    private final String evaluationId;

    /** The time when the first pool was completed. */
    private Instant startTime;

    /** The time when the last pool was completed. */
    private Instant endTime;

    /**
     * Build a {@link PoolReporter}.
     *
     * @param declaration the project configuration
     * @param featureGroupsForSummaryStatistics the feature groups for summary statistics only
     * @param totalPools the total number of pools to process
     * @param printDetailedReport is true to print a detailed report on completion, false to summarize
     * @param evaluationId the evaluation identifier
     * @throws NullPointerException if the project configuration is null
     */

    public PoolReporter( EvaluationDeclaration declaration,
                         Set<FeatureGroup> featureGroupsForSummaryStatistics,
                         int totalPools,
                         boolean printDetailedReport,
                         String evaluationId )
    {
        Objects.requireNonNull( declaration,
                                "Specify non-null project configuration when building the feature report." );

        this.declaration = declaration;
        this.totalPools = totalPools;
        this.printDetailedReport = printDetailedReport;
        this.successfulPools = new ConcurrentLinkedQueue<>();
        this.processed = new AtomicInteger( 0 );
        this.featureGroupsForSummaryStatistics = featureGroupsForSummaryStatistics;
        this.evaluationId = evaluationId;
    }

    /**
     * Document a new {@link PoolProcessingResult}.
     *
     * @param result the result
     */

    @Override
    public void accept( PoolProcessingResult result )
    {
        Objects.requireNonNull( result, "cannot accept a null pool processing result." );

        // Register start time
        if ( Objects.isNull( this.startTime ) )
        {
            this.startTime = Instant.now();
        }

        PoolMetadata metadata = result.getPoolRequest()
                                      .getMetadata();
        FeatureGroup featureGroup =
                metadata.getFeatureGroup();

        TimeWindowOuter timeWindow = metadata.getTimeWindow();

        switch ( result.getStatus() )
        {
            case STATISTICS_AVAILABLE_NOT_PUBLISHED_ERROR_STATE ->
                    this.reportStatisticsAvailableNotPublished( featureGroup, timeWindow );
            case STATISTICS_PUBLICATION_SKIPPED ->
            {
                this.successfulPools.add( result.getPoolRequest() );
                this.reportStatisticsSkipped( featureGroup, timeWindow );
            }
            case STATISTICS_NOT_AVAILABLE ->
                    this.reportStatisticsNotAvailable( featureGroup, timeWindow, result.getEvaluationStatusEvents() );
            case STATISTICS_PUBLISHED ->
            {
                this.successfulPools.add( result.getPoolRequest() );
                this.reportStatisticsPublished( featureGroup, timeWindow );
            }
        }

        // Register end time
        if ( this.processed.get() == this.totalPools )
        {
            this.endTime = Instant.now();
        }
    }

    /**
     * Reports by logging information about the status of features.
     *
     * @throws WresProcessingException if no features were completed successfully
     */

    public void report()
    {
        // Finalize results
        Set<PoolRequest> successfulPoolsToReport = Set.copyOf( this.successfulPools );
        Set<FeatureGroup> successfulFeaturesToReport = new TreeSet<>();
        Set<TimeWindowOuter> successfulTimeWindowsToReport = new TreeSet<>();
        for ( PoolRequest poolRequest : successfulPoolsToReport )
        {
            PoolMetadata mainMetadata = poolRequest.getMetadata();
            FeatureGroup nextGroup = mainMetadata.getFeatureGroup();
            TimeWindowOuter nextWindow = mainMetadata.getTimeWindow();
            successfulFeaturesToReport.add( nextGroup );
            successfulTimeWindowsToReport.add( nextWindow );
        }

        // Detailed report
        if ( LOGGER.isInfoEnabled() &&
             this.printDetailedReport
             && !successfulFeaturesToReport.isEmpty() )
        {
            if ( Objects.isNull( this.endTime ) )
            {
                this.endTime = Instant.now();
            }

            if ( this.featureGroupsForSummaryStatistics.isEmpty() )
            {
                LOGGER.info( "Statistics were created for {} pools, which included {} features groups and {} time "
                             + "windows. The time elapsed between the completion of the first and last pools was: {}."
                             + " The feature groups were: {}. The time windows were: {}.",
                             successfulPoolsToReport.size(),
                             successfulFeaturesToReport.size(),
                             successfulTimeWindowsToReport.size(),
                             Duration.between( this.startTime, this.endTime ),
                             PoolReporter.getPoolItemDescription( successfulFeaturesToReport, FeatureGroup::getName ),
                             PoolReporter.getPoolItemDescription( successfulTimeWindowsToReport,
                                                                  TIME_WINDOW_STRINGIFIER ) );
            }
            else
            {
                LOGGER.info( "Statistics were created for {} pools, which included {} features groups and {} time "
                             + "windows. In addition, {} feature groups were evaluated for summary statistics only. "
                             + "The time elapsed between the completion of the first and last pools was: {}. The "
                             + "feature groups were: {}. The time windows were: {}. The feature groups for summary "
                             + "statistics were: {}",
                             successfulPoolsToReport.size(),
                             successfulFeaturesToReport.size(),
                             successfulTimeWindowsToReport.size(),
                             this.featureGroupsForSummaryStatistics.size(),
                             Duration.between( this.startTime, this.endTime ),
                             PoolReporter.getPoolItemDescription( successfulFeaturesToReport,
                                                                  FeatureGroup::getName ),
                             PoolReporter.getPoolItemDescription( successfulTimeWindowsToReport,
                                                                  TIME_WINDOW_STRINGIFIER ),
                             PoolReporter.getPoolItemDescription( this.featureGroupsForSummaryStatistics,
                                                                  FeatureGroup::getName ) );
            }
        }

        // Exception after detailed report: in practice, this should be handled earlier
        // but this is another opportunity to signal that zero successful features is 
        // exceptional behavior
        if ( successfulPoolsToReport.isEmpty() )
        {
            throw new WresProcessingException( "Statistics could not be produced for any pools. This probably occurred "
                                               + "because none of the pools contained valid pairs. Check that the "
                                               + "declaration contains some pools whose boundaries (e.g., earliest and "
                                               + "latest issued times, earliest and latest valid times and earliest "
                                               + "and latest lead durations) are sufficiently broad to capture some "
                                               + "pairs at the desired time scale for at least one feature and "
                                               + "threshold.",
                                               null );
        }

        // Summary report
        if ( LOGGER.isInfoEnabled() )
        {
            if ( this.totalPools == successfulPoolsToReport.size() )
            {
                LOGGER.info( "Finished creating statistics for all {} pools in {}.",
                             this.totalPools,
                             this.getEvaluationName() );
            }
            else
            {
                LOGGER.info( "{} out of {} pools in {} produced statistics and {} out of {} pools did not "
                             + "produce statistics.",
                             successfulPoolsToReport.size(),
                             this.totalPools,
                             this.getEvaluationName(),
                             this.totalPools - successfulPoolsToReport.size(),
                             this.totalPools );
            }
        }
    }

    /**
     * Get a comma separated description of a set of items with extra spacing
     *
     * @param <T> the type of item
     * @param items the items, not null
     * @param stringifier the function that translates the item to a string
     * @return a summary description of the items
     */
    public static <T> String getPoolItemDescription( Set<T> items, Function<T, String> stringifier )
    {
        StringJoiner outer = new StringJoiner( ", ", "[ ", " ]" );

        for ( T next : items )
        {
            String description = stringifier.apply( next );
            outer.add( description );
        }

        return outer.toString();
    }

    /**
     * Report statistics not produced.
     * @param featureGroup the feature group
     * @param timeWindow the time window
     */

    private void reportStatisticsNotAvailable( FeatureGroup featureGroup,
                                               TimeWindowOuter timeWindow,
                                               List<EvaluationStatusMessage> statusEvents )
    {
        if ( LOGGER.isWarnEnabled() )
        {
            // Any status events marked WARN or DEBUG aka "detailed warning" that might help a user?
            List<EvaluationStatusMessage> warnings =
                    statusEvents.stream()
                                .filter( next -> next.getStatusLevel() == StatusLevel.WARN
                                                 || next.getStatusLevel() == StatusLevel.DEBUG )
                                .toList();

            // Any status events that might help a user?
            if ( !warnings.isEmpty() )
            {
                StringBuilder message = new StringBuilder();
                message.append( "[{}/{}] Completed a pool in feature group '{}', but no statistics were produced. This "
                                + "probably occurred because the pool did not contain any pairs. The time window was: "
                                + "{}. Encountered {} evaluation status warnings when creating the pool." );

                // Map the number of warnings by evaluation stage
                Map<EvaluationStage, Long> counts = warnings.stream()
                                                            .collect( Collectors.groupingBy( EvaluationStatusMessage::getEvaluationStage,
                                                                                             Collectors.counting() ) );

                message.append( " Of these warnings, " );

                int count = 0;
                int totalCount = counts.size();
                for ( Map.Entry<EvaluationStage, Long> next : counts.entrySet() )
                {
                    message.append( next.getValue() )
                           .append( " originated from '" )
                           .append( next.getKey() )
                           .append( "'" );
                    count++;

                    if ( count == totalCount )
                    {
                        message.append( "." );
                    }
                    else if ( totalCount > 1 && count == totalCount - 1 )
                    {
                        message.append( ", and " );
                    }
                    else if ( totalCount > 1 )
                    {
                        message.append( ", " );
                    }
                }

                message.append( " An example warning follows for each evaluation stage that produced one or more "
                                + "warnings. To review the individual warnings, turn on debug logging. Example "
                                + "warnings: " );

                // One example warning per evaluation stage
                Map<EvaluationStage, EvaluationStatusMessage> oneWarningPerStage = warnings.stream()
                                                                                           .collect( Collectors.toMap(
                                                                                                   EvaluationStatusMessage::getEvaluationStage,
                                                                                                   Function.identity(),
                                                                                                   ( s,
                                                                                                     a ) -> s ) );
                message.append( oneWarningPerStage )
                       .append( "." );

                LOGGER.warn( message.toString(),
                             this.processed.incrementAndGet(),
                             this.totalPools,
                             featureGroup.getName(),
                             TIME_WINDOW_STRINGIFIER.apply( timeWindow ),
                             warnings.size() );
            }
            else
            {
                LOGGER.warn( "[{}/{}] Completed a pool in feature group '{}', but no statistics were produced. This "
                             + "probably occurred because the pool did not contain any pairs. The time window was: {}.",
                             this.processed.incrementAndGet(),
                             this.totalPools,
                             featureGroup.getName(),
                             TIME_WINDOW_STRINGIFIER.apply( timeWindow ) );
            }
        }
    }

    /**
     * @return the evaluation name
     */
    private String getEvaluationName()
    {
        String start;
        if ( Objects.isNull( this.declaration.label() ) )
        {
            start = "unnamed evaluation";
        }
        else
        {
            start = "evaluation '" + declaration.label() + "'";
        }

        return start + " with identifier " + this.evaluationId;
    }

    /**
     * Report statistics available and published.
     * @param featureGroup the feature group
     * @param timeWindow the time window
     */

    private void reportStatisticsPublished( FeatureGroup featureGroup, TimeWindowOuter timeWindow )
    {
        if ( LOGGER.isInfoEnabled() )
        {
            String extra = "";
            if ( featureGroup.getFeatures().size() > 1 )
            {
                extra = ", which contained " + featureGroup.getFeatures().size() + " features";
            }

            LOGGER.info( "[{}/{}] Completed statistics for a pool in feature group '{}'{}. The time window was: "
                         + "{}.",
                         this.processed.incrementAndGet(),
                         this.totalPools,
                         featureGroup.getName(),
                         extra,
                         TIME_WINDOW_STRINGIFIER.apply( timeWindow ) );
        }
    }

    /**
     * Report statistics produced, not published.
     * @param featureGroup the feature group
     * @param timeWindow the time window
     */

    private void reportStatisticsAvailableNotPublished( FeatureGroup featureGroup, TimeWindowOuter timeWindow )
    {
        if ( LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "[{}/{}] Completed a pool in feature group '{}', which produced statistics, but these "
                         + "statistics were not published because the evaluation was in an errored state and closing. "
                         + "The time window was: {}.",
                         this.processed.incrementAndGet(),
                         this.totalPools,
                         featureGroup.getName(),
                         TIME_WINDOW_STRINGIFIER.apply( timeWindow ) );
        }
    }

    /**
     * Report statistics produced, not published.
     * @param featureGroup the feature group
     * @param timeWindow the time window
     */

    private void reportStatisticsSkipped( FeatureGroup featureGroup, TimeWindowOuter timeWindow )
    {
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "[{}/{}] Completed a pool in feature group '{}', which produced statistics, but these "
                         + "statistics were only used to generate summary statistics. The time window was: {}.",
                         this.processed.incrementAndGet(),
                         this.totalPools,
                         featureGroup.getName(),
                         TIME_WINDOW_STRINGIFIER.apply( timeWindow ) );
        }
    }

    /**
     * @return a function that consumes a {@link TimeWindowOuter} and produces a string representation of it.
     */

    private static Function<TimeWindowOuter, String> getTimeWindowStringifier()
    {
        return timeWindow -> {

            if ( timeWindow.hasUnboundedReferenceTimes() && timeWindow.hasUnboundedValidTimes()
                 && timeWindow.bothLeadDurationsAreUnbounded() )
            {
                return "( Unbounded in all dimensions )";
            }

            StringJoiner joiner = new StringJoiner( ", ", "( ", " )" );

            joiner.add( "Earliest reference time: " + timeWindow.getEarliestReferenceTime() )
                  .add( "Latest reference time: " + timeWindow.getLatestReferenceTime() )
                  .add( "Earliest valid time: " + timeWindow.getEarliestValidTime() )
                  .add( "Latest valid time: " + timeWindow.getLatestValidTime() )
                  .add( "Earliest lead duration: " + timeWindow.getEarliestLeadDuration() )
                  .add( "Latest lead duration: " + timeWindow.getLatestLeadDuration() );

            return joiner.toString();

        };
    }

}
