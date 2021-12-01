package wres.pipeline;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigPlus;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Reports on the completion status of the pools associated with an evaluation. A pool is the atomic unit of work in an 
 * evaluation.
 * 
 * @author James Brown
 */

class PoolReporter implements Consumer<PoolProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolReporter.class );

    /**
     * List of successful pools.
     */

    private final ConcurrentLinkedQueue<PoolRequest> successfulPools;

    /**
     * List of successful feature groups.
     */

    private final ConcurrentLinkedQueue<FeatureGroup> successfulFeatures;

    /**
     * List of successful time windows.
     */

    private final ConcurrentLinkedQueue<TimeWindowOuter> successfulTimeWindows;

    /**
     * Set of paths modified by this feature.
     */

    private final Set<Path> pathsWrittenTo;

    /**
     * The total number of pools to process.
     */

    private final int totalPools;

    /**
     * The project configuration.
     */

    private final ProjectConfigPlus projectConfigPlus;

    /**
     * The number of pools processed so far.
     */

    private AtomicInteger processed;
    
    /**
     * The time when the first pool was completed.
     */

    private Instant startTime;
    
    /**
     * The time when the last pool was completed.
     */

    private Instant endTime;    
    
    /**
     * Is <code>true</code> to print a detailed report in {@link #report()}, <code>false</code> to provide a summary.
     */

    private final boolean printDetailedReport;

    /**
     * Build a {@link PoolReporter}.
     * 
     * @param projectConfigPlus the project configuration
     * @param totalPools the total number of pools to process
     * @param printDetailedReport is true to print a detailed report on completion, false to summarize
     * @throws NullPointerException if the project configuration is null
     */

    PoolReporter( ProjectConfigPlus projectConfigPlus, int totalPools, boolean printDetailedReport )
    {
        Objects.requireNonNull( projectConfigPlus,
                                "Specify non-null project configuration when building the feature report." );

        this.projectConfigPlus = projectConfigPlus;
        this.totalPools = totalPools;
        this.printDetailedReport = printDetailedReport;
        this.successfulFeatures = new ConcurrentLinkedQueue<>();
        this.successfulPools = new ConcurrentLinkedQueue<>();
        this.successfulTimeWindows = new ConcurrentLinkedQueue<>();
        this.processed = new AtomicInteger( 0 );
        this.pathsWrittenTo = new ConcurrentSkipListSet<>();
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
        if( Objects.isNull( this.startTime ) )
        {
            this.startTime = Instant.now();
        }
        
        FeatureGroup featureGroup = result.getPoolRequest()
                                          .getMetadata()
                                          .getFeatureGroup();

        TimeWindowOuter timeWindow = result.getPoolRequest()
                                           .getMetadata()
                                           .getTimeWindow();

        if ( !result.hasStatistics() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "[{}/{}] Completed a pool in feature group '{}', but no statistics were produced. This "
                         + "probably occurred because the pool did not contain any pairs. The time window was: {}.",
                         this.processed.incrementAndGet(),
                         this.totalPools,
                         featureGroup.getName(),
                         timeWindow );
        }
        else
        {
            this.successfulPools.add( result.getPoolRequest() );
            this.successfulFeatures.add( result.getPoolRequest()
                                               .getMetadata()
                                               .getFeatureGroup() );
            this.successfulTimeWindows.add( timeWindow );

            String extra = "";
            if ( featureGroup.getFeatures().size() > 1 )
            {
                extra = ", which contained " + featureGroup.getFeatures().size() + " features";
            }

            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "[{}/{}] Completed statistics for a pool in feature group '{}'{}. The time window was: "
                             + "{}.",
                             this.processed.incrementAndGet(),
                             this.totalPools,
                             featureGroup.getName(),
                             extra,
                             timeWindow );
            }
        }
        
        // Register end time
        if( this.processed.get() == this.totalPools )
        {
            this.endTime = Instant.now();
        }
    }

    /**
     * Reports by logging information about the status of features.
     *  
     * @throws WresProcessingException if no features were completed successfully
     */

    void report()
    {
        // Finalize results
        Set<PoolRequest> successfulPoolsToReport = Set.copyOf( this.successfulPools );
        Set<FeatureGroup> successfulFeaturesToReport = Set.copyOf( this.successfulFeatures );
        Set<TimeWindowOuter> successfulTimeWindowsToReport = Set.copyOf( this.successfulTimeWindows );

        // Detailed report
        if ( LOGGER.isInfoEnabled() &&
             this.printDetailedReport
             &&
             !successfulFeaturesToReport.isEmpty() )
        {
            if( Objects.nonNull( this.endTime ) )
            {
                this.endTime = Instant.now();
            }
            
            LOGGER.info( "Statistics were created for {} pools, which included {} features groups and {} time windows. "
                         + "The feature groups were: {}. The time windows were: {}. The time elapsed between the "
                         + "completion of the first and last pools was: {}.",
                         successfulPoolsToReport.size(),
                         successfulFeaturesToReport.size(),
                         successfulTimeWindowsToReport.size(),
                         PoolReporter.getPoolItemDescription( successfulFeaturesToReport, FeatureGroup::getName ),
                         PoolReporter.getPoolItemDescription( successfulTimeWindowsToReport,
                                                              TimeWindowOuter::toString ),
                         Duration.between( this.startTime, this.endTime ) );
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
                LOGGER.info( "Finished creating statistics for all {} pools in project {}.",
                             this.totalPools,
                             this.projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} pools in project {} produced statistics and {} out of {} pools did not "
                             + "produce statistics.",
                             successfulPoolsToReport.size(),
                             this.totalPools,
                             this.projectConfigPlus,
                             this.totalPools - successfulPoolsToReport.size(),
                             this.totalPools );
            }
        }
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Get a comma separated description of a set of items with extra spacing
     *
     * @param items the items, not null
     * @return a summary description of the items
     */
    static <T> String getPoolItemDescription( Set<T> items, Function<T, String> stringifier )
    {
        StringJoiner outer = new StringJoiner( ", ", "[ ", " ]" );

        for ( T next : items )
        {
            String description = stringifier.apply( next );
            outer.add( description );
        }

        return outer.toString();
    }

}
