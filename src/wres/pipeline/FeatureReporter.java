package wres.pipeline;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigPlus;
import wres.datamodel.space.FeatureGroup;
import wres.events.Evaluation;

/**
 * <p>A {@link Consumer} that records information about the completion state of a {@link FeatureGroup}. The 
 * {@link FeatureReporter} is mutable and is updated asynchronously as {@link FeatureProcessingResult} become available.
 * Some information is reported during execution, and additional information is reported on request (e.g. at the 
 * end of execution) by calling {@link #report()}.
 * 
 * <p>See #71889. This reporting mechanism is flawed insofar as it inspects paths written per feature. Thus, it only 
 * has sight of success or failure when formats are requested that are written per feature and not for those written
 * across features, such as pairs or netCDF. A different reporting mechanism that does not rely on inspecting paths 
 * written, rather numbers produced, is necessary to report on true success or failure per feature.
 * 
 * <p>Furthermore, this class reports on the success (or otherwise) of the publication of statistics and not their
 * consumption. Consumers are out-of-band to publishers and this mechanism is used by publishers only. TODO: move this
 * status reporting to the {@link Evaluation}, specifically to the package private EvaluationStatusTracker and report
 * on "group" consumption more generally, noting that a group may contain more than one feature. See #88698.
 * 
 * @author James Brown
 */

class FeatureReporter implements Consumer<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureReporter.class );

    /**
     * List of successful features.
     */

    private final ConcurrentLinkedQueue<FeatureGroup> successfulFeatures;

    /**
     * Set of paths modified by this feature.
     */

    private final Set<Path> pathsWrittenTo;

    /**
     * The total number of features to process.
     */

    private final int totalFeatures;

    /**
     * The project configuration.
     */

    private final ProjectConfigPlus projectConfigPlus;

    /**
     * The number of features processed so far.
     */

    private AtomicInteger processed;

    /**
     * Is <code>true</code> to print details of individual features in {@link #report()}, <code>false</code> to 
     * provide a summary.
     */

    private final boolean printDetailedReport;

    /**
     * Build a {@link FeatureReporter}.
     * 
     * @param projectConfigPlus the project configuration
     * @param totalFeatures the total number of features to process
     * @param printDetailedReport is true to name features individually in {@link #report()}, false to summarize
     * @throws NullPointerException if the project configuration is null
     */

    FeatureReporter( ProjectConfigPlus projectConfigPlus, int totalFeatures, boolean printDetailedReport )
    {
        Objects.requireNonNull( projectConfigPlus,
                                "Specify non-null project configuration when building the feature report." );

        this.projectConfigPlus = projectConfigPlus;
        this.totalFeatures = totalFeatures;
        this.printDetailedReport = printDetailedReport;
        this.successfulFeatures = new ConcurrentLinkedQueue<>();
        this.processed = new AtomicInteger( 1 );
        this.pathsWrittenTo = new ConcurrentSkipListSet<>();
    }

    /**
     * Document a new {@link FeatureProcessingResult}.
     * 
     * @param result the result
     */

    @Override
    public void accept( FeatureProcessingResult result )
    {
        Objects.requireNonNull( result, "cannot accept a null feature processing result." );

        // Increment the feature count
        int currentFeature = this.processed.getAndIncrement();

        if ( !result.hasStatistics() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "[{}/{}] Completed feature group '{}', but no statistics were created. "
                         + "This probably occurred because no pools contained valid pairs.",
                         currentFeature,
                         this.totalFeatures,
                         result.getFeatureGroup().getName() );
        }
        else
        {
            this.successfulFeatures.add( result.getFeatureGroup() );

            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "[{}/{}] Completed statistics for feature group '{}'",
                             currentFeature,
                             this.totalFeatures,
                             result.getFeatureGroup().getName() );
            }
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
        Set<FeatureGroup> successfulFeaturesToReport = Set.copyOf( this.successfulFeatures );

        // Detailed report
        if ( LOGGER.isInfoEnabled() &&
             this.printDetailedReport
             &&
             !successfulFeaturesToReport.isEmpty() )
        {
            LOGGER.info( "Statistics were created for these feature groups: {}",
                         FeatureReporter.getFeaturesDescription( successfulFeaturesToReport ) );
        }

        // Exception after detailed report: in practice, this should be handled earlier
        // but this is another opportunity to signal that zero successful features is 
        // exceptional behavior
        if ( successfulFeaturesToReport.isEmpty() )
        {
            throw new WresProcessingException( "Statistics could not be produced for any feature tuples. This probably "
                                               + "occurred because none of the feature tuples contained any pools with "
                                               + "valid pairs. Check that the declaration contains some pools whose "
                                               + "boundaries (e.g., earliest and latest issued times, earliest and "
                                               + "latest valid times and earliest and latest lead durations) are "
                                               + "sufficiently broad to capture some pairs at the desired time scale.",
                                               null );
        }

        // Summary report
        if ( LOGGER.isInfoEnabled() )
        {
            if ( this.totalFeatures == successfulFeaturesToReport.size() )
            {
                LOGGER.info( "Finished creating statistics for all feature groups in project {}.",
                             this.projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} feature groups in project {} produced statistics and "
                             + "{} out of {} feature groups did not produce statistics.",
                             successfulFeaturesToReport.size(),
                             this.totalFeatures,
                             this.projectConfigPlus,
                             this.totalFeatures - successfulFeaturesToReport.size(),
                             this.totalFeatures );
            }
        }
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Get a comma separated description of a set of features groups.
     *
     * @param featureGroups the groups of features, not null
     * @return a summary description of all features
     */
    private static String getFeaturesDescription( Set<FeatureGroup> featureGroups )
    {
        StringJoiner outer = new StringJoiner( ", ", "[ ", " ]" );

        for ( FeatureGroup nextGroup : featureGroups )
        {
            outer.add( nextGroup.getName() );
        }

        return outer.toString();
    }

}
