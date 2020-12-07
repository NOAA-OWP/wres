package wres.control;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigPlus;
import wres.config.generated.Feature;
import wres.datamodel.FeatureTuple;
import wres.io.data.caching.Features;

/**
 * <p>A {@link Consumer} that records information about the completion state of a {@link Feature}. The 
 * {@link FeatureReporter} is mutable and is updated asynchronously as {@link FeatureProcessingResult} become available.
 * Some information is reported during execution, and additional information is reported on request (e.g. at the 
 * end of execution) by calling {@link #report()}.
 * 
 * <p>See #71889. This reporting mechanism is flawed insofar as it inspects paths written per feature. Thus, it only 
 * has sight of success or failure when formats are requested that are written per feature and not for those written
 * across features, such as pairs or netCDF. A different reporting mechanism that does not rely on inspecting paths 
 * written, rather numbers produced, is necessary to report on true success or failure per feature.
 * 
 * @author james.brown@hydrosolved.com
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

    private final ConcurrentLinkedQueue<FeatureTuple> successfulFeatures;

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
            LOGGER.warn( "[{}/{}] Completed feature tuple '{}', but no statistics were produced. "
                         + "This probably occurred because no pools contained valid pairs.",
                         currentFeature,
                         this.totalFeatures,
                         result.getFeature().toStringShort() );
        }
        else
        {
            this.successfulFeatures.add( result.getFeature() );

            this.pathsWrittenTo.addAll( result.getPathsWrittenTo() );

            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "[{}/{}] Completed feature tuple '{}'",
                             currentFeature,
                             this.totalFeatures,
                             result.getFeature().toStringShort() );
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
        List<FeatureTuple> successfulFeaturesToReport = List.copyOf( successfulFeatures );

        // Detailed report
        if ( LOGGER.isInfoEnabled() &&
             this.printDetailedReport
             &&
             !successfulFeaturesToReport.isEmpty() )
        {
            LOGGER.info( "The following feature tuples succeeded: {}",
                         Features.getFeaturesDescription( successfulFeaturesToReport ) );
        }

        // Exception after detailed report: in practice, this should be handled earlier
        // but this is another opportunity to signal that zero successful features is 
        // exceptional behavior
        if ( successfulFeaturesToReport.isEmpty() )
        {
            throw new WresProcessingException( "No features were successfully evaluated.", null );
        }

        // Summary report
        if ( LOGGER.isInfoEnabled() )
        {
            if ( this.totalFeatures == successfulFeaturesToReport.size() )
            {
                LOGGER.info( "All features in project {} succeeded.",
                             projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} features in project {} succeeded and "
                             + "{} out of {} features did not succeed.",
                             successfulFeaturesToReport.size(),
                             totalFeatures,
                             projectConfigPlus,
                             totalFeatures - successfulFeaturesToReport.size(),
                             totalFeatures );
            }
        }
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }
}
