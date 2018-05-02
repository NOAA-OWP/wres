package wres.control;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigPlus;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;

/**
 * <p>A {@link Consumer} that records information about the completion state of a {@link Feature}. The 
 * {@link FeatureReport} is mutable and is updated asynchronously as {@link FeatureProcessingResult} become available.
 * Some information is reported during execution, and additional information is reported on request (e.g. at the 
 * end of execution) by calling {@link #report()}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

class FeatureReport implements Consumer<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureReport.class );

    /**
     * List of successful features.
     */

    private final ConcurrentLinkedQueue<Feature> successfulFeatures;

    /**
     * List of features that failed due to missing data.
     */

    private final ConcurrentLinkedQueue<Feature> missingDataFeatures;

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
     * Build a {@link FeatureReport}.
     * 
     * @param projectConfigPlus the project configuration
     * @param totalFeatures the total number of features to process
     * @param printDetailedReport is true to name features individually in {@link #report()}, false to summarize
     * @throws NullPointerException if the project configuration is null
     */

    FeatureReport( ProjectConfigPlus projectConfigPlus, int totalFeatures, boolean printDetailedReport )
    {
        Objects.requireNonNull( projectConfigPlus,
                                "Specify non-null project configuration when building the feature report." );

        this.projectConfigPlus = projectConfigPlus;
        this.totalFeatures = totalFeatures;
        this.printDetailedReport = printDetailedReport;
        this.successfulFeatures = new ConcurrentLinkedQueue<>();
        this.missingDataFeatures = new ConcurrentLinkedQueue<>();
        this.processed = new AtomicInteger( 1 );
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
        
        // Data available
        if ( result.hadData() )
        {
            this.successfulFeatures.add( result.getFeature() );
            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "[{}/{}] Completed feature '{}'",
                             currentFeature,
                             this.totalFeatures,
                             ConfigHelper.getFeatureDescription( result.getFeature() ) );
            }
        }
        // No data available
        else
        {
            this.missingDataFeatures.add( result.getFeature() );
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "[{}/{}] Not enough data found for feature '{}':",
                             currentFeature,
                             this.totalFeatures,
                             ConfigHelper.getFeatureDescription( result.getFeature() ),
                             result.getCause() );
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
        List<Feature> successfulFeaturesToReport =
                Collections.unmodifiableList( new ArrayList<>( successfulFeatures ) );
        List<Feature> missingDataFeaturesToReport =
                Collections.unmodifiableList( new ArrayList<>( missingDataFeatures ) );

        // Detailed report
        if ( LOGGER.isInfoEnabled() && this.printDetailedReport )
        {
            LOGGER.info( "The following features succeeded: {}",
                         ConfigHelper.getFeaturesDescription( successfulFeaturesToReport ) );

            if ( !missingDataFeatures.isEmpty() )
            {
                LOGGER.info( "The following features were missing data: {}",
                             ConfigHelper.getFeaturesDescription( missingDataFeaturesToReport ) );
            }
        }

        // Exception after detailed report
        if ( successfulFeaturesToReport.isEmpty() )
        {
            throw new WresProcessingException( "No features were successful.",
                                               null );
        }

        // Summary report
        if ( LOGGER.isInfoEnabled() )
        {
            if ( totalFeatures == successfulFeaturesToReport.size() )
            {
                LOGGER.info( "All features in project {} were successfully "
                             + "evaluated.",
                             projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} features in project {} were successfully "
                             + "evaluated, {} out of {} features were missing data.",
                             successfulFeaturesToReport.size(),
                             totalFeatures,
                             projectConfigPlus,
                             missingDataFeaturesToReport.size(),
                             totalFeatures );
            }
        }
    }

}
