package wres.control;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.io.writing.SharedWriters;
import wres.io.writing.SharedWriters.SharedWritersBuilder;
import wres.util.ProgressMonitor;

/**
 * Class with functions to help in generating metrics and processing metric products.
 *
 * TODO: abstract away the functions used for graphical processing to a separate helper, GraphicalProductsHelper.
 *
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
class ProcessorHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProcessorHelper.class );

    /**
     * Default data factory.
     */

    private static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }


    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     * 
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @throws IOException when an issue occurs during ingest
     * @throws ProjectConfigException if the project configuration is invalid
     * @throws WresProcessingException when an issue occurs during processing
     */

    static void processProjectConfig( final ProjectConfigPlus projectConfigPlus,
                                      final ExecutorServices executors )
            throws IOException, ProjectConfigException
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        ProgressMonitor.setShowStepDescription( true );
        ProgressMonitor.resetMonitor();

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        ProjectDetails projectDetails = Operations.ingest( projectConfig );
        Operations.prepareForExecution( projectDetails );

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<FeaturePlus> decomposedFeatures;

        try
        {
            decomposedFeatures = Operations.decomposeFeatures( projectDetails );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Failed to retrieve the set of features.", e );
        }

        // Read external thresholds from the configuration, per feature
        // Compare on locationId only. TODO: consider how better to transmit these thresholds
        // to wres-metrics, given that they are resolved by project configuration that is
        // passed separately to wres-metrics. Options include moving MetricProcessor* to 
        // wres-control, since they make processing decisions, or passing ResolvedProject onwards
        final Map<FeaturePlus, ThresholdsByMetric> thresholds =
                new TreeMap<>( FeaturePlus::compareByLocationId );
        thresholds.putAll( ConfigHelper.readExternalThresholdsFromProjectConfig( projectConfig ) );

        // The project code - ideally project hash
        String projectIdentifier = String.valueOf( projectDetails.getKey() );

        ResolvedProject resolvedProject = ResolvedProject.of( projectConfigPlus,
                                                              decomposedFeatures,
                                                              projectIdentifier,
                                                              thresholds );

        // Build any writers of incremental formats that are shared across features
        final Set<MetricConstants> metricsForSharedWriting = resolvedProject.getDoubleScoreMetrics();
        final int thresholdCountForSharedWriting = resolvedProject.getThresholdCount( MetricOutputGroup.DOUBLE_SCORE );
        SharedWritersBuilder sharedWritersBuilder = new SharedWritersBuilder();
        if ( ConfigHelper.getIncrementalFormats( projectConfig )
                         .contains( DestinationType.NETCDF ) )
        {
            sharedWritersBuilder.setNetcdfDoublescoreWriter( ConfigHelper.getNetcdfWriter( projectIdentifier,
                                                                                           projectConfig,
                                                                                           resolvedProject.getFeatureCount(),
                                                                                           thresholdCountForSharedWriting,
                                                                                           metricsForSharedWriting ) );
        }
         
        // Iterate the features, closing any shared writers on completion
        try ( SharedWriters sharedWriters = sharedWritersBuilder.build() )
        {

            // Tasks for features 
            List<CompletableFuture<Void>> featureTasks = new ArrayList<>();

            // Report on the completion state of all features
            // Report detailed state by default (final arg = true)
            // TODO: demote to summary report (final arg = false) for >> feature count
            FeatureReport featureReport = new FeatureReport( projectConfigPlus, decomposedFeatures.size(), true );

            // Deactivate progress monitoring within features, as features are processed asynchronously - the internal
            // completion state of features has no value when reported in this way
            ProgressMonitor.deactivate();

            // Create one task per feature
            for ( FeaturePlus feature : decomposedFeatures )
            {
                CompletableFuture<Void> nextFeatureTask = CompletableFuture.supplyAsync( new FeatureProcessor( feature,
                                                                                                               resolvedProject,
                                                                                                               projectDetails,
                                                                                                               executors,
                                                                                                               sharedWriters,
                                                                                                               DATA_FACTORY ),
                                                                                         executors.getFeatureExecutor() )
                                                                           .thenAccept( featureReport );

                // Add to list of tasks
                featureTasks.add( nextFeatureTask );
            }

            // Run the tasks, and join on all tasks. The main thread will wait until all are completed successfully
            // or one completes exceptionally for reasons other than lack of data
            try
            {
                ProcessorHelper.doAllOrException( featureTasks ).join();

                // Report on features
                featureReport.report();
            }
            catch ( CompletionException e )
            {
                throw new WresProcessingException( "Project failed to complete with the following error: ", e );
            }

        }
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally 
     */

    static <T> CompletableFuture<Object> doAllOrException( final List<CompletableFuture<T>> futures )
    {
        //Complete when all futures are completed
        final CompletableFuture<Void> allDone =
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) );
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<T> oneExceptional = new CompletableFuture<>();
        //Link the two
        for ( final CompletableFuture<T> completableFuture : futures )
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally( exception -> {
                oneExceptional.completeExceptionally( exception );
                return null;
            } );
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf( allDone, oneExceptional );
    }

    /**
     * Look at a chain of exceptions, returns true if ANY is a NoDataException.
     *
     * Intended as a stop-gap measure until we figure out how to avoid creating
     * NoDataExceptions at lower levels of the software. Once that is resolved,
     * this method can be removed.
     *
     * @param e the exception (and its chained causes) to look at
     * @return true when either NoDataException or InsufficientDataException is
     * found, false otherwise
     */

    static boolean wasNoDataExceptionInThisStack( Exception e )
    {
        Throwable cause = e;
        while ( cause != null )
        {
            if ( cause instanceof NoDataException )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }


    /**
     * A value object that a) reduces count of args for some methods and
     * b) provides names for those objects. Can be removed if we can reduce the
     * count of dependencies in some of our methods, or if we prefer to see all
     * dependencies clearly laid out in the method signature.
     */

    static class ExecutorServices
    {

        /**
         * The feature executor.
         */
        private final ExecutorService featureExecutor;

        /**
         * The pair executor.
         */
        private final ExecutorService pairExecutor;

        /**
         * The threshold executor.
         */
        private final ExecutorService thresholdExecutor;

        /**
         * The metric executor.
         */
        private final ExecutorService metricExecutor;

        /**
         * The product executor.
         */
        private final ExecutorService productExecutor;

        /**
         * Build. 
         * 
         * @param featureExecutor the feature executor
         * @param pairExecutor the pair executor
         * @param thresholdExecutor the threshold executor
         * @param metricExecutor the metric executor
         */
        ExecutorServices( ExecutorService featureExecutor,
                          ExecutorService pairExecutor,
                          ExecutorService thresholdExecutor,
                          ExecutorService metricExecutor,
                          ExecutorService productExecutor )
        {
            this.featureExecutor = featureExecutor;
            this.pairExecutor = pairExecutor;
            this.thresholdExecutor = thresholdExecutor;
            this.metricExecutor = metricExecutor;
            this.productExecutor = productExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for features.
         * @return the metric executor
         */

        ExecutorService getFeatureExecutor()
        {
            return this.featureExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for pairs.
         * @return the pair executor
         */

        ExecutorService getPairExecutor()
        {
            return this.pairExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for thresholds.
         * @return the threshold executor
         */

        ExecutorService getThresholdExecutor()
        {
            return this.thresholdExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for metrics.
         * @return the metric executor
         */

        ExecutorService getMetricExecutor()
        {
            return this.metricExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for products.
         * @return the product executor
         */

        ExecutorService getProductExecutor()
        {
            return this.productExecutor;
        }
    }
}
