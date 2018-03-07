package wres.control;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.processing.MetricProcessorException;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.InputGenerator;
import wres.io.retrieval.IterationFailedException;
import wres.io.utilities.NoDataException;
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
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws IOException when an issue occurs during ingest
     * @throws ProjectConfigException if the project configuration is invalid
     * @throws WresProcessingException when an issue occurs during processing
     */

    static void processProjectConfig( final ProjectConfigPlus projectConfigPlus,
                                      final ExecutorService pairExecutor,
                                      final ExecutorService thresholdExecutor,
                                      final ExecutorService metricExecutor )
            throws IOException, ProjectConfigException
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        ProgressMonitor.setShowStepDescription( true );
        ProgressMonitor.resetMonitor();

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        ProjectDetails projectDetails = Operations.ingest( projectConfig );

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<Feature> decomposedFeatures;

        try
        {
            decomposedFeatures = Operations.decomposeFeatures( projectDetails );
        }
        catch ( SQLException e )
        {
            IOException ioe = new IOException( "Failed to retrieve the set of features.", e );
            ProcessorHelper.addException( ioe );
            throw ioe;
        }

        List<Feature> successfulFeatures = new ArrayList<>();
        List<Feature> missingDataFeatures = new ArrayList<>();

        // Read external thresholds from the configuration, per feature
        // Compare on locationId only. TODO: improve the representation of features
        // TODO: MUST move this threshold reading to wres-metrics as this is project internals related to metrics, 
        // not API stuff. However, there are two barriers: 1) it involves file IO, which
        // should not happen in metrics; and 2) it requires a consistent, system-wide, definition of features
        // because external thresholds are read for multiple features. Both would be solved if we had an internal
        // representation of a full project configuration that was passed around the system after ingest.
        // The current approach is deeply unsatisfying.
        final Map<FeaturePlus, List<Set<Threshold>>> thresholds = new TreeMap<>( FeaturePlus::compareByLocationId );
        thresholds.putAll( ConfigHelper.readThresholdsFromProjectConfig( projectConfig ) );

        // Reduce our triad of executors to one object
        ExecutorServices executors = new ExecutorServices( pairExecutor,
                                                           thresholdExecutor,
                                                           metricExecutor );

        int currentFeature = 0;

        for ( Feature feature : decomposedFeatures )
        {
            ProgressMonitor.resetMonitor();

            if ( LOGGER.isInfoEnabled() )
            {
                currentFeature++;
                LOGGER.info( "[{}/{}] Processing feature '{}'",
                             currentFeature,
                             decomposedFeatures.size(),
                             ConfigHelper.getFeatureDescription( feature ) );
            }

            FeatureProcessingResult result =
                    processFeature( feature,
                                    thresholds.get( FeaturePlus.of( feature ) ),
                                    projectConfigPlus,
                                    projectDetails,
                                    executors );

            if ( result.hadData() )
            {
                successfulFeatures.add( result.getFeature() );
            }
            else
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( "Not enough data found for feature {}:",
                                 ConfigHelper.getFeatureDescription( result.getFeature() ),
                                 result.getCause() );
                }
                missingDataFeatures.add( result.getFeature() );
            }
        }

        printFeaturesReport( projectConfigPlus,
                             decomposedFeatures,
                             successfulFeatures,
                             missingDataFeatures );
    }


    /**
     * Print a report to the log about which features were successful and not.
     * Also, throw an exception if zero features were successful.
     * @param projectConfigPlus the project config just processed
     * @param decomposedFeatures the features decomposed from the config
     * @param successfulFeatures the features that succeeded
     * @param missingDataFeatures the features that were missing data
     * @throws WresProcessingException when zero features were successful
     */

    private static void printFeaturesReport( final ProjectConfigPlus projectConfigPlus,
                                             final Set<Feature> decomposedFeatures,
                                             final List<Feature> successfulFeatures,
                                             final List<Feature> missingDataFeatures )
    {
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "The following features succeeded: {}",
                         ConfigHelper.getFeaturesDescription( successfulFeatures ) );

            if ( !missingDataFeatures.isEmpty() )
            {
                LOGGER.info( "The following features were missing data: {}",
                             ConfigHelper.getFeaturesDescription( missingDataFeatures ) );
            }
        }

        if ( successfulFeatures.isEmpty() )
        {
            throw new WresProcessingException( "No features were successful.",
                                               null );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            if ( decomposedFeatures.size() == successfulFeatures.size() )
            {
                LOGGER.info( "All features in project {} were successfully "
                             + "evaluated.",
                             projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} features in project {} were successfully "
                             + "evaluated, {} out of {} features were missing data.",
                             successfulFeatures.size(),
                             decomposedFeatures.size(),
                             projectConfigPlus,
                             missingDataFeatures.size(),
                             decomposedFeatures.size() );
            }
        }
    }


    /**
     * Processes a {@link ProjectConfigPlus} for a specific {@link Feature} using a prescribed {@link ExecutorService}
     * for each of the pairs, thresholds and metrics.
     * 
     * @param feature the feature to process
     * @param thresholds an optional set of (canonical) thresholds for which
     *                   results are required, may be null
     * @param projectConfigPlus the project configuration
     * @param projectDetails the project details to use
     * @param executors the executors for pairs, thresholds, and metrics
     * @throws WresProcessingException when an error occurs during processing
     * @return a feature result
     */

    private static FeatureProcessingResult processFeature( final Feature feature,
                                                           final List<Set<Threshold>> thresholds,
                                                           final ProjectConfigPlus projectConfigPlus,
                                                           final ProjectDetails projectDetails,
                                                           final ExecutorServices executors )
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        final String featureDescription = ConfigHelper.getFeatureDescription( feature );
        final String errorMessage = "While processing feature " + featureDescription;

        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        final MetricProcessorForProject processor;
        try
        {
            processor = MetricFactory.getInstance( DATA_FACTORY )
                                     .getMetricProcessorForProject( projectConfig,
                                                                    thresholds,
                                                                    executors.getThresholdExecutor(),
                                                                    executors.getMetricExecutor() );
        }
        catch ( final MetricProcessorException e )
        {
            throw new WresProcessingException( errorMessage, e );
        }

        // Build an InputGenerator for the next feature
        InputGenerator metricInputs = Operations.getInputs( projectDetails,
                                                            feature );

        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        final List<CompletableFuture<?>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // During the pipeline, only write types that are not end-of-pipeline types unless they refer to
        // a format that can be written incrementally
        BiPredicate<MetricOutputGroup, DestinationType> onlyWriteTheseTypes =
                ( type, format ) -> !processor.getCachedMetricOutputTypes().contains( type )
                                    || ConfigHelper.getIncrementalFormats( projectConfig ).contains( format );

        try
        {
            // Iterate
            for ( final Future<MetricInput<?>> nextInput : metricInputs )
            {
                // Complete all tasks asynchronously:
                // 1. Get some pairs from the database
                // 2. Compute the metrics
                // 3. Process any intermediate verification results
                // 4. Monitor progress
                final CompletableFuture<Void> c =
                        CompletableFuture.supplyAsync( new PairsByTimeWindowProcessor( nextInput, processor ),
                                                       executors.getPairExecutor() )
                                         .thenAcceptAsync( new ProductProcessor( projectConfigPlus,
                                                                                 onlyWriteTheseTypes ),
                                                           executors.getPairExecutor() )
                                         .thenAccept( aVoid -> ProgressMonitor.completeStep() );

                // Add the future to the list
                listOfFutures.add( c );
            }
        }
        catch ( IterationFailedException re )
        {
            if ( ProcessorHelper.wasInsufficientDataOrNoDataInThisStack( re ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    re );
            }
            else
            {
                ProcessorHelper.addException( re );
            }
        }

        // Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
        try
        {
            doAllOrException( listOfFutures ).join();
        }
        catch ( CompletionException e )
        {
            // If there was simply not enough data for this feature, OK
            if ( ProcessorHelper.wasInsufficientDataOrNoDataInThisStack( e ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    e );
            }

            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( errorMessage, e );
        }

        // Generate cached output if available
        if ( processor.hasCachedMetricOutput() )
        {
            try
            {
                // End of pipeline processor
                // Only process cached types that were not written incrementally
                BiPredicate<MetricOutputGroup, DestinationType> nowWriteTheseTypes =
                        ( type, format ) -> processor.getCachedMetricOutputTypes().contains( type )
                                            && !ConfigHelper.getIncrementalFormats( projectConfig ).contains( format );
                ProductProcessor endOfPipeline =
                        new ProductProcessor( projectConfigPlus,
                                              nowWriteTheseTypes );

                // Generate output
                endOfPipeline.accept( processor.getCachedMetricOutput() );

            }
            catch ( MetricOutputAccessException e )
            {
                throw new WresProcessingException( errorMessage, e );
            }
        }

        return new FeatureProcessingResult( feature, true, null );
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

    private static CompletableFuture<?> doAllOrException( final List<CompletableFuture<?>> futures )
    {
        //Complete when all futures are completed
        final CompletableFuture<?> allDone =
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) );
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<?> oneExceptional = new CompletableFuture<>();
        //Link the two
        for ( final CompletableFuture<?> completableFuture : futures )
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
     * Returns true if the given config has one or more of given output type.
     * @param config the config to search
     * @param type the type of output to look for
     * @return true if the output type is present, false otherwise
     */

    static boolean configNeedsThisTypeOfOutput( ProjectConfig config,
                                                DestinationType type )
    {
        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return false;
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType().equals( type ) )
            {
                return true;
            }
        }

        return false;
    }


    /**
     * Look at a chain of exceptions, returns true if ANY is a NoDataException
     * or if ANY is an InsufficientDataException
     *
     * Intended as a stop-gap measure until we figure out how to avoid creating
     * NoDataExceptions at lower levels of the software. Once that is resolved,
     * this method can be removed.
     *
     * @param e the exception (and its chained causes) to look at
     * @return true when either NoDataException or InsufficientDataException is
     * found, false otherwise
     */

    private static boolean wasInsufficientDataOrNoDataInThisStack( Exception e )
    {
        Throwable cause = e;
        while ( cause != null )
        {
            if ( cause instanceof NoDataException
                 || cause instanceof InsufficientDataException )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * List of exceptions encountered during processing.
     */
    private static final List<Exception> exceptionList = new ArrayList<>();

    /**
     * A lock to use when mutating the list of exceptions.
     */

    private static final Object EXCEPTION_LOCK = new Object();

    /**
     * Add an exception to the list of exceptions.
     * 
     * @param exception the exception to add
     */

    private static void addException( Exception exception )
    {
        synchronized ( EXCEPTION_LOCK )
        {
            ProcessorHelper.exceptionList.add( exception );
        }
    }

    /**
     * Return a list of processing exceptions encountered.
     * 
     * @return a list of processing exceptions
     */

    public static List<Exception> getEncounteredExceptions()
    {
        synchronized ( EXCEPTION_LOCK )
        {
            return Collections.unmodifiableList( ProcessorHelper.exceptionList );
        }
    }


    /**
     * A value object that a) reduces count of args for some methods and
     * b) provides names for those objects. Can be removed if we can reduce the
     * count of dependencies in some of our methods, or if we prefer to see all
     * dependencies clearly laid out in the method signature.
     */

    private static class ExecutorServices
    {

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
         * Build. 
         * 
         * @param pairExecutor the pair executor
         * @param thresholdExecutor the threshold executor
         * @param metricExecutor the metric executor
         */
        ExecutorServices( ExecutorService pairExecutor,
                          ExecutorService thresholdExecutor,
                          ExecutorService metricExecutor )
        {
            this.pairExecutor = pairExecutor;
            this.thresholdExecutor = thresholdExecutor;
            this.metricExecutor = metricExecutor;
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
    }
}
