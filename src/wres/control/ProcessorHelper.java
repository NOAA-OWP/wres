package wres.control;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.*;
import wres.datamodel.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.ThresholdReader;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.SharedStatisticsWriters;
import wres.io.writing.SharedStatisticsWriters.SharedWritersBuilder;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

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
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @throws IOException when an issue occurs during ingest
     * @throws ProjectConfigException if the project configuration is invalid
     * @throws WresProcessingException when an issue occurs during processing
     * @return the paths to which outputs were written
     */

    static Set<Path> processProjectConfig( SystemSettings systemSettings,
                                           Database database,
                                           Executor executor,
                                           final ProjectConfigPlus projectConfigPlus,
                                           final ExecutorServices executors,
                                           DatabaseLockManager lockManager )
            throws IOException
    {
        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        ProgressMonitor.setShowStepDescription( false );
        ProgressMonitor.resetMonitor();

        // Create output directory prior to ingest, fails early when it fails.
        Path outputDirectory = ProcessorHelper.createTempOutputDirectory();

        // Get a unit mapper for the declared measurement units
        PairConfig pairConfig = projectConfig.getPair();
        String desiredMeasurementUnit = pairConfig.getUnit();
        UnitMapper unitMapper = UnitMapper.of( database, desiredMeasurementUnit );

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        Project project = Operations.ingest( systemSettings,
                                             database,
                                             executor,
                                             projectConfig,
                                             lockManager );
        Operations.prepareForExecution( project );

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<FeatureTuple> decomposedFeatures;

        try
        {
            decomposedFeatures = project.getFeatures();
        }
        catch ( SQLException e )
        {
            throw new IOException( "Failed to retrieve the set of features.", e );
        }

        if ( decomposedFeatures.isEmpty() )
        {
            throw new NoDataException( "There were no data correlated by "
                                       + " geographic features specified "
                                       + "available for evaluation." );
        }

        // Read external thresholds from the configuration, per feature
        // Compare on left dataset's feature name only.
        // TODO: consider how better to transmit these thresholds
        // to wres-metrics, given that they are resolved by project configuration that is
        // passed separately to wres-metrics. Options include moving MetricProcessor* to
        // wres-control, since they make processing decisions, or passing ResolvedProject onwards
        ThresholdReader thresholdReader = new ThresholdReader( systemSettings,
                                                               projectConfig,
                                                               unitMapper,
                                                               decomposedFeatures,
                                                               LeftOrRightOrBaseline.LEFT );
        Map<FeatureTuple, ThresholdsByMetric> thresholds = thresholdReader.read();

        // Features having thresholds as reported by the threshold reader.
        Set<FeatureTuple> havingThresholds = thresholdReader.getEvaluatableFeatures();

        // If the left dataset name exists in thresholds, keep it in the set.
        decomposedFeatures = Collections.unmodifiableSet( havingThresholds );

        if ( decomposedFeatures.isEmpty() )
        {
            throw new NoDataException( "There were data correlated by "
                                       + "geographic features specified " 
                                       + "available for evaluation but "
                                       + "there were no thresholds available "
                                       + "for any of those features." );
        }

        // The project code - ideally project hash
        String projectIdentifier = String.valueOf( project.getInputCode() );

        ResolvedProject resolvedProject = ResolvedProject.of(
                projectConfigPlus,
                decomposedFeatures,
                projectIdentifier,
                thresholds,
                outputDirectory
        );

        // Obtain the duration units for outputs: #55441
        String durationUnitsString = projectConfig.getOutputs().getDurationFormat().value().toUpperCase();
        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

        // Build any writers of incremental formats that are shared across features
        SharedWritersBuilder sharedWritersBuilder = new SharedWritersBuilder();
        if ( ConfigHelper.getIncrementalFormats( projectConfig ).contains( DestinationType.NETCDF ) )
        {
            // Use the gridded netcdf writer
            sharedWritersBuilder.setNetcdfOutputWriter(
                    NetcdfOutputWriter.of(
                            systemSettings,
                            executor,
                            projectConfig,
                            durationUnits,
                            outputDirectory,
                            thresholds
                    )
            );
        }

        SharedSampleDataWriters sharedSampleWriters = null;
        SharedSampleDataWriters sharedBaselineSampleWriters = null;

        // If there are multiple destinations for pairs, ignore these. The system chooses the destination.
        // Writing the same pairs, more than once, to that single destination does not make sense.
        // See #55948-12 and #55948-13. Ultimate solution is to improve the schema to prevent multiple occurrences.
        if ( !project.getPairDestinations().isEmpty() )
        {
            DecimalFormat decimalFormatter = null;
            if ( Objects.nonNull( project.getPairDestinations().get( 0 ).getDecimalFormat() ) )
            {
                decimalFormatter = ConfigHelper.getDecimalFormatter( project.getPairDestinations().get( 0 ) );
            }

            sharedSampleWriters =
                    SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(), PairsWriter.DEFAULT_PAIRS_NAME ),
                                                durationUnits,
                                                decimalFormatter );
            // Baseline writer?
            if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
            {
                sharedBaselineSampleWriters = SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(),
                                                                                     PairsWriter.DEFAULT_BASELINE_PAIRS_NAME ),
                                                                          durationUnits,
                                                                          decimalFormatter );
            }
        }

        Set<Path> pathsWrittenTo = new HashSet<>();

        // Iterate the features, closing any shared writers on completion
        SharedStatisticsWriters sharedStatisticsWriters = sharedWritersBuilder.build();

        // Tasks for features
        List<CompletableFuture<Void>> featureTasks = new ArrayList<>();

        // Report on the completion state of all features
        // Report detailed state by default (final arg = true)
        // TODO: demote to summary report (final arg = false) for >> feature count
        FeatureReporter featureReport = new FeatureReporter( projectConfigPlus, decomposedFeatures.size(), true );

        // Deactivate progress monitoring within features, as features are processed asynchronously - the internal
        // completion state of features has no value when reported in this way
        ProgressMonitor.deactivate();

        // Share an instance of a unit mapper across features
        SharedWriters sharedWriters = SharedWriters.of( sharedStatisticsWriters,
                                                        sharedSampleWriters,
                                                        sharedBaselineSampleWriters );
        
        // Create one task per feature
        for ( FeatureTuple feature : decomposedFeatures )
        {
            Supplier<FeatureProcessingResult> featureProcessor = new FeatureProcessor( feature,
                                                                                       resolvedProject,
                                                                                       project,
                                                                                       unitMapper,
                                                                                       executors,
                                                                                       sharedWriters );

            CompletableFuture<Void> nextFeatureTask = CompletableFuture.supplyAsync( featureProcessor,
                                                                                     executors.getFeatureExecutor() )
                                                                       .thenAccept( featureReport );

            // Add to list of tasks
            featureTasks.add( nextFeatureTask );
        }

        // Run the tasks, and join on all tasks. The main thread will wait until all are completed successfully
        // or one completes exceptionally for reasons other than lack of data
        try
        {
            // Complete the feature tasks
            ProcessorHelper.doAllOrException( featureTasks ).join();

            // Find the paths written to by writers
            pathsWrittenTo.addAll( featureReport.getPathsWrittenTo() );

            // Find the paths written to by shared writers
            pathsWrittenTo.addAll( sharedStatisticsWriters.get() );

            if ( sharedWriters.hasSharedSampleWriters() )
            {
                pathsWrittenTo.addAll( sharedWriters.getSampleDataWriters().get() );
            }
            if ( sharedWriters.hasSharedBaselineSampleWriters() )
            {
                pathsWrittenTo.addAll( sharedWriters.getBaselineSampleDataWriters().get() );
            }

            // Report on the features
            featureReport.report();
        }
        catch ( CompletionException e )
        {
            throw new WresProcessingException( "Project failed to complete with the following error: ", e );
        }
        finally
        {
            // Clean up by closing shared writers
            sharedWriters.close();

            // Clean-up an empty output directory: #67088
            try ( Stream<Path> outputs = Files.list( outputDirectory ) )
            {
                if ( outputs.count() == 0 )
                {
                    // Will only succeed for an empty directory
                    boolean status = Files.deleteIfExists( outputDirectory );

                    LOGGER.debug( "Attempted to remove empty output directory {} with success status: {}",
                                  outputDirectory,
                                  status );
                }
            }
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Creates a temporary directory for the outputs with the correct permissions. 
     *
     * @return the path to the temporary output directory
     * @throws IOException if the temporary directory cannot be created
     */

    private static Path createTempOutputDirectory() throws IOException
    {
        // Where outputs files will be written
        Path outputDirectory = null;

        // POSIX-compliant
        if ( FileSystems.getDefault().supportedFileAttributeViews().contains( "posix" ) )
        {
            // Permissions for temp directory require group read so that the tasker
            // may give the output to the client on GET. Write so that the tasker
            // may remove the output on client DELETE. Execute for dir reads.            
            Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                               PosixFilePermission.OWNER_WRITE,
                                                               PosixFilePermission.OWNER_EXECUTE,
                                                               PosixFilePermission.GROUP_READ,
                                                               PosixFilePermission.GROUP_WRITE,
                                                               PosixFilePermission.GROUP_EXECUTE );

            FileAttribute<Set<PosixFilePermission>> fileAttribute =
                    PosixFilePermissions.asFileAttribute( permissions );

            outputDirectory = Files.createTempDirectory( "wres_evaluation_output_",
                                                         fileAttribute );
        }
        // Not POSIX-compliant
        else
        {
            outputDirectory = Files.createTempDirectory( "wres_evaluation_output_" );
        }

        return outputDirectory;
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param <T> the type of future
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
     * <p>Returns a map of features from the <code>featuresToEvaluate</code> for which external thresholds are available
     * in the <code>externalThresholds</code>. The keys within the returned map correspond to the instances within the
     * <code>featuresToEvaluate</code>, which are fully qualified. 
     *
     * <p>Logs a warning message if:
     * 
     * <ol>
     * <li>Thresholds have been defined for one or more features individually; and</li>
     * <li>The list of features that require computation contains one or more features for which
     * thresholds have not been defined</li>
     * </ol>
     * 
     * <p>Returns a map with the fully qualified features from the <code>featuresToEvaluate</code> as keys. 
     * 
     * <p>TODO: superseded by #62205, which aligns the declaration of thresholds and features.
     * 
     * @param externalThresholds the features with thresholds
     * @param featuresToEvaluate the features to evaluate
     * @return The map of filtered features to evaluate
     * @throws IllegalArgumentException if some expected thresholds are missing
     */

    private static Map<FeatureTuple, ThresholdsByMetric>
            reconcileFeaturesAndExternalThresholds( Map<String, ThresholdsByMetric> externalThresholds,
                                                    Set<FeatureTuple> featuresToEvaluate )
    {
        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                      + "thresholds are available.",
                      featuresToEvaluate.size(),
                      externalThresholds.size() );
        Map<FeatureTuple, ThresholdsByMetric> filteredFeatures = new TreeMap<>();

        // Get the thresholds indexed by canonical feature name only
        Map<String, ThresholdsByMetric> externalThresholdsByKey =
                ProcessorHelper.getThresholdsByCanonicalFeatureName( externalThresholds );


        // Iterate the features to evaluate, filtering any for which external thresholds are not available
        Set<FeatureTuple> missingThresholds = new HashSet<>();

        for ( FeatureTuple featureTuple : featuresToEvaluate )
        {
            // The right dataset is the one being evaluated, but the thresholds
            // most commonly apply to the left dataset, so match on the left.
            String featureName = featureTuple.getLeft()
                                             .getName();

            if ( externalThresholdsByKey.containsKey( featureName ) )
            {
                filteredFeatures.put( featureTuple, externalThresholdsByKey.get( featureName ) );
            }
            else
            {
                missingThresholds.add( featureTuple );
            }
        }

        if ( !missingThresholds.isEmpty() && LOGGER.isWarnEnabled() )
        {
            StringJoiner joiner = new StringJoiner( ", " );

            for ( FeatureTuple feature : missingThresholds )
            {
                joiner.add( feature.getLeft()
                                   .getName() );
            }

            LOGGER.debug( "featuresToEvaluate: {}", featuresToEvaluate );
            LOGGER.debug( "externalThresholds: {}", externalThresholds );
            LOGGER.debug( "missingThresholds: {}", missingThresholds );
            LOGGER.warn( "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
                         "While attempting to reconcile the features to ",
                         "evaluate with the features for which thresholds ",
                         "are available, found ",
                         featuresToEvaluate.size(),
                         " features to evaluate and ",
                         externalThresholds.size(),
                         " features for which thresholds are available, but ",
                         missingThresholds.size(),
                         " features for which thresholds could not be ",
                         "reconciled with features to evaluate. Features without ",
                         "thresholds will be skipped. If the number of features ",
                         "without thresholds is larger than expected, ensure ",
                         "that the type of feature name (featureType) is properly ",
                         "declared for the external source of thresholds. The ",
                         "features without thresholds are: ",
                         joiner,
                         "." );
        }

        return Collections.unmodifiableMap( filteredFeatures );
    }

    /**
     * Returns a map of thresholds that are keyed to a canonical identifier, based on the identifier found in the first 
     * entry. Currently limited to {@link FeatureType#NWS_ID} and {@link FeatureType#USGS_ID}. TODO: superseded 
     * by #62205, which aligns the declaration of thresholds and features.
     * 
     * @param thresholds the external thresholds
     * @return a map of thresholds whose keys are partially matched by canonical name only
     */

    private static Map<String, ThresholdsByMetric>
            getThresholdsByCanonicalFeatureName( Map<String, ThresholdsByMetric> thresholds )
    {
        Map<String, ThresholdsByMetric> thresholdsByKey = null;

        if ( !thresholds.isEmpty() )
        {
            LOGGER.debug( "Discovered a source of thresholds by feature." );
            thresholdsByKey = new TreeMap<>();
        }
        else
        {
            throw new IllegalArgumentException( "Could not identify the type of feature name by which to reconcile "
                                                + "features from an external source with features from an internal "
                                                + "source." );
        }

        thresholdsByKey.putAll( thresholds );

        return Collections.unmodifiableMap( thresholdsByKey );
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
         * @param productExecutor the product executor
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

    /**
     * A value object for shared writers.
     */

    static class SharedWriters
    {
        /**
         * Shared writers for statstics.
         */

        private final SharedStatisticsWriters sharedStatisticsWriters;

        /**
         * Shared writers for sample data.
         */

        private final SharedSampleDataWriters sharedSampleWriters;

        /**
         * Shared writers for baseline sampled data.
         */

        private final SharedSampleDataWriters sharedBaselineSampleWriters;

        /**
         * Returns an instance.
         * 
         * @param sharedStatisticsWriters
         * @param sharedSampleWriters
         * @param sharedBaselineSampleWriters
         */
        static SharedWriters of( SharedStatisticsWriters sharedStatisticsWriters,
                                 SharedSampleDataWriters sharedSampleWriters,
                                 SharedSampleDataWriters sharedBaselineSampleWriters )

        {
            return new SharedWriters( sharedStatisticsWriters, sharedSampleWriters, sharedBaselineSampleWriters );
        }

        /**
         * Returns the shared statistics writers.
         * 
         * @return the shared statistics writers.
         */

        SharedStatisticsWriters getStatisticsWriters()
        {
            return this.sharedStatisticsWriters;
        }

        /**
         * Returns the shared sample data writers.
         * 
         * @return the shared sample data writers.
         */

        SharedSampleDataWriters getSampleDataWriters()
        {
            return this.sharedSampleWriters;
        }

        /**
         * Returns the shared sample data writers for baseline data.
         * 
         * @return the shared sample data writers  for baseline data.
         */

        SharedSampleDataWriters getBaselineSampleDataWriters()
        {
            return this.sharedBaselineSampleWriters;
        }

        /**
         * Returns <code>true</code> if shared statistics writers are available, otherwise <code>false</code>.
         * 
         * @return true if shared statistics writers are available
         */

        boolean hasSharedStatisticsWriters()
        {
            return Objects.nonNull( this.sharedStatisticsWriters );
        }

        /**
         * Returns <code>true</code> if shared sample writers are available, otherwise <code>false</code>.
         * 
         * @return true if shared sample writers are available
         */

        boolean hasSharedSampleWriters()
        {
            return Objects.nonNull( this.sharedSampleWriters );
        }

        /**
         * Returns <code>true</code> if shared sample writers are available for the baseline samples, otherwise 
         * <code>false</code>.
         * 
         * @return true if shared sample writers are available for the baseline samples
         */

        boolean hasSharedBaselineSampleWriters()
        {
            return Objects.nonNull( this.sharedBaselineSampleWriters );
        }

        /**
         * Attempts to close all shared writers.
         * @throws IOException when a resource could not be closed
         */

        void close() throws IOException
        {
            if ( this.hasSharedStatisticsWriters() )
            {
                this.getStatisticsWriters().close();
            }

            if ( this.hasSharedSampleWriters() )
            {
                this.getSampleDataWriters().close();
            }

            if ( this.hasSharedBaselineSampleWriters() )
            {
                this.getBaselineSampleDataWriters().close();
            }
        }

        /**
         * Hidden constructor.
         * 
         * @param sharedStatisticsWriters
         * @param sharedSampleWriters
         * @param sharedBaselineSampleWriters
         */
        private SharedWriters( SharedStatisticsWriters sharedStatisticsWriters,
                               SharedSampleDataWriters sharedSampleWriters,
                               SharedSampleDataWriters sharedBaselineSampleWriters )
        {
            this.sharedStatisticsWriters = sharedStatisticsWriters;
            this.sharedSampleWriters = sharedSampleWriters;
            this.sharedBaselineSampleWriters = sharedBaselineSampleWriters;
        }

    }

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }
}
