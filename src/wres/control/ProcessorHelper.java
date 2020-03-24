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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationType;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.SharedStatisticsWriters;
import wres.io.writing.SharedStatisticsWriters.SharedWritersBuilder;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;

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

    static Set<Path> processProjectConfig( final ProjectConfigPlus projectConfigPlus,
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
        UnitMapper unitMapper = UnitMapper.of( desiredMeasurementUnit );
        
        // Read external thresholds from the configuration, per feature
        // Compare on locationId only. TODO: consider how better to transmit these thresholds
        // to wres-metrics, given that they are resolved by project configuration that is
        // passed separately to wres-metrics. Options include moving MetricProcessor* to 
        // wres-control, since they make processing decisions, or passing ResolvedProject onwards
        final Map<FeaturePlus, ThresholdsByMetric> thresholds =
                new TreeMap<>( FeaturePlus::compareByLocationId );
        thresholds.putAll( ConfigHelper.readExternalThresholdsFromProjectConfig( projectConfig, unitMapper ) );

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        Project project = Operations.ingest( projectConfig, lockManager );
        Operations.prepareForExecution( project );

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<FeaturePlus> decomposedFeatures;

        try
        {
            decomposedFeatures = Operations.decomposeFeatures( project );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Failed to retrieve the set of features.", e );
        }

        // Validate the thresholds-by-feature against the features 
        decomposedFeatures = ProcessorHelper.filterOutFeaturesMissingRequiredThresholds( thresholds.keySet(),
                                                                                         decomposedFeatures,
                                                                                         projectConfig );

        // The project code - ideally project hash
        String projectIdentifier = String.valueOf( project.getInputCode() );

        ResolvedProject resolvedProject = ResolvedProject.of( projectConfigPlus,
                                                              decomposedFeatures,
                                                              projectIdentifier,
                                                              thresholds,
                                                              outputDirectory );

        // Obtain the duration units for outputs: #55441
        String durationUnitsString = projectConfig.getOutputs().getDurationFormat().value().toUpperCase();
        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

        // Build any writers of incremental formats that are shared across features
        /* skip the general-purpose incomplete netcdf writer
        final Set<MetricConstants> metricsForSharedWriting = resolvedProject.getDoubleScoreMetrics();
        final int thresholdCountForSharedWriting = resolvedProject.getThresholdCount( MetricOutputGroup.DOUBLE_SCORE );
        */
        SharedWritersBuilder sharedWritersBuilder = new SharedWritersBuilder();
        if ( ConfigHelper.getIncrementalFormats( projectConfig )
                         .contains( DestinationType.NETCDF ) )
        {
            /* skip the general-purpose incomplete netcdf writer
            sharedWritersBuilder.setNetcdfDoublescoreWriter( ConfigHelper.getNetcdfWriter( projectIdentifier,
                                                                                           projectConfig,
                                                                                           resolvedProject.getFeatureCount(),
                                                                                           thresholdCountForSharedWriting,
                                                                                           metricsForSharedWriting ) );
            */
            // Use the gridded netcdf writer
            sharedWritersBuilder.setNetcdfOutputWriter( NetcdfOutputWriter.of( projectConfig,
                                                                               durationUnits,
                                                                               unitMapper,
                                                                               outputDirectory ) );
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
        for ( FeaturePlus feature : decomposedFeatures )
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
     * Returns a list of features that should work within the declaration.
     *
     * <p>Logs a warning message if:
     * 
     * <ol>
     * <li>Thresholds have been defined for one or more features individually; and</li>
     * <li>The list of features that require computation contains one or more features for which
     * thresholds have not been defined; and</li>
     * <li>The project declaration contains metrics that require thresholds.</li>
     * </ol>
     * 
     * <p>In these circumstances, thresholds will be missing for some metrics,
     * and these
     * 
     * @param featuresWithThresholds the features with thresholds
     * @param featuresToEvaluate the features to evaluate
     * @param projectConfig the project declaration
     * @return The list of filtered features to evaluate
     * @throws IllegalArgumentException if some expected thresholds are missing
     */

    private static Set<FeaturePlus>
    filterOutFeaturesMissingRequiredThresholds( Set<FeaturePlus> featuresWithThresholds,
                                                Set<FeaturePlus> featuresToEvaluate,
                                                ProjectConfig projectConfig )
    {
        Set<FeaturePlus> filteredFeatures = new HashSet<>( featuresToEvaluate );

        // Thresholds have been defined by feature, so validate them
        if ( !featuresWithThresholds.isEmpty() )
        {
            // Determine whether any metrics require thresholds
            Set<MetricConstants> metrics = MetricConfigHelper.getMetricsFromConfig( projectConfig );
            boolean requiresThresholds = metrics.stream()
                                                .anyMatch( nextMetric -> nextMetric.isInGroup( SampleDataGroup.DICHOTOMOUS )
                                                                         || nextMetric.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY )
                                                                         || nextMetric.isInGroup( SampleDataGroup.MULTICATEGORY ) );

            // If thresholds are required, check that all features have them 
            if ( requiresThresholds )
            {
                filteredFeatures.clear();
                Set<FeaturePlus> missingThresholds = new HashSet<>();

                for ( FeaturePlus featurePlus : featuresToEvaluate )
                {
                    if ( featuresWithThresholds.contains( featurePlus ) )
                    {
                        filteredFeatures.add( featurePlus );
                    }
                    else
                    {
                        missingThresholds.add( featurePlus );
                    }
                }

                if ( !missingThresholds.isEmpty() && LOGGER.isWarnEnabled() )
                {
                    StringJoiner joiner = new StringJoiner( ", " );

                    for ( FeaturePlus featurePlus : missingThresholds )
                    {
                        String description = ConfigHelper.getFeatureDescription( featurePlus );
                        joiner.add( description );
                    }

                    LOGGER.warn( "{}{}{}{}{}{}{}{}",
                                 "The project declaration contains some ",
                                 "metrics for which thresholds are required, ",
                                 "but some features for which thresholds are ",
                                 "not available. Found the following features ",
                                 "to evaluate in the project for which ",
                                 "thresholds were missing from an ",
                                 "external source of thresholds: ",
                                 joiner );
                }
            }
        }

        return Collections.unmodifiableSet( filteredFeatures );
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
