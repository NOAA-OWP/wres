package wres.control;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.control.ProcessorHelper.ExecutorServices;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.DataGenerator;
import wres.util.IterationFailedException;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.SharedStatisticsWriters;

/**
 * Encapsulates a task (with subtasks) for processing all verification results associated with one {@link FeaturePlus}.
 * 
 * @author james.brown@hydrosolved.com
 */

class FeatureProcessor implements Supplier<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureProcessor.class );
    
    /**
     * The feature.
     */

    private final FeaturePlus feature;

    /**
     * The resolved project.
     */

    private final ResolvedProject resolvedProject;

    /**
     * The project details.
     */

    private final Project project;

    /**
     * The executors services.
     */

    private final ExecutorServices executors;

    /**
     * The shared writers.
     */

    private final SharedStatisticsWriters sharedWriters;

    /**
     * Error message.
     */

    private final String errorMessage;

    /**
     * Shared writers for paired sample data.
     */
    
    private SharedSampleDataWriters sharedSampleWriters;

    /**
     * Shared writers for paired sample data associated with a baseline source.
     */
    
    private SharedSampleDataWriters sharedBaselineSampleWriters;

    /**
     * Build a processor. 
     * 
     * @param feature the feature to process
     * @param resolvedProject the resolved project
     * @param project the project details to use
     * @param executors the executors for pairs, thresholds, and metrics
     * @param sharedSampleWriters writers of sample data that are shared across features
     * @param sharedBaselineSampleWriters writers of baseline sample data that are shared across features
     */

    FeatureProcessor( FeaturePlus feature,
                      ResolvedProject resolvedProject,
                      Project project,
                      ExecutorServices executors,
                      SharedStatisticsWriters sharedWriters,
                      SharedSampleDataWriters sharedSampleWriters,
                      SharedSampleDataWriters sharedBaselineSampleWriters )
    {
        this.feature = feature;
        this.resolvedProject = resolvedProject;
        this.project = project;
        this.executors = executors;
        this.sharedWriters = sharedWriters;
        this.sharedSampleWriters = sharedSampleWriters;
        this.sharedBaselineSampleWriters = sharedBaselineSampleWriters;

        // Error message
        String featureDescription = ConfigHelper.getFeatureDescription( this.feature );
        errorMessage = "While processing feature " + featureDescription;
    }

    @Override
    public FeatureProcessingResult get()
    {
        // Report
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Started feature '{}'",
                         ConfigHelper.getFeatureDescription( this.feature.getFeature() ) );
        }
        
        final ProjectConfig projectConfig = this.resolvedProject.getProjectConfig();
        final ThresholdsByMetric thresholds =
                this.resolvedProject.getThresholdForFeature( this.feature );


        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        final MetricProcessorForProject processor;
        try
        {
            processor = MetricFactory.ofMetricProcessorForProject( projectConfig,
                                                                   thresholds,
                                                                   this.executors.getThresholdExecutor(),
                                                                   this.executors.getMetricExecutor() );
        }
        catch ( final MetricParameterException e )
        {
            throw new WresProcessingException( this.errorMessage, e );
        }

        // Build an InputGenerator for the next feature
        DataGenerator metricInputs = Operations.getInputs( this.project,
                                                           this.feature.getFeature(),
                                                           this.resolvedProject.getOutputDirectory() );

        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        final List<CompletableFuture<Set<Path>>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // During the pipeline, only write types that are not end-of-pipeline types unless they refer to
        // a format that can be written incrementally
        BiPredicate<StatisticGroup, DestinationType> onlyWriteTheseTypes =
                ( type, format ) -> !processor.getMetricOutputTypesToCache().contains( type )
                                    || ConfigHelper.getIncrementalFormats( projectConfig ).contains( format );

        try
        {
            // Iterate
            for ( final Future<SampleData<?>> nextInput : metricInputs )
            {
                // Complete all statistics tasks asynchronously:
                // 1. Get some sample data from the database
                // 2. Compute statistics from the sample data
                // 3. Produce outputs from the statistics                
                SupplySampleData pairSupplier = SupplySampleData.of( nextInput );
                
                final CompletableFuture<Set<Path>> statisticsTasks =
                        CompletableFuture.supplyAsync( pairSupplier, this.executors.getPairExecutor() )
                                         .thenApplyAsync( ProduceStatisticsFromSampleData.of( processor ),
                                                          this.executors.getPairExecutor() )
                                         .thenApplyAsync( metricOutputs -> {
                                             ProduceOutputsFromStatistics outputProcessor =
                                                     ProduceOutputsFromStatistics.of( this.resolvedProject,
                                                                                      onlyWriteTheseTypes,
                                                                                      this.sharedWriters );
                                             outputProcessor.accept( metricOutputs );
                                             outputProcessor.close();
                                             return outputProcessor.get();
                                         },
                                                          this.executors.getProductExecutor() );

                // Add the task to the list
                listOfFutures.add( statisticsTasks );               

                // Create a task for serializing the sample data
                if ( Objects.nonNull( this.sharedSampleWriters ) )
                {
                    CompletableFuture<Set<Path>> sampleDataTask =
                            this.getPairWritingTask( pairSupplier, this.sharedSampleWriters );

                    listOfFutures.add( sampleDataTask );
                }

                // Create a task for serializing the baseline data
                if ( Objects.nonNull( projectConfig.getInputs().getBaseline() )
                     && Objects.nonNull( this.sharedBaselineSampleWriters ) )
                {
                    CompletableFuture<Set<Path>> baselineSampleDataTask =
                            this.getPairWritingTask( SupplySampleData.of( nextInput, true ),
                                                     this.sharedBaselineSampleWriters );

                    listOfFutures.add( baselineSampleDataTask );
                }
            }
        }
        catch ( IterationFailedException re )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( "Iteration failed", re );
        }

        // Complete all tasks or one exceptionally
        try
        {
            // Wait for completion of all data slices
            ProcessorHelper.doAllOrException( listOfFutures ).join();
        }
        catch ( CompletionException e )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( this.errorMessage, e );
        }

        // Generate cached output if available
        Set<Path> endOfPipelinePaths = this.generateEndOfPipelineProducts( processor );

        Set<Path> paths = new HashSet<>( endOfPipelinePaths );

        // Unearth the Set<Path> inside listOfFutures now that join() was called
        // above.
        for ( CompletableFuture<Set<Path>> completedFuture : listOfFutures )
        {
            Set<Path> innerPaths = completedFuture.getNow( Collections.emptySet() );
            paths.addAll( innerPaths );
        }

        Set<Path> allPaths = Collections.unmodifiableSet( paths );

        return new FeatureProcessingResult( this.feature.getFeature(),
                                            allPaths );
    }
    
    /**
     * Returns a task that writes pairs.
     * 
     * @param pairSupplier the supplier of paired data to write
     * @param sharedWriters the consumers of paired data for writing
     */

    private CompletableFuture<Set<Path>> getPairWritingTask( SupplySampleData pairSupplier,
                                                             SharedSampleDataWriters sharedWriters )
    {
        return CompletableFuture.supplyAsync( pairSupplier, this.executors.getProductExecutor() )
                                .thenApplyAsync( sampleData -> {
                                    sharedWriters.accept( sampleData );
                                    return sharedWriters.get();
                                },
                                                 this.executors.getProductExecutor() );
    }
    
    /**
     * Generates products at the end of the processing pipeline.
     * 
     * @param processor the processor from which to obtain the inputs for product generation
     */

    private Set<Path> generateEndOfPipelineProducts( MetricProcessorForProject processor )
    {
        if ( processor.hasCachedMetricOutput() )
        {
            try
            {
                // Determine the cached types
                Set<StatisticGroup> cachedTypes = processor.getCachedMetricOutputTypes();

                // Only process cached types that were not written incrementally
                BiPredicate<StatisticGroup, DestinationType> nowWriteTheseTypes =
                        ( type, format ) -> cachedTypes.contains( type )
                                            && !ConfigHelper.getIncrementalFormats( this.resolvedProject.getProjectConfig() )
                                                            .contains( format );
                try ( // End of pipeline processor
                      ProduceOutputsFromStatistics endOfPipeline =
                              ProduceOutputsFromStatistics.of( this.resolvedProject,
                                                               nowWriteTheseTypes,
                                                               this.sharedWriters ) )
                {
                    // Generate output
                    endOfPipeline.accept( processor.getCachedMetricOutput() );
                    return endOfPipeline.get();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new WresProcessingException( this.errorMessage, e );
            }
        }

        return Collections.emptySet();
    }

}
