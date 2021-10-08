package wres.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeSeries;
import wres.events.Evaluation;
import wres.events.EvaluationEventUtilities;
import wres.io.concurrency.Pipelines;
import wres.io.pooling.PoolFactory;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleRetrieverFactory;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.metrics.MetricParameterException;
import wres.pipeline.ProcessorHelper.EvaluationDetails;
import wres.pipeline.ProcessorHelper.Executors;
import wres.pipeline.ProcessorHelper.SharedWriters;
import wres.pipeline.statistics.MetricProcessor;
import wres.pipeline.statistics.MetricProcessorByTimeEnsemblePairs;
import wres.pipeline.statistics.MetricProcessorByTimeSingleValuedPairs;
import wres.statistics.generated.Statistics;

/**
 * Encapsulates a task (with subtasks) for processing all verification results associated with one {@link FeatureTuple}.
 * 
 * @author James Brown
 */

class FeatureProcessor implements Supplier<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureProcessor.class );

    /**
     * The feature group.
     */

    private final FeatureGroup featureGroup;

    /**
     * The project.
     */

    private final Project project;

    /**
     * The unit mapper.
     */

    private final UnitMapper unitMapper;

    /**
     * The resolved project.
     */

    private final ResolvedProject resolvedProject;

    /**
     * The evaluation description.
     */

    private final Evaluation evaluation;

    /**
     * The executors services.
     */

    private final Executors executors;

    /**
     * The shared writers.
     */

    private final SharedWriters sharedWriters;

    /**
     * Error message.
     */

    private final String errorMessage;

    /**
     * Monitor.
     */

    private final EvaluationEvent monitor;

    /**
     * A unique identifier for the feature group for messaging purposes.
     */

    private final String groupIdForMessaging;

    /**
     * Build a processor. 
     * 
     * @param evaluationDetails the evaluation details
     * @param featureGroup the feature group to evaluate
     * @param unitMapper the unit mapper
     * @param executors the executors for pairs, thresholds, and metrics
     * @param sharedWriters shared writers
     * @throws NullPointerException if any required input is null
     */

    FeatureProcessor( EvaluationDetails evaluationDetails,
                      FeatureGroup featureGroup,
                      UnitMapper unitMapper,
                      Executors executors,
                      SharedWriters sharedWriters )
    {
        Project localProject = evaluationDetails.getProject();
        ResolvedProject localResolvedProject = evaluationDetails.getResolvedProject();
        Evaluation localEvaluation = evaluationDetails.getEvaluation();
        EvaluationEvent localMonitor = evaluationDetails.getMonitor();

        Objects.requireNonNull( featureGroup );
        Objects.requireNonNull( localProject );
        Objects.requireNonNull( localResolvedProject );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( sharedWriters );
        Objects.requireNonNull( localEvaluation );
        Objects.requireNonNull( localMonitor );

        this.featureGroup = featureGroup;
        this.resolvedProject = localResolvedProject;
        this.project = localProject;
        this.unitMapper = unitMapper;
        this.executors = executors;
        this.sharedWriters = sharedWriters;
        this.evaluation = localEvaluation;
        this.monitor = localMonitor;
        this.groupIdForMessaging = EvaluationEventUtilities.getUniqueId();

        // Error message
        this.errorMessage = "While processing feature group " + featureGroup;
    }

    @Override
    public FeatureProcessingResult get()
    {
        // Report
        LOGGER.debug( "Started feature group '{}'", this.getFeatureGroup() );

        ProjectConfig projectConfig = this.resolvedProject.getProjectConfig();
        List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature =
                this.resolvedProject.getThresholdsByMetricAndFeature();

        // TODO: do NOT rely on the declared type. Instead, determine it, post-ingest,
        // from the ResolvedProject. See #57301.
        DatasourceType type = projectConfig.getInputs()
                                           .getRight()
                                           .getType();

        // In future, other types of pools may be handled here
        // Pairs that contain ensemble forecasts
        if ( type == DatasourceType.ENSEMBLE_FORECASTS )
        {

            // Create a feature-shaped retriever factory to support retrieval for this project
            RetrieverFactory<Double, Ensemble> retrieverFactory = EnsembleRetrieverFactory.of( this.project,
                                                                                               this.getFeatureGroup(),
                                                                                               this.unitMapper );

            List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> pools =
                    PoolFactory.getEnsemblePools( this.evaluation.getEvaluationDescription(),
                                                  this.project,
                                                  this.getFeatureGroup(),
                                                  this.unitMapper,
                                                  retrieverFactory );
            this.monitor.setPoolCount( pools.size() );

            // Stand-up the pair writers
            PairsWriter<Double, Ensemble> pairsWriter = null;
            PairsWriter<Double, Ensemble> basePairsWriter = null;
            if ( this.sharedWriters.hasSharedSampleWriters() )
            {
                pairsWriter = this.sharedWriters.getSampleDataWriters().getEnsembleWriter();
            }
            if ( this.sharedWriters.hasSharedBaselineSampleWriters() )
            {
                basePairsWriter = this.sharedWriters.getBaselineSampleDataWriters().getEnsembleWriter();
            }

            List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                    this.getEnsembleProcessors( thresholdsByMetricAndFeature );

            return this.processFeature( this.evaluation,
                                        projectConfig,
                                        processors,
                                        pools,
                                        pairsWriter,
                                        basePairsWriter,
                                        this.getEnsembleTraceCountEstimator() );
        }
        // All other types
        else
        {
            // Create a feature-shaped retriever factory to support retrieval for this project
            RetrieverFactory<Double, Double> retrieverFactory = SingleValuedRetrieverFactory.of( this.project,
                                                                                                 this.unitMapper );

            List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> pools =
                    PoolFactory.getSingleValuedPools( this.evaluation.getEvaluationDescription(),
                                                      this.project,
                                                      this.featureGroup,
                                                      this.unitMapper,
                                                      retrieverFactory );
            this.monitor.setPoolCount( pools.size() );

            // Stand-up the pair writers
            PairsWriter<Double, Double> pairsWriter = null;
            PairsWriter<Double, Double> basePairsWriter = null;
            if ( this.sharedWriters.hasSharedSampleWriters() )
            {
                pairsWriter = this.sharedWriters.getSampleDataWriters().getSingleValuedWriter();
            }
            if ( this.sharedWriters.hasSharedBaselineSampleWriters() )
            {
                basePairsWriter = this.sharedWriters.getBaselineSampleDataWriters().getSingleValuedWriter();
            }

            List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                    this.getSingleValuedProcessors( thresholdsByMetricAndFeature );

            return this.processFeature( this.evaluation,
                                        projectConfig,
                                        processors,
                                        pools,
                                        pairsWriter,
                                        basePairsWriter,
                                        this.getSingleValuedTraceCountEstimator() ); // Two traces per pool
        }
    }

    /**
     * Processes a feature.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param evaluation the evaluation
     * @param projectConfig the project declaration
     * @param processors the metric processors
     * @param pools the data pools
     * @param pairsWriter the pairs writer
     * @param basePairsWriter the baseline pairs writer
     * @param traceCountEstimator estimates the trace count in a pool
     * @return a processing result
     */

    private <L, R> FeatureProcessingResult processFeature( Evaluation evaluation,
                                                           ProjectConfig projectConfig,
                                                           List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> processors,
                                                           List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> pools,
                                                           PairsWriter<L, R> pairsWriter,
                                                           PairsWriter<L, R> basePairsWriter,
                                                           ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator )
    {
        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        List<CompletableFuture<Void>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        LOGGER.debug( "Submitting {} pools in group {} with identifier {} for asynchronous processing.",
                      pools.size(),
                      this.getFeatureGroup().getName(),
                      this.getGroupIdForMessaging() );

        // Something published?
        AtomicBoolean published = new AtomicBoolean();
        AtomicReference<Set<FeatureTuple>> featuresWithData = new AtomicReference<>( new HashSet<>() );

        for ( Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier : pools )
        {
            // Behold, the feature processing pipeline
            final CompletableFuture<Void> pipeline =
                    // Retrieve the pairs                
                    CompletableFuture.supplyAsync( poolSupplier, this.executors.getPairExecutor() )
                                     // Write the main pairs, as needed
                                     .thenApply( this.getPairWritingTask( false, pairsWriter, projectConfig ) )
                                     // Write the baseline pairs, as needed
                                     .thenApply( this.getPairWritingTask( true, basePairsWriter, projectConfig ) )
                                     // Compute the statistics
                                     .thenApply( this.getStatisticsProcessingTask( processors,
                                                                                   projectConfig,
                                                                                   traceCountEstimator,
                                                                                   featuresWithData ) )
                                     // Publish the statistics for awaiting format consumers
                                     .thenAcceptAsync( statistics -> {

                                         boolean success = this.publish( evaluation,
                                                                         statistics,
                                                                         this.getGroupIdForMessaging() );

                                         // Notify that something was published
                                         // This is needed to confirm group completion - cannot complete a message
                                         // group if nothing was published to it.
                                         if ( success )
                                         {
                                             published.set( true );
                                         }

                                     },
                                                       // Consuming happens in the product thread pool and publishing
                                                       // should happen in a different one because production is
                                                       // flow-controlled with respect to consumption using a naive 
                                                       // blocking approach, which would otherwise risk deadlock. 
                                                       this.executors.getPairExecutor() );

            // Add the task to the list
            listOfFutures.add( pipeline );
        }

        // Complete all tasks or one exceptionally
        try
        {
            // Wait for completion of all data slices
            Pipelines.doAllOrException( listOfFutures ).join();

            // Published? Then mark complete.
            if ( published.get() )
            {
                // Notify consumers that all statistics for this group have been published
                evaluation.markGroupPublicationCompleteReportedSuccess( this.getGroupIdForMessaging() );
            }
        }
        catch ( CompletionException e )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( this.errorMessage, e );
        }

        return new FeatureProcessingResult( this.featureGroup, featuresWithData.get(), published.get() );
    }

    /**
     * Publishes the statistics to an evaluation.
     * 
     * @param evaluation the evaluation
     * @param statistics the statistics
     * @param groupId the statistics group identifier
     * @return true if something was published, otherwise false
     * @throws EvaluationEventException if the statistics could not be published
     */

    private boolean publish( Evaluation evaluation,
                             List<StatisticsForProject> statistics,
                             String groupId )
    {
        Objects.requireNonNull( evaluation, "Cannot publish statistics without an evaluation." );
        Objects.requireNonNull( statistics, "Cannot publish null statistics." );
        Objects.requireNonNull( groupId, "Cannot publish statistics without a group identifier." );

        boolean returnMe = false;

        try
        {
            for ( StatisticsForProject nextStatistics : statistics )
            {
                Collection<Statistics> publishMe = MessageFactory.parse( nextStatistics );

                for ( Statistics next : publishMe )
                {
                    evaluation.publish( next, groupId );
                    returnMe = true;
                }
            }

            LOGGER.debug( "Published statistics: {}.", returnMe );

        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new WresProcessingException( "Interrupted while completing evaluation "
                                               + evaluation.getEvaluationId()
                                               + ".",
                                               e );
        }

        return returnMe;
    }

    /**
     * @return the feature group identifier
     */

    private String getGroupIdForMessaging()
    {
        return this.groupIdForMessaging;
    }

    /**
     * Returns a task that writes pairs. Returns an empty set of paths, since pairs are not written per feature. Paths
     * to pairs should be reported for all features, not per feature. See #71874.
     * 
     * @param <L> the left type of data
     * @param <R> the right type of data
     * @param useBaseline is true to write the baseline pairs
     * @param sharedWriters the consumers of paired data for writing
     * @param projectConfig the project declaration
     * @return a task that writes pairs
     */

    private <L, R> UnaryOperator<Pool<TimeSeries<Pair<L, R>>>> getPairWritingTask( boolean useBaseline,
                                                                                   PairsWriter<L, R> sharedWriters,
                                                                                   ProjectConfig projectConfig )
    {
        return pairs -> {

            if ( Objects.nonNull( sharedWriters ) )
            {
                // Baseline data?
                if ( useBaseline && Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
                {
                    sharedWriters.accept( pairs.getBaselineData() );
                }
                else
                {
                    sharedWriters.accept( pairs );
                }
            }
            // #71874: pairs are not written per feature so do not report on the 
            // paths to pairs for each feature. Report on the pairs for all features.
            return pairs;
        };
    }

    /**
     * Returns a function that consumes a {@link Pool} and produces {@link StatisticsForProject}.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param processors the metric processors
     * @param projectConfig the project declaration
     * @param traceCountEstimator a function that estimates trace count, in order to help with monitoring
     * @param featuresWithData an atomic reference to update that includes the features with some data
     * @return a function that consumes a pool and produces one blob of statistics for each processor
     */

    private <L, R> Function<Pool<TimeSeries<Pair<L, R>>>, List<StatisticsForProject>>
            getStatisticsProcessingTask( List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> processors,
                                         ProjectConfig projectConfig,
                                         ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator,
                                         AtomicReference<Set<FeatureTuple>> featuresWithData )
    {
        return pool -> {
            Objects.requireNonNull( pool );

            List<StatisticsForProject> returnMe = new ArrayList<>();

            // No data in the composition
            if ( pool.get().isEmpty()
                 && ( !pool.hasBaseline() || pool.getBaselineData().get().isEmpty() ) )
            {
                LOGGER.debug( "Empty pool discovered for {}: no statistics will be produced.", pool.getMetadata() );

                // Empty container
                StatisticsForProject empty = new StatisticsForProject.Builder().build();
                returnMe.add( empty );

                return returnMe;
            }

            // Register features with data
            this.registerFeaturesWithData( pool, featuresWithData );
            
            // Implement all processing and store the results
            try
            {
                // One blob of statistics for each processor, one processor for each metrics declaration
                for ( MetricProcessor<Pool<TimeSeries<Pair<L, R>>>> processor : processors )
                {
                    StatisticsForProject statistics = processor.apply( pool );
                    StatisticsForProject.Builder builder = new StatisticsForProject.Builder();

                    builder.addStatistics( statistics )
                           .setMinimumSampleSize( processor.getMetrics().getMinimumSampleSize() );

                    // Compute separate statistics for the baseline?
                    int baselineTraceCount = 0;
                    if ( pool.hasBaseline() )
                    {
                        Pool<TimeSeries<Pair<L, R>>> baseline = pool.getBaselineData();

                        if ( projectConfig.getInputs().getBaseline().isSeparateMetrics() )
                        {
                            LOGGER.debug( "Computing separate statistics for the baseline pairs associated with pool {}.",
                                          baseline.getMetadata() );

                            StatisticsForProject baselineStatistics = processor.apply( baseline );
                            builder.addStatistics( baselineStatistics );
                        }

                        baselineTraceCount = traceCountEstimator.applyAsInt( baseline );
                    }

                    StatisticsForProject nextStatistics = builder.build();
                    returnMe.add( nextStatistics );

                    this.monitor.registerPool( pool, traceCountEstimator.applyAsInt( pool ), baselineTraceCount );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new WresProcessingException( this.errorMessage, e );
            }

            return Collections.unmodifiableList( returnMe );
        };
    }

    /**
     * Registers features that produced some data.
     * @param <L> the left pair type
     * @param <R> the right pair type
     * @param pool the pool
     * @param featuresWithData an atomic reference to update with the set of features that produced some data
     */
    
    private <L, R> void registerFeaturesWithData( Pool<TimeSeries<Pair<L, R>>> pool,
                                                  AtomicReference<Set<FeatureTuple>> featuresWithData )
    {
        UnaryOperator<Set<FeatureTuple>> operator = tuples -> {
            Set<FeatureTuple> returnMe = new HashSet<>( tuples );
            Set<FeatureTuple> existingTuples = pool.getMiniPools()
                                                   .stream()
                                                   .filter( next -> !next.get().isEmpty() )
                                                   .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                                                   .collect( Collectors.toSet() );
            returnMe.addAll( existingTuples );

            return returnMe;
        };

        featuresWithData.getAndUpdate( operator );
    }
    
    /**
     * @param metrics the metrics
     * @return the ensemble processors
     * @throws MetricParameterException if any metric parameters are incorrect
     */

    private List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
            getEnsembleProcessors( List<ThresholdsByMetricAndFeature> metrics )
    {
        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors = new ArrayList<>();

        for ( ThresholdsByMetricAndFeature nextMetrics : metrics )
        {
            FeatureGroup featureGroup = this.getFeatureGroup();
            ThresholdsByMetricAndFeature nextGroup = nextMetrics.getThresholdsByMetricAndFeature( featureGroup );
            MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> nextProcessor =
                    new MetricProcessorByTimeEnsemblePairs( nextGroup,
                                                            this.executors.getThresholdExecutor(),
                                                            this.executors.getMetricExecutor() );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @param metrics the metrics
     * @return the single-valued processors
     * @throws MetricParameterException if any metric parameters are incorrect
     */

    private List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
            getSingleValuedProcessors( List<ThresholdsByMetricAndFeature> metrics )
    {
        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors = new ArrayList<>();

        for ( ThresholdsByMetricAndFeature nextMetrics : metrics )
        {
            FeatureGroup featureGroup = this.getFeatureGroup();
            ThresholdsByMetricAndFeature nextGroup = nextMetrics.getThresholdsByMetricAndFeature( featureGroup );
            MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> nextProcessor =
                    new MetricProcessorByTimeSingleValuedPairs( nextGroup,
                                                                this.executors.getThresholdExecutor(),
                                                                this.executors.getMetricExecutor() );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @return a function that estimates the number of traces in a pool of single-valued time-series
     */

    private ToIntFunction<Pool<TimeSeries<Pair<Double, Double>>>> getSingleValuedTraceCountEstimator()
    {
        return pool -> 2 * pool.get().size();
    }

    /**
     * @return a function that estimates the number of traces in a pool of ensemble time-series
     */

    private ToIntFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> getEnsembleTraceCountEstimator()
    {
        return pool -> pool.get()
                           .size()
                       * ( pool.get()
                               .get( 0 )
                               .getEvents()
                               .first()
                               .getValue()
                               .getRight()
                               .size()
                           + 1 );
    }

    /**
     * @return the feature group
     */

    private FeatureGroup getFeatureGroup()
    {
        return this.featureGroup;
    }

}
