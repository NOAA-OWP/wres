package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricInputSliceException;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.MetricProcessorByLeadTime.MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link EnsemblePairs} and configured transformations of {@link EnsemblePairs}. For example, metrics that consume
 * {@link SingleValuedPairs} may be processed after transforming the {@link EnsemblePairs} with an appropriate mapping
 * function, such as an ensemble mean.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class MetricProcessorEnsemblePairsByLeadTime extends MetricProcessorByLeadTime
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce
     * {@link VectorOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, VectorOutput> discreteProbabilityVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput> discreteProbabilityMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce {@link ScalarOutput}.
     */

    private final MetricCollection<EnsemblePairs, ScalarOutput> ensembleScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce {@link VectorOutput}.
     */

    private final MetricCollection<EnsemblePairs, VectorOutput> ensembleVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     */

    private final MetricCollection<EnsemblePairs, MultiVectorOutput> ensembleMultiVector;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> toSingleValues;

    /**
     * Function to map between ensemble pairs and discrete probabilities.
     */

    private final BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> toDiscreteProbabilities;

    @Override
    public MetricOutputForProjectByLeadThreshold apply( MetricInput<?> input )
    {
        if ( ! ( input instanceof EnsemblePairs ) )
        {
            throw new MetricCalculationException( "Expected ensemble pairs for metric processing." );
        }
        Integer leadTime = input.getMetadata().getLeadTimeInHours();
        Objects.requireNonNull( leadTime, "Expected a non-null forecast lead time in the input metadata." );

        //Metric futures 
        MetricFuturesByLeadTimeBuilder futures = new MetricFuturesByLeadTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Process the metrics that consume ensemble pairs
        if ( hasMetrics( MetricInputGroup.ENSEMBLE ) )
        {
            processEnsemblePairs( leadTime, (EnsemblePairs) input, futures );
        }
        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            SingleValuedPairs singleValued = slicer.transformPairs( (EnsemblePairs) input, toSingleValues );
            processSingleValuedPairs( leadTime, singleValued, futures );
        }
        //Process the metrics that consume discrete probability pairs
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY ) )
        {
            processDiscreteProbabilityPairs( leadTime, (EnsemblePairs) input, futures );
        }

        // Log
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Completed processing of metrics for feature '{}' at lead time {}.",
                          input.getMetadata().getIdentifier().getGeospatialID(),
                          input.getMetadata().getLeadTimeInHours() );
        }

        //Process and return the result       
        MetricFuturesByLeadTime futureResults = futures.build();
        //Add for merge with existing futures, if required
        addToMergeMap( leadTime, futureResults );
        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorEnsemblePairsByLeadTime( final DataFactory dataFactory,
                                            final ProjectConfig config,
                                            final ExecutorService executor,
                                            final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        super( dataFactory, config, executor, mergeList );
        //Validate the configuration
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            throw new MetricConfigurationException( "Cannot configure dichotomous metrics for ensemble inputs: correct the configuration '"
                                                    + config.getLabel() + "'." );
        }
        if ( hasMetrics( MetricInputGroup.MULTICATEGORY ) )
        {
            throw new MetricConfigurationException( "Cannot configure multicategory metrics for ensemble inputs: correct the configuration '"
                                                    + config.getLabel() + "'." );
        }
        //Construct the metrics
        //Discrete probability input, vector output
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ) )
        {
            discreteProbabilityVector =
                    metricFactory.ofDiscreteProbabilityVectorCollection( executor,
                                                                         getSelectedMetrics( metrics,
                                                                                             MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                             MetricOutputGroup.VECTOR ) );
        }
        else
        {
            discreteProbabilityVector = null;
        }
        //Discrete probability input, multi-vector output
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
        {
            discreteProbabilityMultiVector =
                    metricFactory.ofDiscreteProbabilityMultiVectorCollection( executor,
                                                                              getSelectedMetrics( metrics,
                                                                                                  MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                                  MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            discreteProbabilityMultiVector = null;
        }
        //Ensemble input, vector output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ) )
        {
            if ( metrics.contains( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                 && Objects.isNull( config.getInputs().getBaseline() ) )
            {
                throw new MetricConfigurationException( "Specify a non-null baseline from which to generate the '"
                                                        + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE
                                                        + "'." );
            }
            ensembleVector = metricFactory.ofEnsembleVectorCollection( executor,
                                                                       getSelectedMetrics( metrics,
                                                                                           MetricInputGroup.ENSEMBLE,
                                                                                           MetricOutputGroup.VECTOR ) );
        }
        else
        {
            ensembleVector = null;
        }
        //Ensemble input, scalar output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCALAR ) )
        {
            ensembleScalar = metricFactory.ofEnsembleScalarCollection( executor,
                                                                       getSelectedMetrics( metrics,
                                                                                           MetricInputGroup.ENSEMBLE,
                                                                                           MetricOutputGroup.SCALAR ) );
        }
        else
        {
            ensembleScalar = null;
        }
        //Ensemble input, multi-vector output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            ensembleMultiVector = metricFactory.ofEnsembleMultiVectorCollection( executor,
                                                                                 getSelectedMetrics( metrics,
                                                                                                     MetricInputGroup.ENSEMBLE,
                                                                                                     MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            ensembleMultiVector = null;
        }

        //Construct the default mapper from ensembles to single-values: this is not currently configurable
        toSingleValues = in -> dataFactory.pairOf( in.getItemOne(),
                                                   Arrays.stream( in.getItemTwo() ).average().getAsDouble() );

        //Construct the default mapper from ensembles to probabilities: this is not currently configurable
        toDiscreteProbabilities = dataFactory.getSlicer()::transformPair;
    }

    /**
     * Processes a set of metric futures that consume {@link EnsemblePairs}, which are mapped from the input pairs,
     * {@link EnsemblePairs}, using a configured mapping function.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairs( Integer leadTime, EnsemblePairs input, MetricFuturesByLeadTimeBuilder futures )
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCALAR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.ENSEMBLE ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.ENSEMBLE );
                double[] sorted = getSortedClimatology( input, global );
                global.forEach( threshold -> {
                    Threshold useMe = getThreshold( threshold, sorted );
                    try
                    {
                        futures.addScalarOutput( dataFactory.getMapKey( leadTime, useMe ),
                                                 processEnsembleThreshold( useMe, input, ensembleScalar ) );
                    }
                    catch ( MetricInputSliceException e )
                    {
                        LOGGER.error( e.getMessage(), e );
                    }
                } );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.ENSEMBLE ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.ENSEMBLE );
                double[] sorted = getSortedClimatology( input, global );
                global.forEach( threshold -> {
                    Threshold useMe = getThreshold( threshold, sorted );
                    try
                    {
                        futures.addVectorOutput( dataFactory.getMapKey( leadTime, useMe ),
                                                 processEnsembleThreshold( useMe, input, ensembleVector ) );
                    }
                    catch ( MetricInputSliceException e )
                    {
                        LOGGER.error( e.getMessage(), e );
                    }
                } );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.ENSEMBLE ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.ENSEMBLE );
                double[] sorted = getSortedClimatology( input, global );
                global.forEach( threshold -> {
                    Threshold useMe = getThreshold( threshold, sorted );
                    try
                    {
                        futures.addMultiVectorOutput( dataFactory.getMapKey( leadTime, useMe ),
                                                      processEnsembleThreshold( useMe, input, ensembleMultiVector ) );
                    }
                    catch ( MetricInputSliceException e )
                    {
                        LOGGER.error( e.getMessage(), e );
                    }
                } );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DiscreteProbabilityPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}, using a configured mapping function. Skips any thresholds for which
     * {@link Double#isFinite(double)} returns <code>false</code> on the threshold value(s).
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairs( Integer leadTime,
                                                  EnsemblePairs input,
                                                  MetricFuturesByLeadTimeBuilder futures )
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.DISCRETE_PROBABILITY ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.DISCRETE_PROBABILITY );
                double[] sorted = getSortedClimatology( input, global );
                global.forEach( threshold -> {
                    //Only process discrete thresholds
                    if ( threshold.isFinite() )
                    {
                        Threshold useMe = getThreshold( threshold, sorted );
                        futures.addVectorOutput( dataFactory.getMapKey( leadTime, useMe ),
                                                 processDiscreteProbabilityThreshold( useMe,
                                                                                      input,
                                                                                      discreteProbabilityVector ) );
                    }
                } );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.DISCRETE_PROBABILITY ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.DISCRETE_PROBABILITY );
                double[] sorted = getSortedClimatology( input, global );
                global.forEach( threshold -> {
                    if ( threshold.isFinite() )
                    {
                        Threshold useMe = getThreshold( threshold, sorted );
                        futures.addMultiVectorOutput( dataFactory.getMapKey( leadTime, useMe ),
                                                      processDiscreteProbabilityThreshold( useMe,
                                                                                           input,
                                                                                           discreteProbabilityMultiVector ) );
                    }
                } );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDiscreteProbabilityThreshold( Threshold threshold,
                                                 EnsemblePairs pairs,
                                                 MetricCollection<DiscreteProbabilityPairs, T> collection )
    {
        //Slice the pairs
        DiscreteProbabilityPairs transformed = dataFactory.getSlicer()
                                                          .transformPairs( pairs, threshold, toDiscreteProbabilities );
        return CompletableFuture.supplyAsync( () -> collection.apply( transformed ) );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     * @throws MetricInputSliceException if the threshold fails to slice any data
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processEnsembleThreshold( Threshold threshold,
                                      EnsemblePairs pairs,
                                      MetricCollection<EnsemblePairs, T> collection )
                    throws MetricInputSliceException
    {
        //Slice the pairs
        EnsemblePairs subset = dataFactory.getSlicer().sliceByLeft( pairs, threshold );
        return CompletableFuture.supplyAsync( () -> collection.apply( subset ) );
    }

}
