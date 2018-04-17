package wres.engine.statistics.metric;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.engine.statistics.metric.categorical.ContingencyTable;

/**
 * <p>
 * An immutable collection of {@link Metric} that consume a common class of {@link MetricInput} and return a common
 * class of {@link MetricOutput}. Multiple instances of the same metric are allowed (e.g. with different parameter
 * values).
 * </p>
 * <p>
 * For metrics that implement {@link Collectable} and whose method {@link Collectable#getCollectionOf()} returns a
 * common superclass (by {@link Metric#getID()}), the intermediate output is computed once and applied to all 
 * subclasses within the collection. For example, if the {@link MetricCollection} contains several {@link Score} that 
 * extend {@link ContingencyTable} and implement {@link Collectable}, the contingency table will be computed once, 
 * with all dependent scores using this result.
 * </p>
 * <p>
 * Build a collection with a {@link MetricCollectionBuilder#of()}.
 * </p>
 * <p>
 * When a group contains a collection of metrics that do not need to be computed for all inputs, a non-empty set of
 * {@link MetricConstants} may be defined. These metrics are ignored during calculation.
 * </p>
 * 
 * @param <S> the input type
 * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
 * @param <U> the output type
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricCollection<S extends MetricInput<?>, T extends MetricOutput<?>, U extends MetricOutput<?>>
        implements BiFunction<S, Set<MetricConstants>, MetricOutputMapByMetric<U>>
{

    /**
     * Logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger( MetricCollection.class );

    /**
     * Instance of a {@link DataFactory} for constructing a {@link MetricOutput}.
     */

    private final DataFactory dataFactory;

    /**
     * A collection of {@link Metric} that are not {@link Collectable}.
     */

    private final Map<MetricConstants, Metric<S, U>> metrics;

    /**
     * A collection of {@link Metric} that are {@link Collectable}. The metrics are indexed by 
     * {@link Collectable#getCollectionOf()}.
     */

    private final Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> collectableMetrics;

    /**
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    /**
     * Applies the input to the collection for all metrics in the collection.
     * 
     * @param input the input
     * @return the collection output
     */

    public MetricOutputMapByMetric<U> apply( final S input )
    {
        return this.applyParallel( input, Collections.emptySet() );
    }

    /**
     * Computes all metrics except the metrics in the ignore set.
     * 
     * @param input the input
     * @param ignoreTheseMetrics the set of metrics to ignore
     */

    @Override
    public MetricOutputMapByMetric<U> apply( final S input, final Set<MetricConstants> ignoreTheseMetrics )
    {
        return this.applyParallel( input, ignoreTheseMetrics );
    }

    /**
     * A builder to build the immutable collection.
     * 
     * @param <S> the input type
     * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
     * @param <U> the output type
     */

    protected static class MetricCollectionBuilder<S extends MetricInput<?>, T extends MetricOutput<?>, U extends MetricOutput<?>>
    {

        /**
         * Executor service. By default, uses {@link ForkJoinPool#commonPool()}.
         */

        private ExecutorService metricPool;

        /**
         * The {@link DataFactory} to build a {@link MetricOutput}.
         */

        private DataFactory dataFactory;

        /**
         * The list of {@link Metric}.
         */

        private final Map<MetricConstants, Metric<S, U>> metrics = new EnumMap<>( MetricConstants.class );

        /**
         * The list of {@link Collectable}.
         */

        private final Map<MetricConstants, Collectable<S, T, U>> collectableMetrics =
                new EnumMap<>( MetricConstants.class );

        /**
         * Returns a builder.
         * 
         * @param <P> the type of metric input
         * @param <Q> the type of intermediate output for a {@link Collectable} metric
         * @param <R> the type of metric output
         * @return a builder
         */

        protected static <P extends MetricInput<?>, Q extends MetricOutput<?>, R extends MetricOutput<?>>
                MetricCollectionBuilder<P, Q, R> of()
        {
            return new MetricCollectionBuilder<>();
        }

        /**
         * Add a {@link Metric} to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> addMetric( final Metric<S, U> metric )
        {
            this.metrics.put( metric.getID(), metric );
            return this;
        }

        /**
         * Add a {@link Collectable} metric to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> addCollectable( final Collectable<S, T, U> metric )
        {
            this.collectableMetrics.put( metric.getID(), metric );
            return this;
        }

        /**
         * Sets the {@link ExecutorService} for parallel computations.
         * 
         * @param metricPool the executor service
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> setExecutorService( final ExecutorService metricPool )
        {
            this.metricPool = metricPool;
            return this;
        }

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

        /**
         * Build the metric collection.
         * 
         * @return the metric
         * @throws MetricParameterException if one or more parameters is invalid
         */

        protected MetricCollection<S, T, U> build() throws MetricParameterException
        {
            return new MetricCollection<>( this );
        }

    }

    /**
     * Default method for computing the metric results and returning them in a collection with a prescribed type.
     * Collects instances of {@link Collectable} by {@link Collectable#getCollectionOf()} and computes their common
     * (intermediate) input once. Computes the metrics in parallel using the supplied {@link #metricPool} or, where not
     * supplied, the {@link ForkJoinPool#commonPool()}.
     * 
     * @param input the metric input
     * @param ignoreTheseMetrics a set of metrics that should be ignored when computing this collection
     * @return the output for each metric, contained in a collection
     * @throws MetricCalculationException if the calculation fails for any reason, with the cause set
     */

    private MetricOutputMapByMetric<U> applyParallel( final S input, Set<MetricConstants> ignoreTheseMetrics )
            throws MetricCalculationException
    {
        //Bounds checks
        if ( Objects.isNull( input ) )
        {
            throw new MetricCalculationException( "Specify non-null input to the metric collection." );
        }
        if ( Objects.isNull( ignoreTheseMetrics ) )
        {
            throw new MetricCalculationException( "Specify a non-null set of metrics to ignore, such as the empty "
                                                  + "set." );
        }
        
        // Count elements in metrics and collected metrics
        int count = this.metrics.size() + this.collectableMetrics.values().stream().mapToInt( Map::size ).sum();
        if ( ignoreTheseMetrics.size() == count )
        {
            throw new MetricCalculationException( "Cannot ignore all metrics in the store: specify some metrics "
                                                  + "to process." );
        }

        //Compute only the required metrics
        Map<MetricConstants, Metric<S, U>> localMetrics = new EnumMap<>( this.metrics );
        Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> localCollectableMetrics =
                new EnumMap<>( this.collectableMetrics );
        localMetrics.keySet().removeAll( ignoreTheseMetrics );
        localCollectableMetrics.keySet().removeAll( ignoreTheseMetrics );
        
        //Remove from each map in the collection
        localCollectableMetrics.forEach( ( key, value ) -> value.keySet().removeAll( ignoreTheseMetrics ) );

        //Collection of future metric results
        final Map<MetricConstants, CompletableFuture<U>> metricFutures = new EnumMap<>( MetricConstants.class );

        //Create the futures for the collectable metrics
        for ( Map<MetricConstants, Collectable<S, T, U>> next : localCollectableMetrics.values() )
        {
            // Proceed
            if ( ! next.isEmpty() )
            {
                Iterator<Collectable<S, T, U>> iterator = next.values().iterator();
                
                Collectable<S, T, U> baseMetric = iterator.next();
                final CompletableFuture<T> baseFuture =
                        CompletableFuture.supplyAsync( () -> baseMetric.getInputForAggregation( input ),
                                                       this.metricPool );
                //Using the future dependent result, compute a future of each of the independent results
                next.forEach( ( id, metric ) -> metricFutures.put( id,
                                                                   baseFuture.thenApplyAsync( metric::aggregate,
                                                                                              this.metricPool ) ) );
            }
        }
        //Create the futures for the ordinary metrics
        localMetrics.forEach( ( key, value ) -> metricFutures.put( key,
                                                                   CompletableFuture.supplyAsync( () -> value.apply( input ),
                                                                                                  this.metricPool ) ) );
        //Compute the results
        Map<MetricConstants, U> returnMe = new EnumMap<>( MetricConstants.class );
        MetricConstants nextMetric = null;

        this.logStartOfCalculation();

        try
        {
            for ( Map.Entry<MetricConstants, CompletableFuture<U>> nextResult : metricFutures.entrySet() )
            {
                nextMetric = nextResult.getKey();
                returnMe.put( nextMetric, nextResult.getValue().get() ); //This is blocking
            }
        }
        catch ( ExecutionException e )
        {
            throw new MetricCalculationException( "While processing metric '" + nextMetric + "'.", e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new MetricCalculationException( "While processing metric '" + nextMetric + "'.", e );
        }

        this.logEndOfCalculation( returnMe );

        return this.dataFactory.ofMetricOutputMapByMetric( returnMe );
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private MetricCollection( final MetricCollectionBuilder<S, T, U> builder ) throws MetricParameterException
    {
        //Set 
        this.dataFactory = builder.dataFactory;
        this.metricPool = builder.metricPool;
        this.metrics = new EnumMap<>( builder.metrics );
        this.collectableMetrics = new EnumMap<>( MetricConstants.class );

        //Set the collectable metrics
        builder.collectableMetrics.forEach( ( id, metric ) -> {
            if ( collectableMetrics.containsKey( metric.getCollectionOf() ) )
            {
                this.collectableMetrics.get( metric.getCollectionOf() ).put( id, metric );
            }
            else
            {
                Map<MetricConstants, Collectable<S, T, U>> addMe = new EnumMap<>( MetricConstants.class );
                addMe.put( id, metric );
                this.collectableMetrics.put( metric.getCollectionOf(), addMe );
            }
        } );

        validate();
    }

    /**
     * Validates the copied collection parameters on construction.
     * 
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private void validate() throws MetricParameterException
    {
        //Validate 
        if ( Objects.isNull( metricPool ) )
        {
            throw new MetricParameterException( "Cannot construct the metric collection without an executor service." );
        }
        if ( Objects.isNull( dataFactory ) )
        {
            throw new MetricParameterException( "Cannot construct the metric collection without a metric output "
                                                + "factory." );
        }
        if ( metrics.isEmpty() && collectableMetrics.isEmpty() )
        {
            throw new MetricParameterException( "Cannot construct a metric collection without any metrics." );
        }
    }

    /**
     * Logs the start of a calculation.
     */

    private void logStartOfCalculation()
    {
        if ( LOGGER.isDebugEnabled() )
        {
            // Determine the metrics to compute
            Set<MetricConstants> started = new TreeSet<>();
            Set<MetricConstants> collected = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collected.addAll( next.keySet() ) );
            started.addAll( metrics.keySet() );
            started.addAll( collected );

            LOGGER.debug( "Attempting to compute metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). The metrics include {}.",
                          metrics.size(),
                          collected.size(),
                          started );
        }
    }

    /**
     * Logs the end of a calculation.
     * 
     * @param results the results to log
     */

    private void logEndOfCalculation( Map<MetricConstants,U> results )
    {
        if ( LOGGER.isDebugEnabled() )
        {
            // Determine the metrics computed
            Set<MetricConstants> collected = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collected.addAll( next.keySet() ) );
            Set<MetricConstants> completed = results.keySet();

            LOGGER.debug( "Finished computing metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). Obtained {} result(s) of the {} result(s) expected. Results were "
                          + "obtained for these metrics {}.",
                          metrics.size(),
                          collected.size(),
                          results.size(),
                          metrics.size() + collected.size(),
                          completed );
        }
    }

}
