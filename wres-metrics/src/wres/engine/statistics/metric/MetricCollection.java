package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.Statistic;
import wres.engine.statistics.metric.categorical.ContingencyTable;

/**
 * <p>
 * An immutable collection of {@link Metric} that consume a common class of {@link Pool} and return a common
 * class of {@link Statistic}. Multiple instances of the same metric are allowed (e.g. with different parameter
 * values).
 * </p>
 * <p>
 * For metrics that implement {@link Collectable} and whose method {@link Collectable#getCollectionOf()} returns a
 * common superclass (by {@link Metric#getMetricName()}), the intermediate output is computed once and applied to all 
 * subclasses within the collection. For example, if the {@link MetricCollection} contains several {@link Score} that 
 * extend {@link ContingencyTable} and implement {@link Collectable}, the contingency table will be computed once, 
 * with all dependent scores using this result.
 * </p>
 * <p>
 * Build a collection with a {@link Builder#of()}.
 * </p>
 * <p>
 * When a group contains a collection of metrics that do not need to be computed for all inputs, a non-empty set of
 * {@link MetricConstants} may be defined. These metrics are ignored during calculation.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @param <S> the input type
 * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
 * @param <U> the output type
 */

public class MetricCollection<S extends Pool<?>, T extends Statistic<?>, U extends Statistic<?>>
        implements Function<S, List<U>>
{

    /**
     * Logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger( MetricCollection.class );

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
     * All metrics in the collection.
     */
    
    private final Set<MetricConstants> collected;
    
    /**
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    /**
     * Computes all metrics.
     * 
     * @param input the input
     * @return statistics the statistics
     * @throws NullPointerException if the input is null
     * @throws MetricCalculationException if the calculation fails for any other reason
     */

    @Override
    public List<U> apply( S input )
    {
        Objects.requireNonNull( input, "Specify non-null input to the metric collection." );

        return this.apply( input, this.metrics, this.collectableMetrics );
    }

    /**
     * Computes a subset of metrics.
     * 
     * @param input the input
     * @param metrics the metrics
     * @return the statistics
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the subset is invalid
     * @throws MetricCalculationException if the calculation fails for any other reason
     */

    public List<U> apply( S input, Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( input, "Specify non-null input to the metric collection." );
        Objects.requireNonNull( input, "Specify some metrics to calculate." );

        // None match?
        if( this.collected.stream().noneMatch( metrics::contains ) )
        {
            throw new IllegalArgumentException( "This metric collection did not contain any of " + metrics + "." );
        }
        
        // Filtered metrics
        Map<MetricConstants, Metric<S, U>> filtered =
                this.metrics.entrySet()
                            .stream()
                            .filter( next -> metrics.contains( next.getKey() ) )
                            .collect( Collectors.toMap( Entry::getKey, Entry::getValue ) );

        // Filtered collectable metrics
        Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> cFiltered =
                new EnumMap<>( MetricConstants.class );

        for ( Entry<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> next : this.collectableMetrics.entrySet() )
        {
            MetricConstants aggregator = next.getKey();
            Map<MetricConstants, Collectable<S, T, U>> aggregated = next.getValue();

            Map<MetricConstants, Collectable<S, T, U>> fAggregated =
                    aggregated.entrySet()
                              .stream()
                              .filter( nextEntry -> metrics.contains( nextEntry.getKey() ) )
                              .collect( Collectors.toMap( Entry::getKey, Entry::getValue ) );

            if ( !fAggregated.isEmpty() )
            {
                cFiltered.put( aggregator, fAggregated );
            }
        }

        return this.apply( input, filtered, cFiltered );
    }

    /**
     * @return the metrics in the collection.
     */
    
    public Set<MetricConstants> getMetrics()
    {
        return this.collected;
    }
    
    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( " " );

        joiner.add( "The following metrics are in collection object 'MetricCollection@" + this.hashCode() + "': " );
        joiner.add( "Ordinary metrics: " + this.metrics.keySet() + "; and " );
        joiner.add( "Collectable metrics by parent: "
                    + this.collectableMetrics.entrySet()
                                             .stream()
                                             .collect( Collectors.toMap( Entry::getKey,
                                                                         b -> b.getValue().keySet() ) ) );
        return joiner.toString();
    }

    /**
     * A builder to build the immutable collection.
     * 
     * @param <S> the input type
     * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
     * @param <U> the output type
     */

    protected static class Builder<S extends Pool<?>, T extends Statistic<?>, U extends Statistic<?>>
    {

        /**
         * Executor service. By default, uses {@link ForkJoinPool#commonPool()}.
         */

        private ExecutorService metricPool;

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

        protected static <P extends Pool<?>, Q extends Statistic<?>, R extends Statistic<?>>
                Builder<P, Q, R> of()
        {
            return new Builder<>();
        }

        /**
         * Add a {@link Metric} to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected Builder<S, T, U> addMetric( final Metric<S, U> metric )
        {
            this.metrics.put( metric.getMetricName(), metric );
            return this;
        }

        /**
         * Add a {@link Collectable} metric to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected Builder<S, T, U> addCollectableMetric( final Collectable<S, T, U> metric )
        {
            this.collectableMetrics.put( metric.getMetricName(), metric );
            return this;
        }

        /**
         * Sets the {@link ExecutorService} for parallel computations.
         * 
         * @param metricPool the executor service
         * @return the builder
         */

        protected Builder<S, T, U> setExecutorService( final ExecutorService metricPool )
        {
            this.metricPool = metricPool;
            return this;
        }

        /**
         * Build the metric collection.
         * 
         * @return the metric
         * @throws MetricParameterException if one or more parameters is invalid
         */

        protected MetricCollection<S, T, U> build()
        {
            return new MetricCollection<>( this );
        }

    }

    /**
     * Computes the results for the prescribed metrics.
     * 
     * @param input the metric input
     * @param metrics the metrics to compute
     * @param collectableMetrics the collectable metrics to compute
     * @return the output for each metric, contained in a collection
     * @throws MetricCalculationException if the metric calculation fails for any reason
     */

    private List<U> apply( S input,
                           Map<MetricConstants, Metric<S, U>> metrics,
                           Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> collectableMetrics )
    {
        try
        {
            return this.applyInternal( input, metrics, collectableMetrics );
        }
        catch ( ExecutionException e )
        {
            throw new MetricCalculationException( "Computation of the metric collection failed: ", e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new MetricCalculationException( "Computation of the metric collection was cancelled: ", e );
        }
    }

    /**
     * Computes the results for the prescribed metrics.
     * 
     * @param input the metric input
     * @param metrics the metrics to compute
     * @param collectableMetrics the collectable metrics to compute
     * @return the output for each metric, contained in a collection
     * @throws ExecutionException if the execution fails
     * @throws InterruptedException if the execution is cancelled
     * @throws MetricCalculationException if one or more of the inputs is invalid
     */

    private List<U> applyInternal( S input,
                                   Map<MetricConstants, Metric<S, U>> metrics,
                                   Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> collectableMetrics )
            throws InterruptedException, ExecutionException
    {

        //Collection of future metric results
        final List<CompletableFuture<U>> metricFutures = new ArrayList<>();

        //Create the futures for the collectable metrics
        for ( Map<MetricConstants, Collectable<S, T, U>> next : collectableMetrics.values() )
        {
            // Proceed
            if ( !next.isEmpty() )
            {
                Iterator<Collectable<S, T, U>> iterator = next.values().iterator();

                Collectable<S, T, U> baseMetric = iterator.next();
                final CompletableFuture<T> baseFuture =
                        CompletableFuture.supplyAsync( () -> baseMetric.getInputForAggregation( input ),
                                                       this.metricPool );
                //Using the future dependent result, compute a future of each of the independent results
                next.forEach( ( id, metric ) -> metricFutures.add( baseFuture.thenApplyAsync( metric::aggregate,
                                                                                              this.metricPool ) ) );
            }
        }
        //Create the futures for the ordinary metrics
        metrics.forEach( ( key,
                           value ) -> metricFutures.add( CompletableFuture.supplyAsync( () -> value.apply( input ),
                                                                                        this.metricPool ) ) );
        //Compute the results
        List<U> unpacked = new ArrayList<>();

        this.logStartOfCalculation( LOGGER, metrics, collectableMetrics );

        for ( CompletableFuture<U> nextResult : metricFutures )
        {
            unpacked.add( nextResult.get() ); //This is blocking
        }

        List<U> returnMe = Collections.unmodifiableList( unpacked );

        this.logEndOfCalculation( LOGGER, metrics, collectableMetrics, returnMe );

        return returnMe;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private MetricCollection( final Builder<S, T, U> builder )
    {

        //Set 
        this.metricPool = builder.metricPool;

        Map<MetricConstants, Metric<S, U>> localMetrics = new EnumMap<>( builder.metrics );
        Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> localCollectableMetrics =
                new EnumMap<>( MetricConstants.class );

        Set<MetricConstants> localCollected = new HashSet<>( localMetrics.keySet() );
        
        //Set the collectable metrics
        builder.collectableMetrics.forEach( ( id, metric ) -> {

            MetricConstants parent = metric.getCollectionOf();

            if ( localCollectableMetrics.containsKey( parent ) )
            {
                localCollectableMetrics.get( parent ).put( id, metric );
            }
            else
            {
                Map<MetricConstants, Collectable<S, T, U>> addMe = new EnumMap<>( MetricConstants.class );
                addMe.put( id, metric );
                localCollectableMetrics.put( parent, addMe );
            }
            
            localCollected.add( id );
        } );

        this.metrics = Collections.unmodifiableMap( localMetrics );
        this.collectableMetrics = Collections.unmodifiableMap( localCollectableMetrics );
        this.collected = Collections.unmodifiableSet( localCollected );
        
        this.validate();
    }

    /**
     * Validates the copied collection parameters on construction.
     * 
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private void validate()
    {
        //Validate 
        if ( Objects.isNull( this.metricPool ) )
        {
            throw new MetricParameterException( "Cannot construct the metric collection without an executor service." );
        }
        if ( this.metrics.isEmpty() && this.collectableMetrics.isEmpty() )
        {
            throw new MetricParameterException( "Cannot construct a metric collection without any metrics." );
        }
    }

    /**
     * Logs the start of a calculation.
     * 
     * @param logger the logger to use
     * @param metrics the metrics
     * @param collectableMetrics the collectable metrics
     */

    private void logStartOfCalculation( Logger logger,
                                        Map<MetricConstants, Metric<S, U>> metrics,
                                        Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> collectableMetrics )
    {
        if ( logger.isTraceEnabled() )
        {
            // Determine the metrics to compute
            Set<MetricConstants> started = new TreeSet<>();
            Set<MetricConstants> collect = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collect.addAll( next.keySet() ) );
            started.addAll( metrics.keySet() );
            started.addAll( collect );

            logger.trace( "Attempting to compute metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). The metrics include {}.",
                          metrics.size(),
                          collect.size(),
                          started );
        }
    }

    /**
     * Logs the end of a calculation.
     * 
     * @param logger the logger to use
     * @param metrics the metrics
     * @param collectableMetrics the collectable metrics
     * @param results the results to log
     */

    private void logEndOfCalculation( Logger logger,
                                      Map<MetricConstants, Metric<S, U>> metrics,
                                      Map<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> collectableMetrics,
                                      List<U> results )
    {
        if ( logger.isTraceEnabled() )
        {
            // Determine the metrics computed
            Set<MetricConstants> collect = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collect.addAll( next.keySet() ) );
            Set<MetricConstants> completed = Slicer.discover( results, U::getMetricName );

            logger.trace( "Finished computing metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). Obtained {} result(s) of the {} result(s) expected. Results were "
                          + "obtained for these metrics {}.",
                          metrics.size(),
                          collect.size(),
                          completed.size(),
                          metrics.size() + collect.size(),
                          completed );
        }
    }

}
