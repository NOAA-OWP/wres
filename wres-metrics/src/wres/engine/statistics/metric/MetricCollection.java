package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.Statistic;
import wres.engine.statistics.metric.categorical.ContingencyTable;

/**
 * <p>
 * An immutable collection of {@link Metric} that consume a common class of {@link SampleData} and return a common
 * class of {@link Statistic}. Multiple instances of the same metric are allowed (e.g. with different parameter
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

public class MetricCollection<S extends SampleData<?>, T extends Statistic<?>, U extends Statistic<?>>
        implements BiFunction<S, Set<MetricConstants>, ListOfStatistics<U>>
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
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    /**
     * Applies the input to the collection for all metrics in the collection.
     * 
     * @param input the input
     * @return the collection output
     * @throws MetricCalculationException if the calculation fails for any reason
     */

    public ListOfStatistics<U> apply( final S input )
    {
        return this.apply( input, Collections.emptySet() );
    }

    /**
     * Computes all metrics except the metrics in the ignore set.
     * 
     * @param input the input
     * @param ignoreTheseMetrics the set of metrics to ignore
     * @throws MetricCalculationException if the calculation fails for any reason
     */

    @Override
    public ListOfStatistics<U> apply( final S input, final Set<MetricConstants> ignoreTheseMetrics )
    {
        try
        {
            return this.applyInternal( input, ignoreTheseMetrics );
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
    
    @Override
    public String toString() 
    {
        Set<MetricConstants> m = this.getMetrics();
        
        return "The following metrics are in collection object '" + this.hashCode() + "': " + m;
    }

    /**
     * A builder to build the immutable collection.
     * 
     * @param <S> the input type
     * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
     * @param <U> the output type
     */

    protected static class MetricCollectionBuilder<S extends SampleData<?>, T extends Statistic<?>, U extends Statistic<?>>
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

        protected static <P extends SampleData<?>, Q extends Statistic<?>, R extends Statistic<?>>
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
     * @throws ExecutionException if the execution fails
     * @throws InterruptedException if the execution is cancelled
     * @throws MetricCalculationException if one or more of the inputs is invalid
     */

    private ListOfStatistics<U> applyInternal( final S input, final Set<MetricConstants> ignoreTheseMetrics )
            throws InterruptedException, ExecutionException
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

        // If all of the stored metrics are contained in the ignored metrics, throw an 
        // exception: cannot ignore all metrics
        if ( ignoreTheseMetrics.containsAll( this.getMetrics() ) )
        {
            throw new MetricCalculationException( "Cannot ignore all metrics in the store: specify some metrics "
                                                  + "to process. The store contains "
                                                  + this.getMetrics()
                                                  + " and the "
                                                  + "ignored metrics are "
                                                  + ignoreTheseMetrics
                                                  + "." );
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
        final List<CompletableFuture<U>> metricFutures = new ArrayList<>();

        //Create the futures for the collectable metrics
        for ( Map<MetricConstants, Collectable<S, T, U>> next : localCollectableMetrics.values() )
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
        localMetrics.forEach( ( key, value ) -> metricFutures.add( CompletableFuture.supplyAsync( () -> value.apply( input ),
                                                                                                  this.metricPool ) ) );
        //Compute the results
        List<U> unpacked = new ArrayList<>();

        this.logStartOfCalculation( LOGGER );

        for ( CompletableFuture<U> nextResult : metricFutures )
        {
            unpacked.add( nextResult.get() ); //This is blocking
        }
        
        ListOfStatistics<U> returnMe = ListOfStatistics.of( Collections.unmodifiableList( unpacked ) );

        this.logEndOfCalculation( LOGGER, returnMe );

        return returnMe;
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
        if ( metrics.isEmpty() && collectableMetrics.isEmpty() )
        {
            throw new MetricParameterException( "Cannot construct a metric collection without any metrics." );
        }
    }

    /**
     * Logs the start of a calculation.
     * 
     * @param logger the logger to use
     */

    private void logStartOfCalculation( Logger logger )
    {
        if ( logger.isTraceEnabled() )
        {
            // Determine the metrics to compute
            Set<MetricConstants> started = new TreeSet<>();
            Set<MetricConstants> collected = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collected.addAll( next.keySet() ) );
            started.addAll( metrics.keySet() );
            started.addAll( collected );

            logger.trace( "Attempting to compute metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). The metrics include {}.",
                          metrics.size(),
                          collected.size(),
                          started );
        }
    }

    /**
     * Logs the end of a calculation.
     * 
     * @param logger the logger to use
     * @param results the results to log
     */

    private void logEndOfCalculation( Logger logger, ListOfStatistics<U> results )
    {
        if ( logger.isTraceEnabled() )
        {
            // Determine the metrics computed
            Set<MetricConstants> collected = new TreeSet<>();
            collectableMetrics.values().forEach( next -> collected.addAll( next.keySet() ) );
            Set<MetricConstants> completed = Slicer.discover( results, meta -> meta.getMetadata().getMetricID() );

            logger.trace( "Finished computing metrics for a collection that contains {} ordinary metric(s) and {} "
                          + "collectable metric(s). Obtained {} result(s) of the {} result(s) expected. Results were "
                          + "obtained for these metrics {}.",
                          metrics.size(),
                          collected.size(),
                          completed.size(),
                          metrics.size() + collected.size(),
                          completed );
        }
    }

    /**
     * Helper that returns a set of all stored metrics.
     * 
     * @return all stored metrics
     */
    
    private Set<MetricConstants> getMetrics() 
    {
        return Stream.concat( this.collectableMetrics.keySet().stream(), this.metrics.keySet().stream() )
                     .collect( Collectors.toSet() );
    }
    
}
