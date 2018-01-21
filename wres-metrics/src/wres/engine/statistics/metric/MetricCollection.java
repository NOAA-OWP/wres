package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

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
 * 
 * @param <S> the input type
 * @param <T> the intermediate output type for {@link Collectable} metrics in this collection
 * @param <U> the output type
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricCollection<S extends MetricInput<?>, T extends MetricOutput<?>, U extends MetricOutput<?>>
        implements Function<S, MetricOutputMapByMetric<U>>
{

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
     * The metric input.
     */

    private final Future<S> input;

    /**
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    @Override
    public MetricOutputMapByMetric<U> apply( final S input )
    {
        return applyParallel( input );
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
         * The metric input.
         */

        private Future<S> input;

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

        protected MetricCollectionBuilder<S, T, U> add( final Metric<S, U> metric )
        {
            metrics.put( metric.getID(), metric );
            return this;
        }

        /**
         * Add a {@link Collectable} metric to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> add( final Collectable<S, T, U> metric )
        {
            collectableMetrics.put( metric.getID(), metric );
            return this;
        }

        /**
         * Sets the {@link MetricInput}.
         * 
         * @param input the metric input
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T, U> setMetricInput( final Future<S> input )
        {
            this.input = input;
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
     * @param s the metric input
     * @return the output for each metric, contained in a collection
     * @throws MetricCalculationException if the calculation fails for any reason, with the cause set
     */

    private MetricOutputMapByMetric<U> applyParallel( final S s ) throws MetricCalculationException
    {
        //Bounds checks
        if ( Objects.isNull( s ) )
        {
            throw new MetricCalculationException( "Specify non-null input to the metric collection." );
        }
        if ( !Objects.isNull( this.input ) )
        {
            throw new MetricCalculationException( "The collection has already been constructed with a fixed input: "
                                                  + "use call instead of apply to generate the metric results." );
        }

        //Collection of future metric results
        final Map<MetricConstants, CompletableFuture<U>> metricFutures = new EnumMap<>( MetricConstants.class );

        //Create the futures for the collectable metrics
        for ( Map<MetricConstants, Collectable<S, T, U>> next : collectableMetrics.values() )
        {
            Collectable<S, T, U> baseMetric = next.values().iterator().next();
            final CompletableFuture<T> baseFuture = CompletableFuture.supplyAsync(
                                                                                   () -> baseMetric.getCollectionInput( s ),
                                                                                   metricPool );
            //Using the future dependent result, compute a future of each of the independent results
            next.forEach( ( id, metric ) -> metricFutures.put( id,
                                                               baseFuture.thenApplyAsync( metric::aggregate,
                                                                                          metricPool ) ) );
        }
        //Create the futures for the ordinary metrics
        metrics.forEach( ( key, value ) -> metricFutures.put( key,
                                                              CompletableFuture.supplyAsync( () -> value.apply( s ),
                                                                                             metricPool ) ) );
        //Compute the results
        List<U> returnMe = new ArrayList<>();
        MetricConstants nextMetric = null;
        try
        {
            for ( Map.Entry<MetricConstants, CompletableFuture<U>> nextResult : metricFutures.entrySet() )
            {
                nextMetric = nextResult.getKey();
                returnMe.add( nextResult.getValue().get() ); //This is blocking
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
        return dataFactory.ofMap( returnMe );
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
        input = builder.input;
        dataFactory = builder.dataFactory;
        metricPool = builder.metricPool;
        metrics = new EnumMap<>( builder.metrics );
        collectableMetrics = new EnumMap<>( MetricConstants.class );
        //Set the collectable metrics
        builder.collectableMetrics.forEach( ( id, metric ) -> {
            if ( collectableMetrics.containsKey( metric.getCollectionOf() ) )
            {
                collectableMetrics.get( metric.getCollectionOf() ).put( id, metric );
            }
            else
            {
                Map<MetricConstants, Collectable<S, T, U>> addMe = new EnumMap<>( MetricConstants.class );
                addMe.put( id, metric );
                collectableMetrics.put( metric.getCollectionOf(), addMe );
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
        //No null metrics
        for ( Map.Entry<MetricConstants, Metric<S, U>> next : metrics.entrySet() )
        {
            if ( Objects.isNull( next.getValue() ) )
            {
                throw new MetricParameterException( "Cannot construct a metric collection with a null metric." );
            }
        }
        //No null collectable metrics
        for ( Map.Entry<MetricConstants, Map<MetricConstants, Collectable<S, T, U>>> next : collectableMetrics.entrySet() )
        {
            for ( Map.Entry<MetricConstants, Collectable<S, T, U>> nextMap : next.getValue().entrySet() )
            {
                if ( Objects.isNull( nextMap.getValue() ) )
                {
                    throw new MetricParameterException( "Cannot construct a metric collection with a null metric." );
                }
            }
        }
    }
}
