package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputMapByMetric;

/**
 * <p>
 * An immutable collection of {@link Metric} that consume a common class of {@link MetricInput} and return a common
 * class of {@link MetricOutput}. Multiple instances of the same metric are allowed (e.g. with different parameter
 * values).
 * </p>
 * <p>
 * For metrics that implement {@link Collectable} and whose method {@link Collectable#getCollectionOf()} returns a
 * common superclass (by name), the intermediate output is computed once and applied to all subclasses within the
 * collection. For example, if the {@link MetricCollection} contains several {@link Score} that extend
 * {@link ContingencyTable} and implement {@link Collectable}, the contingency table will be computed once, with all
 * dependent scores using this result.
 * </p>
 * <p>
 * Build a collection with a {@link MetricCollectionBuilder#of()}.
 * </p>
 * <p>
 * In order to compute the metrics within a collection asynchronously, the {@link MetricCollection} implements
 * {@link Callable}. In order for {@link Callable#call()} to complete successfully, the {@link MetricCollection} must be
 * constructed with an {@link ExecutorService} and a {@link MetricInput} wrapped in a {@link Future}.
 * </p>
 * <p>
 * A {@link MetricCollection} is not fail-fast. All exceptions thrown by the individual {@link Metric} are logged, but
 * all {@link Metric} will be computed and any successful results returned.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class MetricCollection<S extends MetricInput<?>, T extends MetricOutput<?>>
implements Function<S, MetricOutputMapByMetric<T>>, Callable<MetricOutputMapByMetric<T>>
{

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricCollection.class);    
    
    /**
     * Instance of a {@link DataFactory} for constructing a {@link MetricOutput}.
     */

    private final DataFactory dataFactory;

    /**
     * The collection of {@link Metric}
     */

    private final List<Metric<S, T>> metrics;

    /**
     * The metric input.
     */

    private final Future<S> input;

    /**
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    @Override
    public MetricOutputMapByMetric<T> call() throws InterruptedException
    {
        return callInternal();
    }

    @Override
    public MetricOutputMapByMetric<T> apply(final S input)
    {
        return applyParallel(input);
    }

    /**
     * A builder to build the immutable collection.
     */

    protected static class MetricCollectionBuilder<S extends MetricInput<?>, T extends MetricOutput<?>>
    {

        /**
         * The metric input.
         */

        private Future<S> builderInput;

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

        private final List<Metric<S, T>> builderMetrics = new ArrayList<>();

        /**
         * Returns a builder.
         * 
         * @param <P> the type of metric input
         * @param <Q> the type of metric output
         * @return a builder
         */

        protected static <P extends MetricInput<?>, Q extends MetricOutput<?>> MetricCollectionBuilder<P, Q> of()
        {
            return new MetricCollectionBuilder<>();
        }

        /**
         * Add a {@link Metric} to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T> add(final Metric<S, T> metric)
        {
            Objects.requireNonNull(metric, "Add a non-null metric to the collection.");
            builderMetrics.add(metric);
            return this;
        }

        /**
         * Sets the {@link MetricInput}.
         * 
         * @param input the metric input
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T> setMetricInput(final Future<S> input)
        {
            Objects.requireNonNull(input, "Add a non-null metric input to the collection.");
            builderInput = input;
            return this;
        }

        /**
         * Sets the {@link ExecutorService} for parallel computations.
         * 
         * @param metricPool the executor service
         * @return the builder
         */

        protected MetricCollectionBuilder<S, T> setExecutorService(final ExecutorService metricPool)
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

        protected MetricCollectionBuilder<S, T> setOutputFactory(final DataFactory dataFactory)
        {
            this.dataFactory = dataFactory;
            return this;
        }

        /**
         * Build the metric collection.
         * 
         * @return the metric
         */

        protected MetricCollection<S, T> build()
        {
            return new MetricCollection<>(this);
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
     */

    private MetricOutputMapByMetric<T> applyParallel(final S s) throws MetricCalculationException
    {
        //Bounds checks
        if(!Objects.isNull(this.input))
        {
            throw new MetricCalculationException("The collection has already been constructed with a fixed input.");
        }
        
        if(Objects.isNull(s))
        {
            throw new MetricCalculationException("Specify non-null input to the metric collection.");
        }

        //Collection of future metric results
        final List<CompletableFuture<T>> metricFutures = new ArrayList<>(metrics.size());

        //Collect the instances of Collectable by their getCollectionOf string, which denotes the superclass that
        //provides the intermediate result for all metrics of that superclass
        @SuppressWarnings("unchecked")
        final Map<MetricConstants, List<Collectable<S, MetricOutput<?>, T>>> collectable =
                                                                                         metrics.stream()
                                                                                                .filter(Collectable.class::isInstance)
                                                                                                .map(p -> (Collectable<S, MetricOutput<?>, T>)p)
                                                                                                .collect(Collectors.groupingBy(Collectable::getCollectionOf));
        //Consumer that computes the intermediate output once and applies it to all grouped instances of Collectable
        final Consumer<List<Collectable<S, MetricOutput<?>, T>>> c = x -> {
            final CompletableFuture<MetricOutput<?>> baseFuture = CompletableFuture.supplyAsync(
                                                                                                () -> x.get(0)
                                                                                                       .getCollectionInput(s),
                                                                                                metricPool);
            //Using the future dependent result, compute a future of each of the independent results
            x.forEach(y -> metricFutures.add(baseFuture.thenApplyAsync(y::apply)));
        };

        //Compute the collectable metrics    
        collectable.values().forEach(c);

        //Pool all the future results from the non-collectable metrics into the collection
        //When the collection completes, aggregate the results and return them
        metrics.stream()
               .filter(p -> !(p instanceof Collectable)) //Only work with non-collectable metrics here
               .forEach(y -> metricFutures.add(CompletableFuture.supplyAsync(() -> y.apply(s), metricPool)));
        List<T> returnMe = new ArrayList<>();
        //Throw one exception per collection
        Exception throwMe = null;
        for(CompletableFuture<T> nextResult: metricFutures)
        {
            try
            {
                returnMe.add(nextResult.get()); //This is blocking
            }
            catch(Exception e)
            {
                throwMe = e;
            }
        }
        if(Objects.nonNull( throwMe ))
        {
            LOGGER.error("While processing metric:", throwMe);
        }
        return dataFactory.ofMap(returnMe);
    }

    /**
     * Default method for computing the metric results and returning them in a collection with a prescribed type.
     * Collects instances of {@link Collectable} by {@link Collectable#getCollectionOf()} and computes their common
     * (intermediate) input once. Each metric is computed asynchronously.
     * 
     * @return the output for each metric, contained in a collection
     * @throws InterruptedException if the calculation is interrupted
     */

    private MetricOutputMapByMetric<T> callInternal() throws InterruptedException
    {
        if(Objects.isNull(input))
        {
            throw new MetricCalculationException("No metric input from which to compute the metric collection: "
                + "construct the metric collection with an input.");
        }
        //Collection
        final List<T> m = new ArrayList<>(metrics.size());
        //Collect the instances of Collectable by their getCollectionOf string, which denotes the superclass that
        //provides the intermediate result for all metrics of that superclass
        @SuppressWarnings("unchecked")
        final Map<MetricConstants, List<Collectable<S, MetricOutput<?>, T>>> collectable =
                                                                                         metrics.stream()
                                                                                                .filter(Collectable.class::isInstance)
                                                                                                .map(p -> (Collectable<S, MetricOutput<?>, T>)p)
                                                                                                .collect(Collectors.groupingBy(Collectable::getCollectionOf));
        //For each group of Collectable, create a task       
        final ArrayList<Callable<T>> tasks = new ArrayList<>();
        collectable.forEach((a, b) -> {
            final Callable<MetricOutput<?>> intermediate = getCollectableTask(b.get(0));
            final Future<MetricOutput<?>> result = metricPool.submit(intermediate); //Compute once
            b.forEach(c -> tasks.add(new CollectableTask<>(c, result)));
        });

        //For each non-Collectable metric, add a task
        metrics.stream().filter(p -> !(p instanceof Collectable)).forEach(y -> tasks.add(new MetricTask<>(y, input)));
        //Compute
        final List<Future<T>> results = metricPool.invokeAll(tasks);
        for(final Future<T> next: results)
        {
            try
            {
                m.add(next.get());
            }
            catch(Exception e)
            {
                LOGGER.error("While processing metric:", e);
            }
        }
        return dataFactory.ofMap(m);
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MetricCollection(final MetricCollectionBuilder<S, T> builder)
    {
        //Set 
        metrics = new ArrayList<>();
        metrics.addAll(builder.builderMetrics);
        input = builder.builderInput;
        dataFactory = builder.dataFactory;
        if(Objects.isNull(builder.metricPool))
        {
            metricPool = ForkJoinPool.commonPool();
        }
        else
        {
            metricPool = builder.metricPool;
        }
        //Validate 
        if(Objects.isNull(dataFactory))
        {
            throw new UnsupportedOperationException("Cannot construct the metric collection without a metric output "
                + "factory.");
        }
        if(metrics.isEmpty())
        {
            throw new UnsupportedOperationException("Cannot construct a metric collection with an empty list of "
                + "metrics.");
        }
    }

    /**
     * Returns a {@link Future} containing a {@link MetricOutput}, which is used to compute the intermediate output
     * associated with a {@link Collectable}.
     * 
     * @param metric a metric
     * @return a future metric output
     */

    private Callable<MetricOutput<?>> getCollectableTask(final Collectable<S, MetricOutput<?>, T> metric)
    {
        return new Callable<MetricOutput<?>>()
        {
            @Override
            public MetricOutput<?> call() throws Exception
            {
                return metric.getCollectionInput(input.get());
            }
        };
    }

}
