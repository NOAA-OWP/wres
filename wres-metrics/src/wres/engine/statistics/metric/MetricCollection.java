package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;

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
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricCollection<S extends MetricInput<?>, T extends MetricOutput<?>>
implements Function<S, MetricOutputCollection<T>>, Callable<MetricOutputCollection<T>>
{

    /**
     * The collection of {@link Metric}
     */

    private final ArrayList<Metric<S, T>> metrics;

    /**
     * The metric input.
     */

    private final Future<S> input;

    /**
     * Executor service. By default, the {@link ForkJoinPool#commonPool()}
     */

    private final ExecutorService metricPool;

    @Override
    public MetricOutputCollection<T> call() throws MetricCalculationException, InterruptedException, ExecutionException
    {
        return callInternal();
    }

    @Override
    public MetricOutputCollection<T> apply(final S input)
    {
        return applyParallel(input);
    }

    /**
     * A builder to build the immutable collection.
     */

    public static class MetricCollectionBuilder<S extends MetricInput<?>, T extends MetricOutput<?>>
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
         * The list of {@link Metric}.
         */

        private final ArrayList<Metric<S, T>> builderMetrics = new ArrayList<>();

        /**
         * Returns a builder.
         * 
         * @param <P> the type of metric input
         * @param <Q> the type of metric output
         * @return a builder
         */

        public static <P extends MetricInput<?>, Q extends MetricOutput<?>> MetricCollectionBuilder<P, Q> of()
        {
            return new MetricCollectionBuilder<>();
        }

        /**
         * Add a {@link Metric} to the collection.
         * 
         * @param metric the metric
         * @return the builder
         */

        public MetricCollectionBuilder<S, T> add(final Metric<S, T> metric)
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

        public MetricCollectionBuilder<S, T> setMetricInput(final Future<S> input)
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

        public MetricCollectionBuilder<S, T> setExecutorService(final ExecutorService metricPool)
        {
            Objects.requireNonNull(metricPool, "Add a non-null executor service to the collection.");
            this.metricPool = metricPool;
            return this;
        }

        /**
         * Build the metric collection.
         * 
         * @return the metric
         */

        public MetricCollection<S, T> build()
        {
            return new MetricCollection<>(this);
        }

    }

    /**
     * Default method for computing the metric results and returning them in a collection with a prescribed type.
     * Collects instances of {@link Collectable} by {@link Collectable#getCollectionOf()} and computes their common
     * (intermediate) input once. Computes the metrics in parallel using the supplied {@link #metricPool} or, where
     * not supplied, the {@link ForkJoinPool#commonPool()}
     * 
     * @param s the metric input
     * @return the output for each metric, contained in a collection
     */

    private MetricOutputCollection<T> applyParallel(final S s) throws MetricCalculationException
    {
        if(!Objects.isNull(this.input))
        {
            throw new MetricCalculationException("The collection has already been constructed with a fixed input.");
        }

        //Collection of future metric results
        final List<CompletableFuture<T>> metricFutures = new ArrayList<>(metrics.size());

        //Collect the instances of Collectable by their getCollectionOf string, which denotes the superclass that
        //provides the intermediate result for all metrics of that superclass
        @SuppressWarnings("unchecked")
        final Map<String, List<Collectable<S, MetricOutput<?>, T>>> collectable =
                                                                                metrics.stream()
                                                                                       .filter(Collectable.class::isInstance)
                                                                                       .map(p -> (Collectable<S, MetricOutput<?>, T>)p)
                                                                                       .collect(Collectors.groupingBy(Collectable::getCollectionOf));
        //Consumer that computes the intermediate output once and applies it to all grouped instances of Collectable
        final Consumer<List<Collectable<S, MetricOutput<?>, T>>> c = x -> {
            //Compute a future of the dependent (intermediate) result
            final CompletableFuture<MetricOutput<?>> base = CompletableFuture.supplyAsync(
                                                                                          () -> x.get(0)
                                                                                                 .getCollectionInput(s),
                                                                                          metricPool);
            //Using the future dependent result, compute a future of each of the independent results
            x.forEach(y -> metricFutures.add(metrics.indexOf(y), base.thenApplyAsync(y::apply)));
        };

        //Compute the collectable metrics    
        collectable.values().forEach(c);

        //Pool all the future results from the non-collectable metrics into the collection
        //When the collection completes, aggregate the results and return them
        metrics.stream()
               .filter(p -> !(p instanceof Collectable)) //Only work with non-collectable metrics here
               .forEach(y -> metricFutures.add(metrics.indexOf(y),
                                               CompletableFuture.supplyAsync(() -> y.apply(s), metricPool)));
        final CompletableFuture<List<T>> results = sequence(metricFutures);
        return MetricOutputFactory.ofCollection(results.join()); //This is blocking
    }

    /**
     * Default method for computing the metric results and returning them in a collection with a prescribed type.
     * Collects instances of {@link Collectable} by {@link Collectable#getCollectionOf()} and computes their common
     * (intermediate) input once. Each metric is computed asynchronously.
     * 
     * @return the output for each metric, contained in a collection
     * @throws InterruptedException
     */

    private MetricOutputCollection<T> callInternal() throws MetricCalculationException,
                                                     InterruptedException,
                                                     ExecutionException
    {
        if(Objects.isNull(input))
        {
            throw new MetricCalculationException("No metric input from which to compute the metric collection: "
                + "construct the metric collection with an input.");
        }

        final MetricOutputCollection<T> m = new MetricOutputCollection<>(metrics.size());
        //Collect the instances of Collectable by their getCollectionOf string, which denotes the superclass that
        //provides the intermediate result for all metrics of that superclass
        @SuppressWarnings("unchecked")
        final Map<String, List<Collectable<S, MetricOutput<?>, T>>> collectable =
                                                                                metrics.stream()
                                                                                       .filter(Collectable.class::isInstance)
                                                                                       .map(p -> (Collectable<S, MetricOutput<?>, T>)p)
                                                                                       .collect(Collectors.groupingBy(Collectable::getCollectionOf));
        //For each group of Collectable, create a task       
        final ArrayList<Callable<T>> tasks = new ArrayList<>();
        collectable.forEach((a, b) -> {
            final Callable<MetricOutput<?>> intermediate = getCollectableTask(b.get(0));
            final Future<MetricOutput<?>> result = metricPool.submit(intermediate); //Compute once
            b.forEach(c -> tasks.add(metrics.indexOf(c), new CollectableTask<>(c, result)));
        });

        //For each non-Collectable metric, add a task
        metrics.stream()
               .filter(p -> !(p instanceof Collectable))
               .forEach(y -> tasks.add(metrics.indexOf(y), new MetricTask<>(y, input)));
        //Compute
        final List<Future<T>> results = metricPool.invokeAll(tasks);
        for(final Future<T> next: results)
        {
            m.add(next.get());
        }
        return m;
    }

    /**
     * Hidden constructor.
     */

    private MetricCollection(final MetricCollectionBuilder<S, T> builder)
    {
        if(builder.builderMetrics.isEmpty())
        {
            throw new UnsupportedOperationException("Cannot construct a metric collection with an empty list of "
                + "metrics.");
        }
        metrics = new ArrayList<>();
        metrics.addAll(builder.builderMetrics);
        input = builder.builderInput;
        if(Objects.isNull(builder.metricPool)) {
            metricPool = ForkJoinPool.commonPool();
        } else {
            metricPool = builder.metricPool;
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

    /**
     * Utility method that waits for the completion of a collection of futures and, once complete, returns them in a
     * future collection.
     * 
     * @param futures the list of futures
     * @return the completed list
     */

    private static <T> CompletableFuture<List<T>> sequence(final List<CompletableFuture<T>> futures)
    {
        final CompletableFuture<Void> allDoneFuture =
                                                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v -> futures.stream()
                                                   .map(CompletableFuture::join)
                                                   .collect(Collectors.<T>toList()));
    }

}
