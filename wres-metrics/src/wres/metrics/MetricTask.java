package wres.metrics;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.Statistic;

/**
 * Wraps a {@link Metric} and a {@link Pool} into a {@link Callable} task. The {@link Pool} is itself
 * wrapped in a {@link Future}.
 * 
 * @author James Brown
 */

class MetricTask<S extends Pool<?>, T extends Statistic<?>> implements Callable<T>
{

    /**
     * The metric.
     */
    private final Metric<S, T> metric;

    /**
     * The metric input.
     */
    private final Future<S> input;

    /**
     * Construct a task with a {@link Metric} and a {@link Pool} wrapped in a {@link Future}.
     * 
     * @param metric the metric
     * @param input the metric input
     */

    public MetricTask(final Metric<S, T> metric, final Future<S> input)
    {
        Objects.requireNonNull(metric, "Specify a non-null metric from which to create a task.");
        Objects.requireNonNull(input, "Specify a non-null input from which to create a task.");
        this.metric = metric;
        this.input = input;
    }

    @Override
    public T call() throws InterruptedException, ExecutionException
    {
        final S in = input.get();
        if(Objects.isNull(in))
        {
            throw new MetricCalculationException("Cannot compute a metric with null input.");
        }
        return metric.apply(in);
    }

}
