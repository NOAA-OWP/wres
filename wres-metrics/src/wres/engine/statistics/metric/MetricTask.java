package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;

/**
 * Wraps a {@link Metric} and a {@link MetricInput} into a {@link Callable} task. The {@link MetricInput} is itself
 * wrapped in a {@link Future}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricTask<S extends MetricInput<?>, T extends MetricOutput<?>> implements Callable<T>
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
     * Construct a task with a {@link Metric} and a {@link MetricInput} wrapped in a {@link Future}.
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
    public T call() throws MetricCalculationException, InterruptedException, ExecutionException
    {
        final S in = input.get();
        if(Objects.isNull(in))
        {
            throw new MetricCalculationException("Cannot compute a metric with null input.");
        }
        return metric.apply(in);
    }

}
