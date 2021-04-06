package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.statistics.Statistic;

/**
 * Wraps a {@link Collectable} and a {@link Statistic} into a {@link Callable} task to compute a metric result from
 * the intermediate input (output).
 * 
 * @author james.brown@hydrosolved.com
 */

class CollectableTask<S extends SampleData<?>, T extends Statistic<?>, U extends Statistic<?>>
implements Callable<U>
{

    /**
     * The intermediate input
     */
    private final Collectable<S, T, U> metric;

    /**
     * The metric input (also a metric output).
     */

    private final Future<T> input;

    /**
     * Construct a task with a {@link Collectable} metric and a {@link Statistic} that represents the intermediate
     * input to the {@link Collectable} metric. The {@link Statistic} is wrapped in a {@link Future}.
     * 
     * @param metric the collectable metric
     * @param input the metric input
     */

    public CollectableTask(final Collectable<S, T, U> metric, final Future<T> input)
    {
        Objects.requireNonNull(metric, "Specify a non-null metric from which to create a task.");
        Objects.requireNonNull(input, "Specify a non-null input from which to create a task.");
        this.metric = metric;
        this.input = input;
    }

    @Override
    public U call() throws InterruptedException, ExecutionException
    {
        final T in = input.get();
        if(Objects.isNull(in))
        {
            throw new SampleDataException("Specify non-null input to the '"+this+"'.");
        }
        return metric.aggregate(in);
    }

}
