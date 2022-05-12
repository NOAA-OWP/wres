package wres.metrics;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.Statistic;

/**
 * Wraps a {@link Collectable} and a {@link Statistic} into a {@link Callable} task to compute a metric result from
 * the intermediate input (output).
 * 
 * @author James Brown
 */

class CollectableTask<S extends Pool<?>, T extends Statistic<?>, U extends Statistic<?>>
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
     * The pool.
     */
    
    private final S pool;
    
    /**
     * Construct a task with a {@link Collectable} metric and an intermediate {@link Statistic}. The {@link Statistic} 
     * is wrapped in a {@link Future}.
     * 
     * @param metric the collectable metric
     * @param statistic the metric input
     * @param pool the pool, optional
     */

    public CollectableTask( final Collectable<S, T, U> metric, final Future<T> statistic, S pool )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric from which to create a task." );
        Objects.requireNonNull( statistic, "Specify a non-null statistic from which to create a task." );
        this.metric = metric;
        this.input = statistic;
        this.pool = pool;
    }

    @Override
    public U call() throws InterruptedException, ExecutionException
    {
        final T in = this.input.get();
        if ( Objects.isNull( in ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }
        return metric.aggregate( in, this.pool );
    }

}
