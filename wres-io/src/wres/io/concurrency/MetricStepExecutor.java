package wres.io.concurrency;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.specification.MetricSpecification;
import wres.io.utilities.Database;
import wres.io.utilities.Debug;

/**
 * @author Christopher Tubbs
 *
 */
class MetricStepExecutor extends WRESThread implements Callable<Double>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricStepExecutor.class);

    /**
     * 
     */
    public MetricStepExecutor(MetricSpecification specification, int step)
    {
        this.specification = specification;
        this.step = step;
    }

    @Override
    public Double call() throws Exception
    {
        Double result = null;
        this.executeOnRun();
        
        if (specification.shouldProcessDirectly() && Metrics.hasDirectFunction(specification.getMetricType()))
        {
            result = Database.submit(() -> {
                    return Metrics.call(specification, step);
                }).get();
            //result = Metrics.call(specification, step);
        }
        else if (Metrics.hasFunction(specification.getMetricType()))
        {
            result = Database.submit(()-> {
                    return Metrics.call(specification.getMetricType(), Metrics.getPairs(specification, step));
            }).get();
            //result = Metrics.call(specification.getMetricType(), Metrics.getPairs(specification, step));
        }
        else
        {
            Debug.debug(LOGGER, "The function: {} is not a valid function. Returning null...", specification);
        }

        this.executeOnComplete();
        return result;
    }

    private final MetricSpecification specification;
    private final int step;
}
