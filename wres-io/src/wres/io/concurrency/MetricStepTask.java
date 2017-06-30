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
public class MetricStepTask extends WRESTask implements Callable<Double>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricStepTask.class);

    /**
     * 
     */
    public MetricStepTask(MetricSpecification specification, int step)
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
            LOGGER.debug("using first option");
            result = Database.submit(() -> {
                    return Metrics.call(specification, step);
                }).get();
            LOGGER.debug("result is {}", result);
        }
        else if (Metrics.hasFunction(specification.getMetricType()))
        {
            LOGGER.debug("using second option");
            result = Database.submit(()-> {
                    return Metrics.call(specification.getMetricType(), Metrics.getPairs(specification, step));
            }).get();
            LOGGER.debug("result is {}", result);
        }
        else
        {
            LOGGER.debug("The function: {} is not a valid function. Returning null...", specification);
        }

        this.executeOnComplete();
        return result;
    }

    private final MetricSpecification specification;
    private final int step;
}
