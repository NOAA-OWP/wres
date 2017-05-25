/**
 * 
 */
package concurrency;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import config.specification.MetricSpecification;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricStepExecutor extends WRESThread implements Callable<Double>
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
            result = Metrics.call(specification, step);
        }
        else if (Metrics.hasFunction(specification.getMetricType()))
        {
            result = Metrics.call(specification.getMetricType(), Metrics.getPairs(specification, step));
        }
        else
        {
            LOGGER.debug("The function: {} is not a valid function. Returning null...",
                         specification);
        }

        this.exectureOnComplete();
        return result;
    }

    private final MetricSpecification specification;
    private final int step;
}
