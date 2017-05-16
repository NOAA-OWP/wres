/**
 * 
 */
package concurrency;

import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import config.specification.MetricSpecification;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricStepExecutor extends WRESThread implements Callable<Double>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyExecutor.class);

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

        if (Metrics.hasFunction(specification.getMetricType()))
        {
            List<PairOfDoubleAndVectorOfDoubles> pairs = Metrics.getPairs(specification, step);
            result = Metrics.call(specification.getMetricType(), pairs);
        }
        else
        {
            LOGGER.debug("The function: '" + specification + "' is not a valid function. Returning null...");
        }
        this.exectureOnComplete();
        return result;
    }

    private final MetricSpecification specification;
    private final int step;
}
