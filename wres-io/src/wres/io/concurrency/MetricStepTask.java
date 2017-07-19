package wres.io.concurrency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.config.specification.MetricSpecification;
import wres.io.utilities.Database;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricStepTask extends WRESCallable<Double>
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
    public Double execute() throws Exception
    {
        Double result = null;
        
        if (specification.shouldProcessDirectly() && Metrics.hasDirectFunction(specification.getMetricType()))
        {
            this.getLogger().debug("using first option");
            result = Database.submit(() -> {
                    return Metrics.call(specification, step);
                }).get();
            this.getLogger().debug("result is {}", result);
        }
        else if (Metrics.hasFunction(specification.getMetricType()))
        {
            this.getLogger().debug("using second option");
            result = Database.submit(()-> {
                    return Metrics.call(specification.getMetricType(), Metrics.getPairs(specification, step));
            }).get();
            this.getLogger().debug("result is {}", result);
        }
        else
        {
            this.getLogger().debug("The function: {} is not a valid function. Returning null...", specification);
        }

        return result;
    }

    private final MetricSpecification specification;
    private final int step;

    @Override
    protected String getTaskName () {
        return "MetricStepTask: Step " + String.valueOf(this.step) + " for " + this.specification.getName();
    }

    @Override
    protected Logger getLogger () {
        return MetricStepTask.LOGGER;
    }
}
