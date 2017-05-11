/**
 * 
 */
package concurrency;

import java.util.List;
import java.util.concurrent.Callable;
import config.specification.MetricSpecification;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * @author Christopher Tubbs
 *
 */
public final class PairFetcher implements Callable<List<PairOfDoubleAndVectorOfDoubles>> {
    
    /**
     * 
     */
    public PairFetcher(MetricSpecification metricSpecification, int progress)
    {
        this.metricSpecification = metricSpecification;
        this.progress = progress;
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> call() throws Exception
    {
        return Metrics.getPairs(this.metricSpecification, this.progress);
    }

    private final MetricSpecification metricSpecification;
    private final int progress;
}
