package wres.io.concurrency;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.config.specification.MetricSpecification;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Christopher Tubbs
 *
 */
public final class PairFetcher extends WRESTask implements Callable<List<PairOfDoubleAndVectorOfDoubles>> {
    
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
        this.executeOnRun();
        List<PairOfDoubleAndVectorOfDoubles> results = Metrics.getPairs(this.metricSpecification, this.progress);
        this.executeOnComplete();
        return results;
    }

    private final MetricSpecification metricSpecification;
    private final int progress;

    @Override
    protected String getTaskName () {
        return "PairFetcher: Step " + String.valueOf(this.progress) + " for " + this.metricSpecification.getName();
    }
}
