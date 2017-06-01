package wres.io.concurrency;

import java.util.List;
import java.util.concurrent.Callable;
import wres.io.config.specification.MetricSpecification;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

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
        this.exectureOnComplete();
        return results;
    }

    private final MetricSpecification metricSpecification;
    private final int progress;
}
