package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.config.specification.MetricSpecification;

import java.util.List;

/**
 * @author Christopher Tubbs
 *
 */
public final class PairFetcher extends WRESCallable<List<PairOfDoubleAndVectorOfDoubles>> {

    private static Logger LOGGER = LoggerFactory.getLogger(PairFetcher.class);

    /**
     * 
     */
    public PairFetcher(MetricSpecification metricSpecification, int progress)
    {
        this.metricSpecification = metricSpecification;
        this.progress = progress;
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> execute() throws Exception
    {
        List<PairOfDoubleAndVectorOfDoubles> results = Metrics.getPairs(this.metricSpecification, this.progress);
        return results;
    }

    private final MetricSpecification metricSpecification;
    private final int progress;

    @Override
    protected String getTaskName () {
        return "PairFetcher: Step " + String.valueOf(this.progress) + " for " + this.metricSpecification.getName();
    }

    @Override
    protected Logger getLogger () {
        return PairFetcher.LOGGER;
    }
}
