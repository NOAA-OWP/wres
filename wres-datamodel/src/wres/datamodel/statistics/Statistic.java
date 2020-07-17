package wres.datamodel.statistics;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * <p>A {@link Statistic} is used to describe {@link SampleData} or to infer something about the population from which
 * it originates.</p>
 * 
 * @param <U> the type of statistic data
 * @author james.brown@hydrosolved.com
 */
public interface Statistic<U>
{

    /**
     * Returns the statistic.
     * 
     * @return the statistic
     */

    U getData();
    
    /**
     * Returns the sample metadata associated with the statistic.
     * 
     * @return the sample metadata associated with the statistic
     */

    SampleMetadata getMetadata();
        
    /**
     * Returns the name of the metric that produced the statistic.
     * 
     * @return the metric name
     */
    
    MetricConstants getMetricName();
}
