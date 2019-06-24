package wres.datamodel.statistics;

import wres.datamodel.sampledata.SampleData;

/**
 * <p>A {@link Statistic} is used to describe {@link SampleData} or to infer something about the population from which
 * it originates.</p>
 * 
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
     * Returns the metadata associated with the statistic.
     * 
     * @return the metadata associated with the statistic
     */

    StatisticMetadata getMetadata();
        
}
