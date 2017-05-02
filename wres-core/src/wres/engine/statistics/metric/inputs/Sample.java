package wres.engine.statistics.metric.inputs;

import gov.noaa.wres.datamodel.Dataset;

/**
 * Identifies a dataset that represents a statistical sample.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface Sample<U extends Dataset<?>>
{

    /**
     * Returns the sample size associated with the dataset. For a skill score, this comprises the sample size used in
     * the numerator, in case the denominator has a different sample size.
     * 
     * @return the sample size
     */

    U getSampleSize();

}
