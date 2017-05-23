package wres.datamodel.metric;

/**
 * Identifies a dataset that represents a statistical sample.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface Sample
{
    /**
     * Returns the sample size associated with the dataset. For a skill score, this comprises the sample size used in
     * the numerator, in case the denominator has a different sample size.
     * 
     * @return the sample size
     */
    int getSampleSize();
}
