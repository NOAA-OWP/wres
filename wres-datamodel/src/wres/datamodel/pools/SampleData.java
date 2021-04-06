package wres.datamodel.pools;

import java.util.List;

import wres.datamodel.VectorOfDoubles;

/**
 * <p>Sample data to be iterated over by a metric. A sample may comprise paired data or unpaired data.
 * Optionally, it may contain a baseline dataset to be used in the same context (e.g. for skill scores) and a 
 * climatological dataset, which is used to derive quantiles from climatological probabilities.</p>
 * 
 * <p>TODO: replace the nomenclature of "sample data" with "pool". See #89095.
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>A dataset may contain values that correspond to a missing value identifier.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface SampleData<S>
{

    /**
     * Returns <code>true</code> if the sample has a baseline for skill calculations, <code>false</code> otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    boolean hasBaseline();

    /**
     * Returns <code>true</code> if the sample has a climatological dataset associated with it, <code>false</code> 
     * otherwise.
     * 
     * @return true if a climatological dataset is defined, false otherwise
     */

    boolean hasClimatology();
    
    /**
     * Returns the raw sample.
     * 
     * @return the raw sample
     */

    List<S> getRawData();

    /**
     * Returns the metadata associated with the sample.
     * 
     * @return the metadata associated with the sample
     */

    SampleMetadata getMetadata();

    /**
     * Returns the baseline data as a {@link SampleData} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    SampleData<S> getBaselineData();

    /**
     * Returns a climatological dataset if {@link #hasClimatology()} returns true, otherwise null.
     * 
     * @return a climatological dataset or null
     */

    VectorOfDoubles getClimatology();

}
