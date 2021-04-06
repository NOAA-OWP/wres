package wres.datamodel.pools;

import java.util.List;
import java.util.function.Supplier;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.time.TimeSeries;

/**
 * <p>An atomic collection of samples from which a statistic is computed using a metric. The samples may comprise paired 
 * or unpaired values. Optionally, it may contain a baseline dataset to be used in the same context (e.g. for skill 
 * scores) and a climatological dataset, which is used to derive quantiles from climatological probabilities.</p>
 * 
 * <p>Optionally, provides a time-series view of the pooled data.</p>.
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>A dataset may contain values that correspond to a missing value identifier. Some implementations may elect to 
 * not provide a time-series view of the data, in which case {@link #get()} should throw an 
 * {@link UnsupportedOperationException}. This may be useful when the time-indexing is not available or not needed.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface Pool<S> extends Supplier<List<TimeSeries<S>>>
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
     * Returns the raw data.
     * 
     * @return the raw data
     */

    List<S> getRawData();

    /**
     * Returns the metadata associated with the sample.
     * 
     * @return the metadata associated with the sample
     */

    PoolMetadata getMetadata();

    /**
     * Returns the baseline data as a {@link Pool} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    Pool<S> getBaselineData();

    /**
     * Returns a climatological dataset if {@link #hasClimatology()} returns true, otherwise null.
     * 
     * @return a climatological dataset or null
     */

    VectorOfDoubles getClimatology();

    /**
     * Returns a time-series view of the data or throws an {@link UnsupportedOperationException} if not implemented.
     * 
     * @throws UnsupportedOperationException if the implementation does not support a time-series view
     */

    List<TimeSeries<S>> get();

}
