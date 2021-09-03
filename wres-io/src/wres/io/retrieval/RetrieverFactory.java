package wres.io.retrieval;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.pooling.PoolsGenerator;

/**
 * <p>An API for the creation of project-relevant retrievers. The API supplies retrievers for each side of a pairing and 
 * for given pool boundaries, where applicable.
 *
 * <p>See also: {@link PoolsGenerator}. A {@link RetrieverFactory} is injected into a {@link PoolsGenerator} to supply
 * the data needed for pool creation.
 * 
 * @author James Brown
 * @param <L> the type of left data
 * @param <R> the type of right and baseline data
 */

public interface RetrieverFactory<L, R>
{

    /**
     * Creates a retriever for all left-ish data without any pool boundaries.
     * 
     * @param features the spatial features
     * @return a retriever for left data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<FeatureKey> features );

    /**
     * Creates a retriever for all baseline-ish data without any pool boundaries.
     * 
     * @param features the spatial features
     * @return a retriever for baseline data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<FeatureKey> features );
    
    /**
     * Creates a retriever for the left-ish data associated with a particular {@link TimeWindowOuter}.
     * 
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for left data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<FeatureKey> features, TimeWindowOuter timeWindow );

    /**
     * Creates a retriever of right-ish data associated with a particular {@link TimeWindowOuter}.
     * 
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for right data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<R>>> getRightRetriever( Set<FeatureKey> features, TimeWindowOuter timeWindow );

    /**
     * Creates a retriever of right-ish data associated with a baseline for a particular {@link TimeWindowOuter}.
     * 
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for baseline data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<FeatureKey> features, TimeWindowOuter timeWindow );

}
