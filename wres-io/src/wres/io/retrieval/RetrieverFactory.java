package wres.io.retrieval;

import java.util.function.Supplier;
import java.util.stream.Stream;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;

/**
 * <p>An API for the creation of project-relevant retrievers. The API supplies retrievers for each side of a pairing and 
 * for given pool boundaries, where applicable.
 *
 * <p>See also: {@link PoolsGenerator}. A {@link RetrieverFactory} is injected into a {@link PoolsGenerator} to supply
 * the data needed for pool creation.
 * 
 * <p>By design, the geospatial shape of a pool is not embedded within the retrieval factory. Thus, one pool may
 * contain one feature or many features or may involve the retrieval of grids, rather than features.
 * 
 * @author james.brown@hydrosolved.com
 * @param <L> the type of left data
 * @param <R> the type of right data
 */

interface RetrieverFactory<L, R>
{

    /**
     * Creates a retriever for all left-ish data, i.e., without any pool boundaries.
     * 
     * @return a retriever for left data
     * @throws DataAccessException if the retriever could not be created for any reason
     */

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever();

    /**
     * Creates a retriever for the left-ish data associated with a particular {@link TimeWindow}.
     * 
     * @param timeWindow the time window
     * @return a retriever for left data
     * @throws DataAccessException if the retriever could not be created for any reason
     */

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever( TimeWindow timeWindow );

    /**
     * Creates a retriever of right-ish data associated with a particular {@link TimeWindow}.
     * 
     * @param timeWindow the time window
     * @return a retriever for right data
     * @throws DataAccessException if the retriever could not be created for any reason
     */

    Supplier<Stream<TimeSeries<R>>> getRightRetriever( TimeWindow timeWindow );

    /**
     * Creates a retriever of right-ish data associated with a baseline for a particular {@link TimeWindow}.
     * 
     * @param timeWindow the time window
     * @return a retriever for baseline data
     * @throws DataAccessException if the retriever could not be created for any reason
     */

    Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( TimeWindow timeWindow );

}
