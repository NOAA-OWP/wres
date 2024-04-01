package wres.io.retrieving;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;

/**
 * <p>An API that creates retrievers for each side of a pairing and for given pool boundaries, where applicable.
 *
 * <p>A {@link RetrieverFactory} is injected into an evaluation pipeline to supply the data needed for pool creation.
 *
 * @author James Brown
 * @param <L> the type of left data
 * @param <R> the type of right data
 * @param <B> the type of baseline data
 */

public interface RetrieverFactory<L, R, B>
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

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features );

    /**
     * Creates a retriever for the left-ish data associated with a particular {@link TimeWindowOuter}.
     *
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for left data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if any input is null
     */

    Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<Feature> features, TimeWindowOuter timeWindow );

    /**
     * Creates a retriever of right-ish data associated with a particular {@link TimeWindowOuter}.
     *
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for right data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if any input is null
     */

    Supplier<Stream<TimeSeries<R>>> getRightRetriever( Set<Feature> features, TimeWindowOuter timeWindow );

    /**
     * Creates a retriever for all baseline-ish data without any pool boundaries.
     *
     * @param features the spatial features
     * @return a retriever for baseline data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<B>>> getBaselineRetriever( Set<Feature> features );

    /**
     * Creates a retriever of right-ish data associated with a baseline for a particular {@link TimeWindowOuter}.
     *
     * @param features the spatial features
     * @param timeWindow the time window
     * @return a retriever for baseline data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if any input is null
     */

    Supplier<Stream<TimeSeries<B>>> getBaselineRetriever( Set<Feature> features, TimeWindowOuter timeWindow );

    /**
     * Creates a retriever for all climatological data.
     *
     * @param features the spatial features
     * @return a retriever for climatological data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if the set of features is null
     */

    Supplier<Stream<TimeSeries<L>>> getClimatologyRetriever( Set<Feature> features );

    /**
     * Creates a retriever for all covariate time-series with a prescribed variable name and without any pool
     * boundaries.
     *
     * @param features the spatial features
     * @param variableName the variable name
     * @return a retriever for covariate data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if any input is null
     */

    Supplier<Stream<TimeSeries<L>>> getCovariateRetriever( Set<Feature> features, String variableName );

    /**
     * Creates a retriever for the covariate time-series with prescribed variable name and {@link TimeWindowOuter}.
     *
     * @param features the spatial features
     * @param variableName the variable name
     * @param timeWindow the time window
     * @return a retriever for covariate data
     * @throws DataAccessException if the retriever could not be created for any reason
     * @throws IllegalArgumentException if the set of features is empty
     * @throws NullPointerException if any input is null
     */

    Supplier<Stream<TimeSeries<L>>> getCovariateRetriever( Set<Feature> features,
                                                           String variableName,
                                                           TimeWindowOuter timeWindow );

}
