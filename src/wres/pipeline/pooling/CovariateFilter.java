package wres.pipeline.pooling;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.CovariateDataset;
import wres.datamodel.pools.Pool;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesUpscaler;

/**
 * Filters a pool against a covariate dataset.
 *
 * @author James Brown
 */
class CovariateFilter<L, R> implements Supplier<Pool<TimeSeries<Pair<L, R>>>>
{
    /** The pool to filter against a covariate. */
    private final Pool<TimeSeries<Pair<L, R>>> pool;

    /** The description of the covariate dataset. */
    private final CovariateDataset covariateDescription;

    /** The covariate data source. */
    private final Supplier<Stream<TimeSeries<L>>> covariateData;

    /** The desired timescale of the covariate. */
    private final TimeScaleOuter desiredTimeScale;

    /** Upscaler. */
    private final TimeSeriesUpscaler<L> upscaler;

    @Override
    public Pool<TimeSeries<Pair<L, R>>> get()
    {
        return this.pool;
    }

    /**
     * Creates an instance.
     * @param pool the pool, required
     * @param covariateDescription the covariate dataset description, required
     * @param covariateData the covariate data source, required
     * @param desiredTimeScale the desired timescale, optional
     * @param upscaler a time-series upscaler, optional unless rescaling is required
     * @throws NullPointerException if any required input is null
     */
    static <L, R> CovariateFilter<L, R> of( Pool<TimeSeries<Pair<L, R>>> pool,
                                            CovariateDataset covariateDescription,
                                            Supplier<Stream<TimeSeries<L>>> covariateData,
                                            TimeScaleOuter desiredTimeScale,
                                            TimeSeriesUpscaler<L> upscaler )
    {
        return new CovariateFilter<>( pool,
                                      covariateDescription,
                                      covariateData,
                                      desiredTimeScale,
                                      upscaler );
    }

    /**
     * Returns the name of the first feature in the pool that has the same feature authority as the covariate dataset,
     * defaulting to the feature authority associated with the
     * {@link wres.config.yaml.components.DatasetOrientation#LEFT} data.
     *
     * @param pool the pool
     * @return the feature
     */

    private Feature getFeatureName( Pool<TimeSeries<Pair<L, R>>> pool )
    {
        FeatureTuple featureTuple = pool.getMetadata()
                                        .getFeatureTuples()
                                        .iterator()
                                        .next();

        switch ( this.covariateDescription.featureNameOrientation() )
        {
            case LEFT ->
            {
                return featureTuple.getLeft();
            }
            case RIGHT ->
            {
                return featureTuple.getRight();
            }
            case BASELINE ->
            {
                return featureTuple.getBaseline();
            }
            default -> throw new IllegalStateException( "Unrecognized dataset orientation, '"
                                                        + this.covariateDescription.featureNameOrientation()
                                                        + "'." );
        }
    }

    /**
     * Creates an instance.
     * @param pool the pool
     * @param covariateDescription the covariate dataset description, required
     * @param covariateData the covariate data source, required
     * @param desiredTimeScale the desired timescale, optional
     * @param upscaler a time-series upscaler, optional unless rescaling is required
     * @throws NullPointerException if any required input is null
     */
    private CovariateFilter( Pool<TimeSeries<Pair<L, R>>> pool,
                             CovariateDataset covariateDescription,
                             Supplier<Stream<TimeSeries<L>>> covariateData,
                             TimeScaleOuter desiredTimeScale,
                             TimeSeriesUpscaler<L> upscaler )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( covariateDescription );
        Objects.requireNonNull( covariateData );
        Objects.requireNonNull( covariateDescription.featureNameOrientation() );

        this.covariateData = covariateData;
        this.covariateDescription = covariateDescription;
        this.pool = pool;
        this.desiredTimeScale = desiredTimeScale;
        this.upscaler = upscaler;
    }
}