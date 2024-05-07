package wres.pipeline.pooling;

import java.util.Objects;
import java.util.function.Predicate;

import wres.config.yaml.components.CovariateDataset;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.TimeSeriesUpscaler;

/**
 * A covariate dataset to use when filtering a pool.
 *
 * @param <T> the covariate data type
 * @param datasetDescription the dataset description, required
 * @param filter the filter to be applied to the covariate, required
 * @param desiredTimeScale the desired timescale or the evaluation, required
 * @param upscaler the upscaler used to upscale the covariate dataset, when required
 * @author James Brown
 */
record Covariate<T>( CovariateDataset datasetDescription,
                     Predicate<T> filter,
                     TimeScaleOuter desiredTimeScale,
                     TimeSeriesUpscaler<T> upscaler )
{
    /**
     * Creates an instance and validate the input.
     * @param datasetDescription the dataset description, required
     * @param filter the filter to be applied to the covariate, required
     * @param desiredTimeScale the desired timescale of the evaluation, optional
     * @param upscaler the upscaler used to upscale the covariate dataset, when required
     */
    Covariate
    {
        Objects.requireNonNull( datasetDescription );
        Objects.requireNonNull( filter );
    }
}
