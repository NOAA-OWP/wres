package wres.config.components;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.CovariateDatasetDeserializer;
import wres.statistics.generated.TimeScale;

/**
 * A covariate dataset.
 * @param dataset the dataset
 * @param minimum the minimum value, optional
 * @param maximum the maximum value, optional
 * @param featureNameOrientation the orientation of the feature names used by the covariate dataset, optional
 * @param rescaleFunction the timescale function to use when it differs from the evaluation timescale function
 * @param purposes the purposes or applications of the covariate dataset, optional
 */
@RecordBuilder
@JsonDeserialize( using = CovariateDatasetDeserializer.class )
public record CovariateDataset( wres.config.components.Dataset dataset,
                                @JsonProperty( "minimum" ) Double minimum,
                                @JsonProperty( "maximum" ) Double maximum,
                                DatasetOrientation featureNameOrientation,
                                @JsonProperty( "rescale_function" ) TimeScale.TimeScaleFunction rescaleFunction,
                                @JsonProperty( "purpose" ) Set<CovariatePurpose> purposes )
{
    /**
     * Creates an instance.
     * @param dataset the dataset, required
     * @param minimum the minimum value, optional
     * @param maximum the maximum value, optional
     * @param featureNameOrientation the orientation of the feature names used by the covariate dataset, optional
     * @param rescaleFunction the timescale function to use when it differs from the evaluation timescale function
     * @param purposes the purposes or applications of the covariate dataset, optional
     */
    public CovariateDataset
    {
        Objects.requireNonNull( dataset, "The covariate dataset cannot be null." );

        if ( Objects.isNull( purposes ) )
        {
            purposes = Collections.emptySet();
        }
    }
}
