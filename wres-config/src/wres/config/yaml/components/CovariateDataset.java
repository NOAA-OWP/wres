package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.CovariateDatasetDeserializer;

/**
 * A covariate dataset.
 * @param dataset the dataset
 * @param minimum the minimum value, optional
 * @param maximum the maximum value, optional
 * @param featureNameOrientation the orientation of the feature names used by the covariate dataset, optional
 */
@RecordBuilder
@JsonDeserialize( using = CovariateDatasetDeserializer.class )
public record CovariateDataset( Dataset dataset,
                                @JsonProperty( "minimum" ) Double minimum,
                                @JsonProperty( "maximum" ) Double maximum,
                                DatasetOrientation featureNameOrientation )
{
    /**
     * Creates an instance.
     * @param dataset the dataset, required
     * @param minimum the minimum value, optional
     * @param maximum the maximum value, optional
     */
    public CovariateDataset
    {
        Objects.requireNonNull( dataset, "The covariate dataset cannot be null." );
    }
}
