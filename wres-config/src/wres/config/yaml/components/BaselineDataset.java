package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.BaselineDeserializer;
import wres.config.yaml.serializers.BaselineDatasetSerializer;

/**
 * The baseline data.
 * @param dataset the dataset
 * @param persistence the order of persistence for a persistence baseline
 */
@RecordBuilder
@JsonSerialize( using = BaselineDatasetSerializer.class )
@JsonDeserialize( using = BaselineDeserializer.class )
public record BaselineDataset( Dataset dataset,
                               @JsonProperty( "persistence" ) Integer persistence )
{
    /**
     * Creates an instance.
     * @param dataset the dataset, required
     * @param persistence the order of persistence, optional
     */
    public BaselineDataset
    {
        Objects.requireNonNull( dataset, "The baseline dataset cannot be null." );
    }
}
