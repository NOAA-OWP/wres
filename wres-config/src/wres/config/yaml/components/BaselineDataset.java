package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.BaselineDatasetDeserializer;
import wres.config.yaml.serializers.TrueSerializer;

/**
 * The baseline data.
 * @param dataset the dataset
 * @param persistence the order of persistence for a persistence baseline
 * @param separateMetrics whether to compute separate metrics for the baseline
 */
@RecordBuilder
@JsonDeserialize( using = BaselineDatasetDeserializer.class )
// Dataset first, everything else afterwards in order of declaration
@JsonPropertyOrder({ "dataset"})
public record BaselineDataset( @JsonUnwrapped   // Use unwrap annotation to serialize everything on the same level
                               Dataset dataset,
                               @JsonUnwrapped
                               @JsonProperty( "persistence" ) Integer persistence,
                               @JsonUnwrapped
                               // Only write the non-default/true value of "separate_metrics"
                               @JsonSerialize( using = TrueSerializer.class )
                               @JsonProperty( "separate_metrics" ) Boolean separateMetrics )
{
    /**
     * Creates an instance.
     * @param dataset the dataset, required
     * @param persistence the order of persistence, optional
     * @param separateMetrics whether to compute separate metrics for the baseline
     */
    public BaselineDataset
    {
        Objects.requireNonNull( dataset, "The baseline dataset cannot be null." );

        if ( Objects.isNull( separateMetrics ) )
        {
            separateMetrics = false;
        }
    }
}
