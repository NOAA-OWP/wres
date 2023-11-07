package wres.config.yaml.components;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.TimeScaleDeserializer;
import wres.config.yaml.serializers.TimeScaleSerializer;

/**
 * A timescale.
 * @param timeScale the timescale
 */
@RecordBuilder
@JsonSerialize( using = TimeScaleSerializer.class )
@JsonDeserialize( using = TimeScaleDeserializer.class )
public record TimeScale( wres.statistics.generated.TimeScale timeScale )
{
    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return DeclarationFactory.PROTBUF_STRINGIFIER.apply( this.timeScale );
    }
}
