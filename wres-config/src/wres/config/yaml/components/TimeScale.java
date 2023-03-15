package wres.config.yaml.components;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.deserializers.TimeScaleDeserializer;

/**
 * A timescale.
 * @param timeScale the timescale
 */
@RecordBuilder
@JsonDeserialize( using = TimeScaleDeserializer.class )
public record TimeScale( wres.statistics.generated.TimeScale timeScale )
{
    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return DeclarationFactory.PROTBUF_STRINGIFIER.apply( timeScale );
    }
}
