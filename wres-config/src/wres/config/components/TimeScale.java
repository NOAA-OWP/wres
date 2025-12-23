package wres.config.components;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.DeclarationFactory;
import wres.config.deserializers.TimeScaleDeserializer;
import wres.config.serializers.TimeScaleSerializer;

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
    @Nonnull
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return DeclarationFactory.PROTBUF_STRINGIFIER.apply( this.timeScale );
    }
}
