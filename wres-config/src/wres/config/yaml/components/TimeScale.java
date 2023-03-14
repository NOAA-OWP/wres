package wres.config.yaml.components;

import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.DeclarationFactory;

/**
 * A timescale.
 * @param timeScale the timescale
 */
@RecordBuilder
public record TimeScale( wres.statistics.generated.TimeScale timeScale )
{
    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        return DeclarationFactory.PROTBUF_STRINGIFIER.apply( timeScale );
    }
}
