package wres.config.yaml.components;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.yaml.DeclarationFactory;

/**
 * A threshold, optionally attached to a named feature.
 * @param threshold a threshold
 * @param featureName a named feature
 */
@RecordBuilder
public record Threshold( wres.statistics.generated.Threshold threshold, String featureName )
{
    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        String thresholdString = DeclarationFactory.PROTBUF_STRINGIFIER.apply( threshold );

        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "threshold", thresholdString )
                .append( "featureName", featureName )
                .build();
    }
}
