package wres.config.yaml.components;

import java.util.Objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.Geometry;

/**
 * A threshold, optionally attached to a named feature.
 * @param threshold a threshold
 * @param type the threshold type to help identify the declaration context
 * @param feature a feature
 * @param featureNameFrom the orientation of the data from which the named feature is taken
 */
@RecordBuilder
public record Threshold( wres.statistics.generated.Threshold threshold,
                         ThresholdType type,
                         Geometry feature,
                         DatasetOrientation featureNameFrom )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Threshold.class );

    /** Default feature orientation. */
    public static final DatasetOrientation DEFAULT_FEATURE_NAME_FROM = DatasetOrientation.LEFT;

    /**
     * Creates an instance.
     * @param threshold a threshold
     * @param type the threshold type to help identify the declaration context
     * @param feature a feature
     * @param featureNameFrom the orientation of the data from which the named feature is taken
     */
    public Threshold
    {
        if ( Objects.nonNull( feature ) && Objects.isNull( featureNameFrom ) )
        {
            LOGGER.debug( "Discovered a threshold for feature {}, but the orientation of the feature name was not "
                          + "supplied. Assuming an orientation of {}.",
                          feature.getName(),
                          Threshold.DEFAULT_FEATURE_NAME_FROM );

            featureNameFrom = Threshold.DEFAULT_FEATURE_NAME_FROM;
        }
    }

    @Override
    public String toString()
    {
        // Remove unnecessary whitespace from the JSON protobuf string
        String thresholdString = DeclarationFactory.PROTBUF_STRINGIFIER.apply( this.threshold() );
        String featureString = DeclarationFactory.PROTBUF_STRINGIFIER.apply( this.feature() );

        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "threshold", thresholdString )
                .append( "thresholdType", this.type() )
                .append( "featureName", featureString )
                .append( "featureNameFrom", this.featureNameFrom() )
                .build();
    }
}
