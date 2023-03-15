package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Custom deserializer for feature groups.
 *
 * @author James Brown
 */
public class FeatureGroupsDeserializer extends JsonDeserializer<FeatureGroups>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroupsDeserializer.class );

    /** Feature deserializer. */
    private static final FeaturesDeserializer FEATURES_DESERIALIZER = new FeaturesDeserializer();

    @Override
    public FeatureGroups deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Array of groups
        if ( node instanceof ArrayNode arrayNode )
        {
            return this.getFeatureGroupsFromArray( reader, context, arrayNode );
        }
        else
        {
            throw new IOException( "When reading the '" + jp.currentName()
                                   + "' declaration of 'feature_groups', discovered an unrecognized data type. Please "
                                   + "fix this declaration and try again." );
        }
    }

    /**
     * Creates a collection of feature groups from an array node.
     *
     * @param context the deserialization context
     * @param featureGroupsNode the feature groups node
     * @return the features
     * @throws IOException if the features could not be mapped
     */

    private FeatureGroups getFeatureGroupsFromArray( ObjectReader reader,
                                                     DeserializationContext context,
                                                     ArrayNode featureGroupsNode )
            throws IOException
    {
        // Preserve insertion order
        Set<GeometryGroup> featureGroups = new LinkedHashSet<>();
        int nodeCount = featureGroupsNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = featureGroupsNode.get( i );
            String groupName = "";
            Set<GeometryTuple> geometries = null;

            // Group name
            if ( nextNode.has( "name" ) )
            {
                JsonNode groupNameNode = nextNode.get( "name" );
                groupName = groupNameNode.asText();
                LOGGER.debug( "When reading feature groups, discovered a group named {}.", groupName );
            }

            // Geometries
            if ( nextNode.has( "features" ) )
            {
                JsonNode featuresNode = nextNode.get( "features" );
                JsonParser parser = featuresNode.traverse();
                parser.setCodec( reader );
                Features features = FEATURES_DESERIALIZER.deserialize( parser, context );
                geometries = features.geometries();
                LOGGER.debug( "Discovered the following collection of geometries associated with a feature group "
                              + "named '{}': {}.", groupName, new Features( geometries ) );
            }

            // Create the group
            if ( Objects.nonNull( geometries ) )
            {
                GeometryGroup geoGroup = GeometryGroup.newBuilder()
                                                      .setRegionName( groupName )
                                                      .addAllGeometryTuples( geometries )
                                                      .build();
                featureGroups.add( geoGroup );
            }
        }

        return new FeatureGroups( Collections.unmodifiableSet( featureGroups ) );
    }
}
