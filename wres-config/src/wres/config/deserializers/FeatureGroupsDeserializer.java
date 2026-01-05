package wres.config.deserializers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.FeatureGroups;
import wres.config.components.FeatureGroupsBuilder;
import wres.config.components.Features;
import wres.config.components.Offset;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Custom deserializer for feature groups.
 *
 * @author James Brown
 */
public class FeatureGroupsDeserializer extends ValueDeserializer<FeatureGroups>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroupsDeserializer.class );

    /** Feature deserializer. */
    private static final FeaturesDeserializer FEATURES_DESERIALIZER = new FeaturesDeserializer();

    @Override
    public FeatureGroups deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );

        // Array of groups
        if ( node instanceof ArrayNode arrayNode )
        {
            return this.getFeatureGroupsFromArray( jp.objectReadContext(), context, arrayNode );
        }
        else
        {
            throw new UncheckedIOException( new IOException( "When reading the '" + jp.currentName()
                                                             + "' declaration of 'feature_groups', discovered an "
                                                             + "unrecognized data type. Please fix this declaration "
                                                             + "and try again." ) );
        }
    }

    /**
     * Creates a collection of feature groups from an array node.
     *
     * @param context the deserialization context
     * @param featureGroupsNode the feature groups node
     * @return the features
     */

    private FeatureGroups getFeatureGroupsFromArray( ObjectReadContext reader,
                                                     DeserializationContext context,
                                                     ArrayNode featureGroupsNode )
    {
        // Preserve insertion order
        Set<GeometryGroup> featureGroups = new LinkedHashSet<>();
        Map<GeometryTuple, Offset> featureOffsets = new LinkedHashMap<>();

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
                groupName = groupNameNode.asString();
                LOGGER.debug( "When reading feature groups, discovered a group named {}.", groupName );
            }

            // Geometries
            if ( nextNode.has( "features" ) )
            {
                JsonNode featuresNode = nextNode.get( "features" );
                JsonParser parser = featuresNode.traverse( reader );
                Features features = FEATURES_DESERIALIZER.deserialize( parser, context );
                geometries = features.geometries();
                featureOffsets.putAll( features.offsets() );
                LOGGER.debug( "Discovered the following collection of geometries associated with a feature group "
                              + "named '{}': {}.", groupName, features );
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

        return FeatureGroupsBuilder.builder()
                                   .geometryGroups( featureGroups )
                                   .offsets( featureOffsets )
                                   .build();
    }
}
