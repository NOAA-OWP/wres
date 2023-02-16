package wres.config.yaml;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * Custom deserializer for features that are represented as an explicit or implicit list of tuples.
 *
 * @author James Brown
 */
class FeaturesDeserializer extends JsonDeserializer<Set<GeometryTuple>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeaturesDeserializer.class );

    public static final String BASELINE = "baseline";
    public static final String RIGHT = "right";
    public static final String LEFT = "left";

    @Override
    public Set<GeometryTuple> deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Explicit/sided features
        if ( node instanceof ObjectNode )
        {
            TreeNode featureNode = node.get( "features" );
            return this.getFeaturesFromArray( reader, ( ArrayNode ) featureNode );
        }
        // Plain array of feature names
        else if ( node instanceof ArrayNode arrayNode )
        {
            return this.getFeaturesFromArray( reader, arrayNode );
        }
        else
        {
            throw new IOException( "When reading the '" + jp.currentName()
                                   + "' declaration of 'features', discovered an unrecognized data type. Please "
                                   + "fix this declaration and try again." );
        }
    }

    /**
     * Generates a geometry node from a plain feature name.
     * @param featureName the feature name
     * @param addBaseline is true to add a baseline feature, false otherwise
     * @return the geometry
     */

    static GeometryTuple getGeneratedFeature( String featureName, boolean addBaseline )
    {
        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( Geometry.newBuilder()
                                                                       .setName( featureName )
                                                                       .build() )
                                                     .setRight( Geometry.newBuilder()
                                                                        .setName( featureName )
                                                                        .build() );

        if ( addBaseline )
        {
            builder.setBaseline( Geometry.newBuilder()
                                         .setName( featureName )
                                         .build() );
        }

        return builder.build();
    }

    /**
     * Creates a collection of features from an array node.
     * @param reader the mapper
     * @param featuresNode the features node
     * @return the features
     * @throws IOException if the features could not be mapped
     */

    private Set<GeometryTuple> getFeaturesFromArray( ObjectReader reader, ArrayNode featuresNode ) throws IOException
    {
        Set<GeometryTuple> features = new HashSet<>();
        int nodeCount = featuresNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = featuresNode.get( i );

            if ( nextNode.has( LEFT )
                 || nextNode.has( RIGHT )
                 || nextNode.has( BASELINE ) )
            {
                GeometryTuple nextFeature = this.getGeometryTuple( reader, nextNode );
                features.add( nextFeature );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Skipping feature {} because it must be generated when all declaration has been parsed.",
                              nextNode.asText() );
            }
        }

        return features;
    }

    /**
     * Creates a set of geometries from a json node.
     * @param reader the reader
     * @param node the node to check for geometries
     * @return the geometries
     * @throws IOException if the geometries could not be mapped
     */

    private GeometryTuple getGeometryTuple( ObjectReader reader, JsonNode node ) throws IOException
    {
        GeometryTuple.Builder builder = GeometryTuple.newBuilder();

        if ( node.has( LEFT ) )
        {
            // Full feature description
            JsonNode leftNode = node.get( LEFT );
            Geometry leftGeometry = this.getGeometry( reader, leftNode );
            builder.setLeft( leftGeometry );
        }

        if ( node.has( RIGHT ) )
        {
            JsonNode rightNode = node.get( RIGHT );
            Geometry rightGeometry = this.getGeometry( reader, rightNode );
            builder.setRight( rightGeometry );
        }

        if ( node.has( BASELINE ) )
        {
            JsonNode baselineNode = node.get( BASELINE );
            Geometry baselineGeometry = this.getGeometry( reader, baselineNode );
            builder.setBaseline( baselineGeometry );
        }

        return builder.build();
    }

    /**
     * Reads a geometry from a geometry node.
     * @param reader the reader
     * @param geometryNode the geometry node
     * @return the geometry
     * @throws IOException if the node could not be read
     */

    private Geometry getGeometry( ObjectReader reader, JsonNode geometryNode ) throws IOException
    {
        if ( geometryNode.has( "name" ) )
        {
            return reader.readValue( geometryNode, Geometry.class );
        }
        else
        {
            String name = geometryNode.asText();
            return Geometry.newBuilder()
                           .setName( name )
                           .build();
        }
    }

}
