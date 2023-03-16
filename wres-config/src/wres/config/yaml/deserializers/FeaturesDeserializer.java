package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
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

import wres.config.yaml.components.Features;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * Custom deserializer for features that are represented as an explicit or implicit list of tuples.
 *
 * @author James Brown
 */
public class FeaturesDeserializer extends JsonDeserializer<Features>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeaturesDeserializer.class );

    private static final String OBSERVED = "observed";
    private static final String PREDICTED = "predicted";
    private static final String BASELINE = "baseline";

    @Override
    public Features deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Explicit/sided features
        if ( node instanceof ObjectNode )
        {
            LOGGER.debug( "Discovered an object of features to parse." );
            TreeNode featureNode = node.get( "features" );
            return this.getFeaturesFromArray( reader, ( ArrayNode ) featureNode );
        }
        // Plain array of feature names
        else if ( node instanceof ArrayNode arrayNode )
        {
            LOGGER.debug( "Discovered a plain array of features to parse." );
            return this.getFeaturesFromArray( reader, arrayNode );
        }
        else
        {
            throw new IOException( "When reading the '" + jp.currentName()
                                   + "' declaration of 'features', discovered an unrecognized data type. Please fix "
                                   + "this declaration and try again." );
        }
    }

    /**
     * Creates a collection of features from an array node.
     * @param reader the mapper
     * @param featuresNode the features node
     * @return the features
     * @throws IOException if the features could not be mapped
     */

    private Features getFeaturesFromArray( ObjectReader reader,
                                           ArrayNode featuresNode )
            throws IOException
    {
        // Preserve insertion order
        Set<GeometryTuple> features = new LinkedHashSet<>();
        int nodeCount = featuresNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = featuresNode.get( i );

            if ( nextNode.has( OBSERVED )
                 || nextNode.has( PREDICTED )
                 || nextNode.has( BASELINE ) )
            {
                GeometryTuple nextFeature = this.getGeometryTuple( reader, nextNode );
                features.add( nextFeature );
            }
            else
            {
                // Apply to the left side only and fill out later because this depends on other declaration, such as
                // whether a baseline is declared
                Geometry leftGeometry = this.getGeometry( reader, nextNode );
                GeometryTuple tuple = GeometryTuple.newBuilder()
                                                   .setLeft( leftGeometry )
                                                   .build();

                features.add( tuple );
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Discovered implicit feature tuple declaration for feature {}.",
                                  nextNode.asText() );
                }
            }
        }

        return new Features( Collections.unmodifiableSet( features ) );
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

        if ( node.has( OBSERVED ) )
        {
            // Full feature description
            JsonNode leftNode = node.get( OBSERVED );
            Geometry leftGeometry = this.getGeometry( reader, leftNode );
            builder.setLeft( leftGeometry );
        }

        if ( node.has( PREDICTED ) )
        {
            JsonNode rightNode = node.get( PREDICTED );
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
