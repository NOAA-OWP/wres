package wres.config.deserializers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Offset;
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

    /** Re-used string. */
    private static final String OBSERVED = "observed";

    /** Re-used string. */
    private static final String PREDICTED = "predicted";

    /** Re-used string. */
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
        Map<GeometryTuple, Offset> offsets = new LinkedHashMap<>();

        int nodeCount = featuresNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = featuresNode.get( i );

            // Explicit feature declaration
            if ( nextNode.has( OBSERVED )
                 || nextNode.has( PREDICTED )
                 || nextNode.has( BASELINE ) )
            {
                Pair<GeometryTuple, Offset> nextFeature = this.getGeometryTuple( reader, nextNode );
                features.add( nextFeature.getKey() );
                if ( Objects.nonNull( nextFeature.getValue() ) )
                {
                    offsets.put( nextFeature.getKey(), nextFeature.getValue() );
                }
            }
            else
            {
                // Apply to the left side only and fill out later because this depends on other declaration, such as
                // whether a baseline is declared
                Pair<Geometry, Double> leftGeometry = this.getGeometry( reader, nextNode );
                GeometryTuple tuple = GeometryTuple.newBuilder()
                                                   .setLeft( leftGeometry.getKey() )
                                                   .build();

                features.add( tuple );
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Discovered implicit feature tuple declaration for feature {}.",
                                  nextNode.asText() );
                }
            }
        }

        return FeaturesBuilder.builder()
                              .geometries( features )
                              .offsets( offsets )
                              .build();
    }

    /**
     * Creates a set of geometries from a json node.
     * @param reader the reader
     * @param node the node to check for geometries
     * @return the geometries
     * @throws IOException if the geometries could not be mapped
     */

    private Pair<GeometryTuple, Offset> getGeometryTuple( ObjectReader reader, JsonNode node ) throws IOException
    {
        GeometryTuple.Builder builder = GeometryTuple.newBuilder();

        Double leftOffset = null;
        Double rightOffset = null;
        Double baselineOffset = null;

        if ( node.has( OBSERVED ) )
        {
            // Full feature description
            JsonNode leftNode = node.get( OBSERVED );
            Pair<Geometry, Double> leftGeometry = this.getGeometry( reader, leftNode );
            builder.setLeft( leftGeometry.getKey() );
            leftOffset = leftGeometry.getValue();
        }

        if ( node.has( PREDICTED ) )
        {
            JsonNode rightNode = node.get( PREDICTED );
            Pair<Geometry, Double> rightGeometry = this.getGeometry( reader, rightNode );
            builder.setRight( rightGeometry.getKey() );
            rightOffset = rightGeometry.getValue();
        }

        if ( node.has( BASELINE ) )
        {
            JsonNode baselineNode = node.get( BASELINE );
            Pair<Geometry, Double> baselineGeometry = this.getGeometry( reader, baselineNode );
            builder.setBaseline( baselineGeometry.getKey() );
            baselineOffset = baselineGeometry.getValue();
        }

        Offset offset = this.getOffset( leftOffset, rightOffset, baselineOffset );

        return Pair.of( builder.build(), offset );
    }

    /**
     * Reads a geometry from a geometry node.
     * @param reader the reader
     * @param geometryNode the geometry node
     * @return the geometry
     * @throws IOException if the node could not be read
     */

    private Pair<Geometry, Double> getGeometry( ObjectReader reader, JsonNode geometryNode ) throws IOException
    {
        if ( geometryNode.has( "name" ) )
        {
            Double offset = null;

            if ( geometryNode.has( "offset" ) )
            {
                offset = geometryNode.get( "offset" )
                                     .asDouble();
            }

            Geometry geometry = reader.readValue( geometryNode, Geometry.class );
            return Pair.of( geometry, offset );
        }
        else
        {
            String name = geometryNode.asText();
            Geometry geometry = Geometry.newBuilder()
                                        .setName( name )
                                        .build();
            return Pair.of( geometry, null );
        }
    }

    /**
     * Returns an {@link Offset} from the supplied numerical offset values.
     * @param leftOffset the left offset value
     * @param rightOffset the right offset value
     * @param baselineOffset the baseline offset value
     * @return the offset
     */
    private Offset getOffset( Double leftOffset, Double rightOffset, Double baselineOffset )
    {
        Offset offset = null;

        if ( Objects.nonNull( leftOffset )
             || Objects.nonNull( rightOffset )
             || Objects.nonNull( baselineOffset ) )
        {
            double left = 0.0;
            double right = 0.0;
            double baseline = 0.0;

            if ( Objects.nonNull( leftOffset ) )
            {
                left = leftOffset;
            }

            if ( Objects.nonNull( rightOffset ) )
            {
                right = rightOffset;
            }

            if ( Objects.nonNull( baselineOffset ) )
            {
                baseline = baselineOffset;
            }

            offset = new Offset( left, right, baseline );
        }

        return offset;
    }
}
