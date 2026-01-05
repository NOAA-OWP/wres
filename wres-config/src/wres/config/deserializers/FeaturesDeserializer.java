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
import tools.jackson.core.TreeNode;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
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
public class FeaturesDeserializer extends ValueDeserializer<Features>
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
    public Features deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();

        // Explicit/sided features
        if ( node instanceof ObjectNode )
        {
            LOGGER.debug( "Discovered an object of features to parse." );
            TreeNode featureNode = node.get( "features" );
            return this.getFeaturesFromArray( mapper, ( ArrayNode ) featureNode );
        }
        // Plain array of feature names
        else if ( node instanceof ArrayNode arrayNode )
        {
            LOGGER.debug( "Discovered a plain array of features to parse." );
            return this.getFeaturesFromArray( mapper, arrayNode );
        }
        else
        {
            throw new UncheckedIOException( new IOException( "When reading the '"
                                                             + jp.currentName()
                                                             + "' declaration of 'features', discovered an "
                                                             + "unrecognized data type. Please fix this declaration "
                                                             + "and try again." ) );
        }
    }

    /**
     * Creates a collection of features from an array node.
     * @param reader the mapper
     * @param featuresNode the features node
     * @return the features
     */

    private Features getFeaturesFromArray( ObjectMapper reader,
                                           ArrayNode featuresNode )
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
                                  nextNode.asString() );
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
     */

    private Pair<GeometryTuple, Offset> getGeometryTuple( ObjectMapper reader, JsonNode node )
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
     * @param mapper the object mapper
     * @param geometryNode the geometry node
     * @return the geometry
     */

    private Pair<Geometry, Double> getGeometry( ObjectMapper mapper, JsonNode geometryNode )
    {
        if ( geometryNode.has( "name" ) )
        {
            Double offset = null;

            if ( geometryNode.has( "offset" ) )
            {
                offset = geometryNode.get( "offset" )
                                     .asDouble();
            }

            ObjectReader reader = mapper.readerFor( Geometry.class );
            Geometry geometry = reader.readValue( geometryNode );
            return Pair.of( geometry, offset );
        }
        else
        {
            String name = geometryNode.asString();
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
