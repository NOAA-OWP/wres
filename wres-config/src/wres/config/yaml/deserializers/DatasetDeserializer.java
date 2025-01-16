package wres.config.yaml.deserializers;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.Variable;

/**
 * Custom deserializer for sources that are represented as an explicit or implicit list of sources.
 *
 * @author James Brown
 */
public class DatasetDeserializer extends JsonDeserializer<Dataset>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DatasetDeserializer.class );

    /** The last node read, which allows for compositions of this class. */
    private JsonNode lastNode = null;

    /** Duration deserializer. **/
    private static final DurationDeserializer DURATION_DESERIALIZER = new DurationDeserializer();

    /** Time zone offset deserializer. **/
    private static final ZoneOffsetDeserializer ZONE_OFFSET_DESERIALIZER = new ZoneOffsetDeserializer();

    /** Time scale deserializer. **/
    private static final TimeScaleDeserializer TIME_SCALE_DESERIALIZER = new TimeScaleDeserializer();

    @Override
    public Dataset deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader reader = ( ObjectReader ) jp.getCodec();
        JsonNode node = reader.readTree( jp );

        // Set the last node read
        this.lastNode = node;

        // Complex collection of explicit sources and other attributes 
        if ( node instanceof ObjectNode )
        {
            TreeNode sourcesNode = node.get( "sources" );
            List<Source> sources;

            // Explicit array?
            if ( sourcesNode instanceof ObjectNode sourceNode )
            {
                Source nextSource = reader.readValue( sourceNode, Source.class );
                sources = List.of( nextSource );
            }
            else if ( sourcesNode instanceof ArrayNode arrayNode )
            {
                sources = this.getSourcesFromArray( reader, arrayNode );
            }
            // Singleton
            else if ( sourcesNode instanceof TextNode textNode )
            {
                sources = this.getSingletonSource( textNode );
            }
            // Data direct
            else if ( sourcesNode instanceof NullNode || Objects.isNull( sourcesNode ) )
            {
                LOGGER.debug( "Discovered a null sources node, which is allowed with data direct." );
                sources = Collections.emptyList();
            }
            else
            {
                throw new IOException( "Unsupported format for sources node: " + sourcesNode.getClass() );
            }

            Variable variable = this.getVariable( reader, node );
            FeatureAuthority featureAuthority = this.getFeatureAuthority( node );
            DataType dataType = this.getDataType( reader, node, jp.currentName() );
            String label = this.getStringValue( reader, node.get( "label" ) );
            EnsembleFilter ensembleFilter = this.getEnsembleFilter( reader, node );
            Duration timeShift = this.getTimeShift( node.get( "time_shift" ), reader, context );
            ZoneOffset zoneOffset = this.getTimeZoneOffset( node.get( "time_zone_offset" ), reader, context );
            TimeScale timeScale = this.getTimeScale( node.get( "time_scale" ), reader, context );
            String unit = this.getStringValue( reader, node.get( "unit" ) );
            return DatasetBuilder.builder()
                                 .sources( sources )
                                 .variable( variable )
                                 .featureAuthority( featureAuthority )
                                 .type( dataType )
                                 .label( label )
                                 .ensembleFilter( ensembleFilter )
                                 .timeShift( timeShift )
                                 .timeZoneOffset( zoneOffset )
                                 .timeScale( timeScale )
                                 .unit( unit )
                                 .build();
        }
        // Singleton
        else if ( node instanceof TextNode textNode )
        {
            List<Source> sources = this.getSingletonSource( textNode );
            return DatasetBuilder.builder()
                                 .sources( sources )
                                 .build();
        }
        // Plain array of sources
        else if ( node instanceof ArrayNode arrayNode )
        {
            List<Source> sources = this.getSourcesFromArray( reader, arrayNode );
            return DatasetBuilder.builder()
                                 .sources( sources )
                                 .build();
        }
        else
        {
            throw new IOException( "When reading the '" + jp.currentName()
                                   + "' declaration of 'sources', discovered an unrecognized data type. Please "
                                   + "fix this declaration and try again." );
        }
    }

    /**
     * @return the last node read
     */

    JsonNode getLastNode()
    {
        return this.lastNode;
    }

    /**
     * Creates a collection of sources from an array node.
     * @param reader the mapper
     * @param sourcesNode the sources node
     * @return the sources
     * @throws IOException if the sources could not be mapped
     */

    private List<Source> getSourcesFromArray( ObjectReader reader, ArrayNode sourcesNode ) throws IOException
    {
        List<Source> sources = new ArrayList<>();
        int nodeCount = sourcesNode.size();

        for ( int i = 0; i < nodeCount; i++ )
        {
            JsonNode nextNode = sourcesNode.get( i );
            Source nextSource;

            // May or may not be addressable with a 'uri' key
            if ( nextNode.has( "uri" ) )
            {
                nextSource = reader.readValue( nextNode, Source.class );
            }
            else
            {
                String nextUriString = nextNode.asText();

                URI uri = UriDeserializer.deserializeUri( nextUriString );
                nextSource = SourceBuilder.builder()
                                          .uri( uri )
                                          .build();
            }

            sources.add( nextSource );
        }

        return sources;
    }

    /**
     * Creates a list of sources from a singleton node of sources.
     * @param node the node of sources
     * @return the list of sources
     */

    private List<Source> getSingletonSource( TextNode node )
    {
        String nextUriString = node.asText();
        URI uri = UriDeserializer.deserializeUri( nextUriString );
        Source source = SourceBuilder.builder()
                                     .uri( uri )
                                     .build();
        return List.of( source );
    }

    /**
     * Creates a variable from a json node.
     * @param reader the mapper
     * @param node the node to check for a variable
     * @return the variable or null
     * @throws IOException if the variable could not be mapped
     */

    private Variable getVariable( ObjectReader reader, JsonNode node ) throws IOException
    {
        if ( !node.has( "variable" ) )
        {
            return null;
        }

        JsonNode variableNode = node.get( "variable" );

        // Plain variable declaration
        if ( !variableNode.has( "name" ) )
        {
            String variableName = variableNode.asText();
            return new Variable( variableName, null, Set.of() );
        }

        // Ordinary variable declaration
        return reader.readValue( variableNode, Variable.class );
    }

    /**
     * Reads the {@link FeatureAuthority} from a node.
     * @param node the node
     * @return the feature authority or null
     */
    private FeatureAuthority getFeatureAuthority( JsonNode node )
    {
        FeatureAuthority featureAuthority = null;

        if ( Objects.nonNull( node.get( "feature_authority" ) ) )
        {
            JsonNode featureAuthorityNode = node.get( "feature_authority" );
            String featureAuthorityName = DeclarationUtilities.toEnumName( featureAuthorityNode.asText() );
            featureAuthority = FeatureAuthority.valueOf( featureAuthorityName );
        }

        return featureAuthority;
    }

    /**
     * Creates a string from a json node.
     * @param reader the mapper
     * @param node the node
     * @return the string or null
     * @throws IOException if the feature authority could not be mapped
     */

    private String getStringValue( ObjectReader reader, JsonNode node ) throws IOException
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        return reader.readValue( node, String.class );
    }

    /**
     * Creates an ensemble filter from a json node.
     * @param reader the mapper
     * @param node the node to check for an ensemble filter
     * @return the filter or null
     * @throws IOException if the filter could not be mapped
     */

    private EnsembleFilter getEnsembleFilter( ObjectReader reader, JsonNode node ) throws IOException
    {
        if ( !node.has( "ensemble_filter" ) )
        {
            return null;
        }

        JsonNode filterNode = node.get( "ensemble_filter" );

        Set<String> members;
        boolean exclude = false;

        // Plain member array
        if ( !filterNode.has( "members" ) )
        {
            members = this.getMembers( reader, filterNode );
        }
        else
        {
            JsonNode memberNode = filterNode.get( "members" );
            members = this.getMembers( reader, memberNode );
            exclude = filterNode.has( "exclude" )
                      && filterNode.get( "exclude" )
                                   .asBoolean();
        }

        // Ordinary member declaration
        return new EnsembleFilter( members, exclude );
    }

    /**
     * @param reader the reader
     * @param node the node
     * @return a set of ensemble members to filter
     * @throws IOException if the members could not be read
     */

    private Set<String> getMembers( ObjectReader reader, JsonNode node ) throws IOException
    {
        JavaType type = reader.getTypeFactory()
                              .constructCollectionType( Set.class, String.class );
        JsonParser parser = reader.treeAsTokens( node );
        Set<String> rawSources = reader.readValue( parser, type );
        return Collections.unmodifiableSet( rawSources );
    }

    /**
     * @param node the node
     * @param reader the reader
     * @param context the context
     * @return the time shift or null
     */

    private Duration getTimeShift( JsonNode node, ObjectReader reader, DeserializationContext context )
            throws IOException
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        return DURATION_DESERIALIZER.deserialize( node.traverse( reader ), context );
    }

    /**
     * @param node the node
     * @param reader the reader
     * @param context the context
     * @return the time shift or null
     */

    private ZoneOffset getTimeZoneOffset( JsonNode node, ObjectReader reader, DeserializationContext context )
            throws IOException
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        return ZONE_OFFSET_DESERIALIZER.deserialize( node.traverse( reader ), context );
    }

    /**
     * @param node the node
     * @param reader the reader
     * @param context the context
     * @return the time shift or null
     */

    private TimeScale getTimeScale( JsonNode node, ObjectReader reader, DeserializationContext context )
            throws IOException
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        return TIME_SCALE_DESERIALIZER.deserialize( node.traverse( reader ), context );
    }

    /**
     * Reads the data type.
     * @param reader the reader
     * @param node the node to read
     * @param context the context to help with logging
     * @return the data type or null
     * @throws IOException if the type could not be read
     */
    private DataType getDataType( ObjectReader reader, JsonNode node, String context ) throws IOException
    {
        if ( node.has( "type" ) )
        {
            return reader.readValue( node.get( "type" ), DataType.class );
        }

        LOGGER.debug( "No data type discovered for {}. The data type will be inferred.", context );

        return null;
    }
}