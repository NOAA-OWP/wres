package wres.config.deserializers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.TreeNode;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.DeclarationUtilities;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.EnsembleFilter;
import wres.config.components.FeatureAuthority;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.TimeScale;
import wres.config.components.Variable;

/**
 * Custom deserializer for sources that are represented as an explicit or implicit list of sources.
 *
 * @author James Brown
 */
public class DatasetDeserializer extends ValueDeserializer<Dataset>
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
    public Dataset deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );
        ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();

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
                Source nextSource = mapper.treeToValue( sourceNode, Source.class );
                sources = List.of( nextSource );
            }
            else if ( sourcesNode instanceof ArrayNode arrayNode )
            {
                sources = this.getSourcesFromArray( mapper, arrayNode );
            }
            // Singleton
            else if ( sourcesNode instanceof StringNode textNode )
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
                throw new UncheckedIOException( new IOException( "Unsupported format for sources node: "
                                                                 + sourcesNode.getClass() ) );
            }

            Variable variable = this.getVariable( mapper, node );
            FeatureAuthority featureAuthority = this.getFeatureAuthority( node );
            DataType dataType = this.getDataType( mapper, node, jp.currentName() );
            String label = this.getStringValue( mapper, node.get( "label" ) );
            EnsembleFilter ensembleFilter = this.getEnsembleFilter( mapper, node );
            Duration timeShift = this.getTimeShift( node.get( "time_shift" ), jp.objectReadContext(), context );
            ZoneOffset zoneOffset =
                    this.getTimeZoneOffset( node.get( "time_zone_offset" ), jp.objectReadContext(), context );
            List<Double> missingValue = this.getMissingValue( node.get( "missing_value" ), jp.objectReadContext() );
            TimeScale timeScale = this.getTimeScale( node.get( "time_scale" ), jp.objectReadContext(), context );
            String unit = this.getStringValue( mapper, node.get( "unit" ) );
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
                                 .missingValue( missingValue )
                                 .build();
        }
        // Singleton
        else if ( node instanceof StringNode textNode )
        {
            List<Source> sources = this.getSingletonSource( textNode );
            return DatasetBuilder.builder()
                                 .sources( sources )
                                 .build();
        }
        // Plain array of sources
        else if ( node instanceof ArrayNode arrayNode )
        {
            List<Source> sources = this.getSourcesFromArray( mapper, arrayNode );
            return DatasetBuilder.builder()
                                 .sources( sources )
                                 .build();
        }
        else
        {
            throw new UncheckedIOException( new IOException( "When reading the '"
                                                             + jp.currentName()
                                                             + "' declaration of 'sources', discovered an unrecognized "
                                                             + "data type. Please fix this declaration and try again." ) );
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
     */

    private List<Source> getSourcesFromArray( ObjectMapper reader, ArrayNode sourcesNode )
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
                nextSource = reader.readerFor( Source.class )
                                   .readValue( nextNode );
            }
            else
            {
                String nextUriString = nextNode.asString();

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

    private List<Source> getSingletonSource( StringNode node )
    {
        String nextUriString = node.asString();
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
     */

    private Variable getVariable( ObjectMapper reader, JsonNode node )
    {
        if ( !node.has( "variable" ) )
        {
            return null;
        }

        JsonNode variableNode = node.get( "variable" );

        // Plain variable declaration
        if ( !variableNode.has( "name" ) )
        {
            String variableName = variableNode.asString();
            return new Variable( variableName, null, Set.of() );
        }

        // Ordinary variable declaration
        return reader.readerFor( Variable.class )
                     .readValue( variableNode );
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
            String featureAuthorityName = DeclarationUtilities.toEnumName( featureAuthorityNode.asString() );
            featureAuthority = FeatureAuthority.valueOf( featureAuthorityName );
        }

        return featureAuthority;
    }

    /**
     * Creates a string from a json node.
     * @param reader the mapper
     * @param node the node
     * @return the string or null
     */

    private String getStringValue( ObjectMapper reader, JsonNode node )
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        return reader.readerFor( String.class )
                     .readValue( node );
    }

    /**
     * Creates an ensemble filter from a json node.
     * @param reader the mapper
     * @param node the node to check for an ensemble filter
     * @return the filter or null
     */

    private EnsembleFilter getEnsembleFilter( ObjectMapper reader, JsonNode node )
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
     */

    private Set<String> getMembers( ObjectMapper reader, JsonNode node )
    {
        JsonParser parser = reader.treeAsTokens( node );
        JavaType type = reader.getTypeFactory()
                              .constructCollectionType( Set.class, String.class );
        Set<String> rawSources = reader.readerFor( type )
                                       .readValue( parser );
        return Collections.unmodifiableSet( rawSources );
    }

    /**
     * @param node the node
     * @param reader the reader
     * @param context the context
     * @return the time shift or null
     */

    private Duration getTimeShift( JsonNode node, ObjectReadContext reader, DeserializationContext context )
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

    private ZoneOffset getTimeZoneOffset( JsonNode node, ObjectReadContext reader, DeserializationContext context )
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
     * @return the time shift or null
     */

    private List<Double> getMissingValue( JsonNode node, ObjectReadContext reader )
    {
        if ( Objects.isNull( node ) )
        {
            return List.of();
        }

        Double[] array = reader.readValue( node.traverse( reader ), Double[].class );
        return Arrays.asList( array );
    }

    /**
     * @param node the node
     * @param reader the reader
     * @param context the context
     * @return the time shift or null
     */

    private TimeScale getTimeScale( JsonNode node, ObjectReadContext reader, DeserializationContext context )
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
     */
    private DataType getDataType( ObjectMapper reader, JsonNode node, String context )
    {
        if ( node.has( "type" ) )
        {
            return reader.readerFor( DataType.class )
                         .readValue( node.get( "type" ) );
        }

        LOGGER.debug( "No data type discovered for {}. The data type will be inferred.", context );

        return null;
    }
}