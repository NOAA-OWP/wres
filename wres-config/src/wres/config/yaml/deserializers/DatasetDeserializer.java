package wres.config.yaml.deserializers;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.Source;
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
            List<Source> sources = this.getSourcesFromArray( reader, ( ArrayNode ) sourcesNode );
            Variable variable = this.getVariable( reader, node );
            String featureAuthority = this.getStringValue( reader, node.get( "feature_authority" ) );
            DataType dataType = this.getDataType( reader, node, jp.currentName() );
            String label = this.getStringValue( reader, node.get( "label" ) );
            EnsembleFilter ensembleFilter = this.getEnsembleFilter( reader, node );
            Duration timeShift = this.getTimeShift( node.get( "time_shift" ) );
            return new Dataset( sources, variable, featureAuthority, dataType, label, ensembleFilter, timeShift );
        }
        // Plain array of sources
        else if ( node instanceof ArrayNode arrayNode )
        {
            List<Source> sources = this.getSourcesFromArray( reader, arrayNode );
            return new Dataset( sources, null, null, null, null, null, null );
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
                URI uri = URI.create( nextUriString );
                nextSource = new Source( uri, null, null, null, null, null );
            }

            sources.add( nextSource );
        }

        return sources;
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
            return new Variable( variableName, null );
        }

        // Ordinary variable declaration
        return reader.readValue( variableNode, Variable.class );
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

        List<String> members;
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
            exclude = filterNode.has( "exclude" ) && filterNode.get( "exclude" )
                                                               .asBoolean();
        }

        // Ordinary member declaration
        return new EnsembleFilter( members, exclude );
    }

    /**
     * @param reader the reader
     * @param node the node
     * @return a list of ensemble members to filter
     * @throws IOException if the members could not be read
     */

    private List<String> getMembers( ObjectReader reader, JsonNode node ) throws IOException
    {
        JavaType type = reader.getTypeFactory()
                              .constructCollectionType( List.class, String.class );
        JsonParser parser = reader.treeAsTokens( node );
        List<String> rawSources = reader.readValue( parser, type );
        return Collections.unmodifiableList( rawSources );
    }

    /**
     * @param node the node
     * @return the time shift or null
     */

    private Duration getTimeShift( JsonNode node )
    {
        if ( Objects.isNull( node ) )
        {
            return null;
        }

        String unitString = node.get( "unit" )
                                .asText()
                                .toUpperCase();

        ChronoUnit unit = ChronoUnit.valueOf( unitString );
        int amount = node.get( "amount" )
                         .asInt();

        return Duration.of( amount, unit );
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