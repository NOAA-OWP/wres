package wres.config.yaml;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import wres.config.yaml.DeclarationFactory.BaselineDataset;
import wres.config.yaml.DeclarationFactory.Dataset;

/**
 * Custom deserializer for a baseline dataset, which composes an ordinary dataset and adds some attributes.
 * 
 * @author James Brown
 */
class BaselineDeserializer extends JsonDeserializer<BaselineDataset>
{
    @Override
    public BaselineDataset deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        // Build a deserializer here because it is stateful
        DatasetDeserializer deserializer = new DatasetDeserializer();
        Dataset basicDataset = deserializer.deserialize( jp, context );

        Integer order = null;

        // The node just read
        JsonNode lastNode = deserializer.getLastNode();
        if ( Objects.nonNull( lastNode )
             && lastNode.has( "persistence" ) )
        {
            order = lastNode.get( "persistence" )
                            .asInt();
        }

        return new BaselineDataset( basicDataset, order );
    }
}