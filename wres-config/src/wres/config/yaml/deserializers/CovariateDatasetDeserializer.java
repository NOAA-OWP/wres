package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.Dataset;

/**
 * Custom deserializer for a covariate dataset, which composes an ordinary dataset and adds some attributes.
 *
 * @author James Brown
 */
public class CovariateDatasetDeserializer extends JsonDeserializer<CovariateDataset>
{
    @Override
    public CovariateDataset deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        // Build a deserializer here because it is stateful
        DatasetDeserializer deserializer = new DatasetDeserializer();
        Dataset basicDataset = deserializer.deserialize( jp, context );

        Double minimum = null;
        Double maximum = null;

        // The node just read
        JsonNode lastNode = deserializer.getLastNode();
        if ( Objects.nonNull( lastNode ) )
        {
            if ( lastNode.has( "minimum" ) )
            {
                JsonNode minNode = lastNode.get( "minimum" );
                minimum = minNode.asDouble();
            }

            if ( lastNode.has( "maximum" ) )
            {
                JsonNode maxNode = lastNode.get( "maximum" );
                maximum = maxNode.asDouble();
            }
        }

        return new CovariateDataset( basicDataset, minimum, maximum );
    }
}