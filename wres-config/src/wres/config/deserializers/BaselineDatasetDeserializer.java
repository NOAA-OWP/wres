package wres.config.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.TextNode;

import wres.config.DeclarationUtilities;
import wres.config.components.BaselineDataset;
import wres.config.components.Dataset;
import wres.config.components.GeneratedBaseline;
import wres.config.components.GeneratedBaselineBuilder;
import wres.config.components.GeneratedBaselines;

/**
 * Custom deserializer for a baseline dataset, which composes an ordinary dataset and adds some attributes.
 * 
 * @author James Brown
 */
public class BaselineDatasetDeserializer extends JsonDeserializer<BaselineDataset>
{
    @Override
    public BaselineDataset deserialize( JsonParser jp, DeserializationContext context ) throws IOException
    {
        Objects.requireNonNull( jp );

        // Build a deserializer here because it is stateful
        DatasetDeserializer deserializer = new DatasetDeserializer();
        Dataset basicDataset = deserializer.deserialize( jp, context );

        GeneratedBaseline generatedBaseline = null;
        Boolean separateMetrics = null;

        // The node just read
        JsonNode lastNode = deserializer.getLastNode();
        if ( Objects.nonNull( lastNode ) )
        {
            if( lastNode.has( "method" ) )
            {
                JsonNode methodNode = lastNode.get( "method" );
                if ( methodNode instanceof TextNode textNode )
                {
                    String methodString = textNode.asText();
                    methodString = DeclarationUtilities.toEnumName( methodString );
                    GeneratedBaselines method = GeneratedBaselines.valueOf( methodString );
                    generatedBaseline = GeneratedBaselineBuilder.builder()
                                                   .method( method )
                                                   .build();
                }
                else
                {
                    ObjectReader mapper = ( ObjectReader ) jp.getCodec();
                    generatedBaseline = mapper.readValue( methodNode, GeneratedBaseline.class );
                }
            }

            if( lastNode.has( "separate_metrics" ) )
            {
                separateMetrics = lastNode.get( "separate_metrics" )
                                          .asBoolean();
            }
        }

        return new BaselineDataset( basicDataset, generatedBaseline, separateMetrics );
    }
}