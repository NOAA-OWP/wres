package wres.config.deserializers;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.StringNode;

import wres.config.DeclarationFactory;
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
public class BaselineDatasetDeserializer extends ValueDeserializer<BaselineDataset>
{
    @Override
    public BaselineDataset deserialize( JsonParser jp, DeserializationContext context )
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
            if ( lastNode.has( "method" ) )
            {
                JsonNode methodNode = lastNode.get( "method" );
                if ( methodNode instanceof StringNode textNode )
                {
                    String methodString = textNode.asString();
                    methodString = DeclarationUtilities.toEnumName( methodString );
                    GeneratedBaselines method = GeneratedBaselines.valueOf( methodString );
                    generatedBaseline = GeneratedBaselineBuilder.builder()
                                                                .method( method )
                                                                .build();
                }
                else
                {
                    ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
                    ObjectReader objectReader = mapper.readerFor( GeneratedBaseline.class );
                    generatedBaseline = objectReader.readValue( methodNode );
                }
            }

            if ( lastNode.has( "separate_metrics" ) )
            {
                separateMetrics = lastNode.get( "separate_metrics" )
                                          .asBoolean();
            }
        }

        return new BaselineDataset( basicDataset, generatedBaseline, separateMetrics );
    }
}