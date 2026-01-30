package wres.config.deserializers;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;

import wres.config.DeclarationFactory;
import wres.config.components.CovariateDataset;
import wres.config.components.CovariateDatasetBuilder;
import wres.config.components.CovariatePurpose;
import wres.config.components.Dataset;
import wres.statistics.generated.TimeScale;

/**
 * Custom deserializer for a covariate dataset, which composes an ordinary dataset and adds some attributes.
 *
 * @author James Brown
 */
public class CovariateDatasetDeserializer extends ValueDeserializer<CovariateDataset>
{
    @Override
    public CovariateDataset deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        // Build a deserializer here because it is stateful
        DatasetDeserializer deserializer = new DatasetDeserializer();
        Dataset basicDataset = deserializer.deserialize( jp, context );

        Double minimum = null;
        Double maximum = null;
        TimeScale.TimeScaleFunction rescaleFunction = null;
        Set<CovariatePurpose> purposes = null;

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

            if ( lastNode.has( "rescale_function" ) )
            {
                JsonNode functionNode = lastNode.get( "rescale_function" );
                String functionString = functionNode.asString()
                                                    .toUpperCase();
                rescaleFunction = TimeScale.TimeScaleFunction.valueOf( functionString );
            }

            if ( lastNode.has( "purpose" ) )
            {
                JsonNode purposeNode = lastNode.get( "purpose" );

                if ( purposeNode.isString() )
                {
                    String purposeString = purposeNode.asString()
                                                      .toUpperCase();
                    CovariatePurpose purposeEnum = CovariatePurpose.valueOf( purposeString );
                    purposes = Collections.singleton( purposeEnum );
                }
                else if ( purposeNode.isArray() )
                {
                    ObjectReadContext reader = jp.objectReadContext();
                    JsonParser parser = reader.treeAsTokens( purposeNode );
                    ObjectMapper mapper = DeclarationFactory.getObjectDeserializer();
                    JavaType type = mapper.getTypeFactory()
                                          .constructCollectionType( Set.class, CovariatePurpose.class );
                    ObjectReader objectReader = mapper.readerFor( type );
                    purposes = objectReader.readValue( parser );
                }
            }
        }

        return CovariateDatasetBuilder.builder().dataset( basicDataset )
                                      .minimum( minimum )
                                      .maximum( maximum )
                                      .rescaleFunction( rescaleFunction )
                                      .purposes( purposes )
                                      .build();
    }
}