package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariateDatasetBuilder;
import wres.config.yaml.components.CovariatePurpose;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.statistics.generated.TimeScale;

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
        TimeScale.TimeScaleFunction rescaleFunction = null;
        Set<CovariatePurpose> purposes = Collections.singleton( CovariatePurpose.FILTER );

        // Not part of the declaration language, just used internally
        DatasetOrientation featureNameOrientation = null;

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
                String functionString = functionNode.asText()
                                                    .toUpperCase();
                rescaleFunction = TimeScale.TimeScaleFunction.valueOf( functionString );
            }

            if ( lastNode.has( "purpose" ) )
            {
                JsonNode purposeNode = lastNode.get( "purpose" );

                if ( purposeNode.isTextual() )
                {
                    String purposeString = purposeNode.asText()
                                                      .toUpperCase();
                    CovariatePurpose purposeEnum = CovariatePurpose.valueOf( purposeString );
                    purposes = Collections.singleton( purposeEnum );
                }
                else if ( purposeNode.isArray() )
                {
                    ObjectReader reader = ( ObjectReader ) jp.getCodec();
                    JavaType type = reader.getTypeFactory()
                                          .constructCollectionType( Set.class, CovariatePurpose.class );
                    JsonParser parser = reader.treeAsTokens( purposeNode );
                    purposes = reader.readValue( parser, type );
                }
            }
        }

        return CovariateDatasetBuilder.builder().dataset( basicDataset )
                                      .minimum( minimum )
                                      .maximum( maximum )
                                      .featureNameOrientation( featureNameOrientation )
                                      .rescaleFunction( rescaleFunction )
                                      .purposes( purposes )
                                      .build();
    }
}