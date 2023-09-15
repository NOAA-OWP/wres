package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.DatasetOrientation;

/**
 * Only serializes a {@link DatasetOrientation} that is not default.
 * @author James Brown
 */
public class ThresholdDatasetOrientationSerializer extends JsonSerializer<DatasetOrientation>
{
    @Override
    public void serialize( DatasetOrientation orientation, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        writer.writeObject( orientation );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, DatasetOrientation orientation )
    {
        // Do not write the default
        return orientation == DeclarationFactory.DEFAULT_THRESHOLD_DATASET_ORIENTATION;
    }
}
