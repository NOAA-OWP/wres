package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationFactory;
import wres.config.components.DatasetOrientation;

/**
 * Only serializes a {@link DatasetOrientation} that is not default.
 * @author James Brown
 */
public class ThresholdDatasetOrientationSerializer extends ValueSerializer<DatasetOrientation>
{
    @Override
    public void serialize( DatasetOrientation orientation, JsonGenerator writer, SerializationContext serializers )
    {
        writer.writePOJO( orientation );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, DatasetOrientation orientation )
    {
        // Do not write the default
        return orientation == DeclarationFactory.DEFAULT_THRESHOLD_DATASET_ORIENTATION;
    }
}
