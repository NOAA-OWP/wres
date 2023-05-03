package wres.config.yaml;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.util.StringQuotingChecker;
import org.yaml.snakeyaml.DumperOptions;

/**
 * <p>Implements an {@link YAMLGenerator} with custom styling.
 *
 * <p>This is not a preferred approach to achieve custom styling, but the default implementation of the
 * {@link YAMLGenerator} does not expose the SnakeYAML {@link DumperOptions} to runtime configuration. If the
 * default implementation exposed these options, they would be accessibleto runtime configuration  via the
 * {@link SerializerProvider#getGenerator()}. TODO: if a future version of Jackson exposes these low-level
 * implementation options, remove this custom generator and the associated {@link YAMLFactoryWithCustomGenerator} that
 * creates it. For further discussion, see:
 * <a href="https://github.com/FasterXML/jackson-dataformats-text/issues/4">https://github.com/FasterXML/jackson-dataformats-text/issues/4</a>.
 *
 * <p>Currently, the only custom serialization performed by this class is to use the flow style when serializing array
 * types.
 *
 * @author James Brown
 */
class YAMLGeneratorWithCustomStyle extends YAMLGenerator
{
    /**
     * Creates an instance.
     * @param ctxt the IO context
     * @param jsonFeatures the json feature count
     * @param yamlFeatures the yaml feature count
     * @param quotingChecker the quoting checker
     * @param codec the codec
     * @param out the writer
     * @param version the dumper option version
     * @throws IOException if the instance could not be created for any reason
     */
    public YAMLGeneratorWithCustomStyle( IOContext ctxt,
                                         int jsonFeatures,
                                         int yamlFeatures,
                                         StringQuotingChecker quotingChecker,
                                         ObjectCodec codec,
                                         Writer out,
                                         DumperOptions.Version version ) throws IOException
    {
        super( ctxt, jsonFeatures, yamlFeatures, quotingChecker, codec, out, version );
    }

    @Override
    public void writeObject( Object object ) throws IOException
    {
        // Use the flow style for arrays
        if ( object.getClass()
                   .isArray() )
        {
            DumperOptions.FlowStyle existing = _outputOptions.getDefaultFlowStyle();

            // Set
            _outputOptions.setDefaultFlowStyle( DumperOptions.FlowStyle.FLOW );

            // Write
            super.writeObject( object );

            // Reset
            _outputOptions.setDefaultFlowStyle( existing );
        }
        // Use the existing style for anything else
        else
        {
            super.writeObject( object );
        }
    }
}
