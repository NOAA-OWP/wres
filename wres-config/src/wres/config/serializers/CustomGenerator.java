package wres.config.serializers;

import java.io.IOException;
import java.io.Writer;

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
 * {@link SerializerProvider#getGenerator()}. If a future version of Jackson exposes these low-level implementation
 * options, remove this custom generator. For further discussion, see:
 * <a href="https://github.com/FasterXML/jackson-dataformats-text/issues/4">https://github.com/FasterXML/jackson-dataformats-text/issues/4</a>.
 *
 * <p>Currently, the only custom serialization performed by this class is to use the flow style when serializing
 * array types.
 *
 * @author James Brown
 */
public class CustomGenerator extends YAMLGenerator
{
    /** Default flow style. */
    private final DumperOptions.FlowStyle defaultFlowStyle;

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
    public CustomGenerator( IOContext ctxt,
                            int jsonFeatures,
                            int yamlFeatures,
                            StringQuotingChecker quotingChecker,
                            ObjectCodec codec,
                            Writer out,
                            DumperOptions.Version version ) throws IOException
    {
        super( ctxt, jsonFeatures, yamlFeatures, quotingChecker, codec, out, version );
        this.defaultFlowStyle = super._outputOptions.getDefaultFlowStyle();
    }

    /**
     * Turn on flow style.
     */
    public void setFlowStyleOn()
    {
        super._outputOptions.setDefaultFlowStyle( DumperOptions.FlowStyle.FLOW );
    }

    /**
     * Turn off flow style.
     */
    public void setFlowStyleOff()
    {
        super._outputOptions.setDefaultFlowStyle( this.defaultFlowStyle );
    }

    @Override
    public void writeObject( Object object ) throws IOException
    {
        // Use the flow style for arrays
        if ( object.getClass()
                   .isArray() )
        {
            DumperOptions.FlowStyle existing = super._outputOptions.getDefaultFlowStyle();

            // Set
            super._outputOptions.setDefaultFlowStyle( DumperOptions.FlowStyle.FLOW );

            // Write
            super.writeObject( object );

            // Reset
            super._outputOptions.setDefaultFlowStyle( existing );
        }
        // Use the existing style for anything else
        else
        {
            super.writeObject( object );
        }
    }
}
