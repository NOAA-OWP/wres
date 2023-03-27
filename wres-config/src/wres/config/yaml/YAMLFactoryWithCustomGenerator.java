package wres.config.yaml;

import java.io.IOException;
import java.io.Writer;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * See the explanation in {@link YAMLGeneratorWithCustomStyle}. This is a workaround to expose the SnakeYAML
 * serialization options that Jackson can see, but fails to expose to configuration.
 *
 * @author James Brown
 */
class YAMLFactoryWithCustomGenerator extends YAMLFactory
{
    @Override
    protected YAMLGenerator _createGenerator( Writer out, IOContext ctxt) throws IOException
    {
        return new YAMLGeneratorWithCustomStyle( ctxt, _generatorFeatures, _yamlGeneratorFeatures,
                                                 _quotingChecker, _objectCodec, out, _version );
    }
}
