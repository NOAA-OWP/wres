package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.reading.fews.PIXMLReader;
import wres.util.Internal;
import wres.util.Strings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by ctubbs on 7/19/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class ZippedPIXMLIngest extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedPIXMLIngest.class);

    private final byte[] content;
    private final String fileName;
    private final List<Conditions.Feature> specifiedFeatures;
    private final DataSourceConfig dataSourceConfig;

    @Internal(exclusivePackage = "wres.io")
    public ZippedPIXMLIngest (final String fileName,
                              final byte[] content,
                              final DataSourceConfig dataSourceConfig,
                              final List<Conditions.Feature> specifiedFeatures)
    {
        this.fileName = fileName;
        this.content = content;
        this.dataSourceConfig = dataSourceConfig;
        this.specifiedFeatures = specifiedFeatures;
    }

    @Override
    public void execute ()
    {
        try (InputStream input = new ByteArrayInputStream(this.content))
        {
            PIXMLReader reader = new PIXMLReader(this.fileName, input, ConfigHelper.isForecast(this.dataSourceConfig));
            reader.setSpecifiedFeatures(this.specifiedFeatures);
            reader.setDataSourceConfig(this.dataSourceConfig);
            reader.parse();
        }
        catch (IOException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }
    }

    @Override
    protected String getTaskName () {
        String type = " forecast";

        if (!ConfigHelper.isForecast(this.dataSourceConfig))
        {
            type = "n observation";
        }

        return "ZippedPIXMLIngest - Saving content from " + this.fileName + " as a" + type;
    }

    @Override
    protected Logger getLogger () {
        return ZippedPIXMLIngest.LOGGER;
    }
}
