package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.reading.XMLReader;
import wres.io.reading.fews.PIXMLReader;
import wres.util.Strings;

import java.io.File;
import java.io.IOException;

/**
 * Created by ctubbs on 7/19/17.
 */
public final class ZippedSourceSaver extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedSourceSaver.class);

    private final File source;
    private final boolean isForecast;

    public ZippedSourceSaver(final File source, final boolean isForecast)
    {
        this.source = source;
        this.isForecast = isForecast;
    }

    @Override
    public void execute ()
    {
        try
        {
            XMLReader reader = new PIXMLReader(this.source, this.isForecast);
            reader.parse();
        }
        catch (IOException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }
    }

    @Override
    protected String getTaskName () {
        String type = "Forecast";

        if (!this.isForecast)
        {
            type = "Observation";
        }

        return "ZippedSourceSaver - Saving" + source.getName() + " in " + source.getAbsolutePath() + " as a " + type;
    }

    @Override
    protected Logger getLogger () {
        return ZippedSourceSaver.LOGGER;
    }
}
