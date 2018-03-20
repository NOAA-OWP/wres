package wres.io.reading.waterml;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.StringJoiner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;

/**
 * Prototype reader for WaterML JSON data. With some extra work in the WaterML
 * objects, XML may be supported as well.
 */
public class WaterMLReader extends BasicSource
{
    private static final DeserializationFeature DESERIALIZATION_FEATURE =
            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

    protected WaterMLReader( ProjectConfig projectConfig, String filename )
    {
        super( projectConfig );
        this.setFilename( filename );
        this.setHash();
    }

    @Override
    public List<IngestResult> save() throws IOException
    {
        boolean wasFoundInCache = false;

        try
        {
            if (!DataSources.hasSource(this.getHash()))
            {
                Response waterml = this.load();
            }
            else
            {
                wasFoundInCache = true;
            }
        }
        catch ( SQLException | IOException e )
        {
            String message = "While saving the";

            if ( ConfigHelper.isForecast( this.getDataSourceConfig() ))
            {
                message += " forecast ";
            }
            else
            {
                message += " observation ";
            }

            message += "from source '" +
                       this.getAbsoluteFilename() +
                       "', encountered an issue.";

            throw new IngestException( message, e );
        }

        return IngestResult.singleItemListFrom(
                this.getProjectConfig(),
                this.getDataSourceConfig(),
                this.getHash(),
                wasFoundInCache
        );
    }

    private Response load() throws IOException
    {
        Response watermlResponse;
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        try ( BufferedReader reader = Files.newBufferedReader( Paths.get(this.getAbsoluteFilename())))
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper = mapper.configure( DESERIALIZATION_FEATURE, false );

            reader.lines().forEach( joiner::add );

            String rawSource = joiner.toString();

            watermlResponse = mapper.readValue(rawSource, new TypeReference<Response>(){});
        }
        catch ( IOException e )
        {
            throw new IOException( "The WaterML file could not be loaded for reading.", e );
        }

        return watermlResponse;
    }
}
