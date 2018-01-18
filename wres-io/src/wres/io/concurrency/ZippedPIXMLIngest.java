package wres.io.concurrency;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.fews.PIXMLReader;
import wres.util.Strings;

/**
 * Created by ctubbs on 7/19/17.
 */
public final class ZippedPIXMLIngest extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedPIXMLIngest.class);

    private final String fileName;
    private final byte[] content;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final List<Feature> specifiedFeatures;
    private final ProjectConfig projectConfig;

    public ZippedPIXMLIngest ( final String fileName,
                               final byte[] content,
                               final DataSourceConfig dataSourceConfig,
                               final DataSourceConfig.Source sourceConfig,
                               final List<Feature> specifiedFeatures,
                               final ProjectConfig projectConfig )
    {
        this.fileName = fileName;
        this.content = content;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.specifiedFeatures = specifiedFeatures;
        this.projectConfig = projectConfig;
    }

    @Override
    public List<IngestResult> execute() throws IOException
    {
        List<IngestResult> result = new ArrayList<>( 1 );
        boolean wasFoundInCache;

        try
        {
            String hash = Strings.getMD5Checksum( content );

            if ( !DataSources.hasSource(hash))
            {
                try (InputStream input = new ByteArrayInputStream(this.content))
                {
                    PIXMLReader reader = new PIXMLReader(this.fileName,
                                                         input,
                                                         hash );

                    reader.setSpecifiedFeatures(this.specifiedFeatures);
                    reader.setDataSourceConfig(this.dataSourceConfig);
                    reader.setSourceConfig( this.sourceConfig );
                    reader.parse();
                    wasFoundInCache = false;
                }
            }
            else
            {
                wasFoundInCache = true;
            }

            IngestResult ingestResult = IngestResult.from( this.projectConfig,
                                                           this.dataSourceConfig,
                                                           hash,
                                                           wasFoundInCache );
            result.add( ingestResult );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to ingest", se );
        }

        return Collections.unmodifiableList( result );
    }

    @Override
    protected Logger getLogger()
    {
        return ZippedPIXMLIngest.LOGGER;
    }

}
