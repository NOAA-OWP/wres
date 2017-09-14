package wres.io.concurrency;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.fews.PIXMLReader;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Created by ctubbs on 7/19/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class ZippedPIXMLIngest extends WRESRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedPIXMLIngest.class);

    private final byte[] content;
    private final String fileName;
    private final List<Feature> specifiedFeatures;
    private final DataSourceConfig dataSourceConfig;
    private final Future<String> futureHash;
    private final ProjectDetails projectDetails;

    @Internal(exclusivePackage = "wres.io")
    public ZippedPIXMLIngest ( final String fileName,
                               final byte[] content,
                               final DataSourceConfig dataSourceConfig,
                               final List<Feature> specifiedFeatures,
                               final ProjectDetails projectDetails)
    {
        this.fileName = fileName;
        this.content = content;
        this.dataSourceConfig = dataSourceConfig;
        this.specifiedFeatures = specifiedFeatures;
        this.projectDetails = projectDetails;

        WRESCallable<String> hasher = new WRESCallable<String>()
        {
            @Override
            protected Logger getLogger()
            {
                return null;
            }

            @Override
            protected String execute() throws Exception
            {
                return Strings.getMD5Checksum( contentToHash );
            }

            private byte[] contentToHash;

            public WRESCallable<String> init(byte[] contentToHash)
            {
                this.contentToHash = contentToHash;
                return this;
            }
        }.init( content );

        this.futureHash = Executor.submit( hasher );
    }

    @Override
    public void execute ()
    {
        try
        {
            String hash = this.futureHash.get();

            if ( !DataSources.hasSource(hash))
            {
                try (InputStream input = new ByteArrayInputStream(this.content))
                {
                    PIXMLReader reader = new PIXMLReader(this.fileName,
                                                         input,
                                                         ConfigHelper.isForecast(this.dataSourceConfig),
                                                         hash,
                                                         this.projectDetails);

                    reader.setSpecifiedFeatures(this.specifiedFeatures);
                    reader.setDataSourceConfig(this.dataSourceConfig);
                    reader.parse();
                }
                catch (IOException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
            else
            {
                this.projectDetails.addSource( hash, this.dataSourceConfig );
            }
        }
        catch ( InterruptedException | ExecutionException | SQLException e )
        {
            LOGGER.error(Strings.getStackTrace( e ));
        }
    }

    @Override
    protected Logger getLogger () {
        return ZippedPIXMLIngest.LOGGER;
    }
}
