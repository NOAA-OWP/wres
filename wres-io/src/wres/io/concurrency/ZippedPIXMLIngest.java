package wres.io.concurrency;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.fews.PIXMLReader;
import wres.system.DatabaseLockManager;
import wres.util.Strings;

/**
 * Created by ctubbs on 7/19/17.
 */
public final class ZippedPIXMLIngest extends WRESCallable<List<IngestResult>>
{
    /**
     * To track whether something was a temp file or not, look for this prefix.
     */
    public static final String TEMP_FILE_PREFIX = "wres_zipped_source_";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedPIXMLIngest.class);

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final byte[] content;
    private final DatabaseLockManager lockManager;

    public ZippedPIXMLIngest ( ProjectConfig projectConfig,
                               DataSource dataSource,
                               byte[] content,
                               DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.content = Arrays.copyOf( content, content.length );
        this.lockManager = lockManager;
    }

    @Override
    public List<IngestResult> execute() throws IOException
    {
        List<IngestResult> result = new ArrayList<>( 1 );
        boolean anotherTaskInChargeOfIngest;
        boolean ingestFullyCompleted;

        String hash = Strings.getMD5Checksum( content );

        // This is a fake uri during first ingest attempt as the bytes are in M.
        URI uri = this.dataSource.getUri();

        try
        {
            if ( !DataSources.hasSource(hash))
            {
                try (InputStream input = new ByteArrayInputStream(this.content))
                {
                    PIXMLReader reader = new PIXMLReader( uri,
                                                          input,
                                                          hash,
                                                          this.lockManager );
                    reader.setDataSourceConfig( this.dataSource.getContext() );
                    reader.setSourceConfig( this.dataSource.getSource() );
                    reader.parse();
                    anotherTaskInChargeOfIngest = !reader.inChargeOfIngest();
                    ingestFullyCompleted = reader.ingestFullyCompleted();
                }
            }
            else
            {
                anotherTaskInChargeOfIngest = true;
                SourceDetails
                        sourceDetails = DataSources.getExistingSource( hash );
                SourceCompletedDetails completedDetails =
                        new SourceCompletedDetails( sourceDetails );
                ingestFullyCompleted = completedDetails.wasCompleted();
            }

            // In the situation where another task has not fully completed
            // ingest of this (inside-zip) source, when we retry this source,
            // it will not be found because the contents will be lost.
            // We could modify DataSource to optionally have a byte[] or we can
            // save it as a temp file here, to be removed after successfully
            // confirming ingest.
            if ( anotherTaskInChargeOfIngest && !ingestFullyCompleted )
            {
                // If we don't save with extension .xml, the retry will think it
                // is invalid.
                File tempFile = File.createTempFile( TEMP_FILE_PREFIX, ".xml" );

                try ( FileOutputStream stream = new FileOutputStream( tempFile ) )
                {
                    stream.write( content );
                    uri = tempFile.toURI();
                }
            }

            // If uri changed, it needs to be reported in the results in case
            // of retry.
            DataSource resultingDataSource = DataSource.of( this.dataSource.getSource(),
                                                            this.dataSource.getContext(),
                                                            this.dataSource.getLinks(),
                                                            uri );

            IngestResult ingestResult = IngestResult.from( this.projectConfig,
                                                           resultingDataSource,
                                                           hash,
                                                           anotherTaskInChargeOfIngest,
                                                           !ingestFullyCompleted );
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
