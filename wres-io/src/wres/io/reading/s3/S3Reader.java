package wres.io.reading.s3;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.IngestSaver;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

public abstract class S3Reader extends BasicSource
{
    /**
     * Get a reader that may be used to access an object store
     * @param projectConfig The configuration for a project
     * @return An object store reader
     */
    public static S3Reader getReader(ProjectConfig projectConfig)
    {
        // We only have one reader; add more when more are created.
        return new NWCALReader( projectConfig );
    }

    S3Reader (ProjectConfig projectConfig)
    {
        super(projectConfig);
    }

    @Override
    public List<IngestResult> save() throws IOException
    {
        List<IngestResult> results = new ArrayList<>(  );
        List<Future<List<IngestResult>>> ingests = new ArrayList<>();
        List<ETagKey> toIngest = new ArrayList<>(  );
        Collection<ETagKey> ingestableObjects = this.getIngestableObjects();

        if (ingestableObjects.isEmpty())
        {
            LeftOrRightOrBaseline evaluationSide = ConfigHelper.getLeftOrRightOrBaseline(
                    this.getProjectConfig(),
                    this.getDataSourceConfig()
            );

            throw new IOException( "No objects could be found in the object store for the " +
                                   evaluationSide.value() +
                                   " side of the input." );
        }

        for (ETagKey tagAndKey : this.getIngestableObjects())
        {
            try
            {
                // TODO: This needs to check if it needs to be stored locally, not if it is gridded
                boolean isVector = true;

                SourceDetails source = DataSources.getExistingSource( tagAndKey.getEtag() );
                boolean fileExists = source != null;

                if (source != null)
                {
                    isVector = source.getIsPointData();
                }

                if (!isVector)
                {
                    fileExists = Files.exists( ConfigHelper.getStoredNetcdfPath(tagAndKey.getKey()));
                }

                if ( fileExists )
                {
                    ingests.add(
                            IngestResult.fakeFutureSingleItemListFrom(
                                    this.getProjectConfig(),
                                    this.getDataSourceConfig(),
                                    tagAndKey.getKey(),
                                    tagAndKey.getEtag()
                            )
                    );
                }
                else
                {
                    toIngest.add( tagAndKey );
                }
            }
            catch ( SQLException e )
            {
                throw new IOException(
                        "The database could not be queried to see if metadata for " +
                        tagAndKey.getKey() + " already exists.",
                        e);
            }
        }

        for (ETagKey tagKey : toIngest)
        {

            WRESCallable<List<IngestResult>> saver = IngestSaver.createTask()
                                                                .withFilePath( this.getKeyURL( tagKey.getKey() ) )
                                                                .withProject( this.getProjectConfig() )
                                                                .withDataSourceConfig( this.getDataSourceConfig() )
                                                                .withHash( tagKey.getEtag() )
                                                                .isRemote()
                                                                .withProgressMonitoring()
                                                                .build();

            ingests.add( Database.ingest(saver));
        }

        for (Future<List<IngestResult>> ingest : ingests)
        {
            try
            {
                results.addAll( ingest.get() );
            }
            catch ( InterruptedException e )
            {
                this.getLogger().warn( "S3 Data ingest was interrupted.", e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "S3 Data could not be ingested.", e );
            }
        }

        return results;
    }

    /**
     * @return a collection of object metadata from the store that might need to be ingested
     */
    private Collection<ETagKey> getIngestableObjects()
    {
        AmazonS3 connection = this.getConnection();
        List<ETagKey> ingestableObjects = new ArrayList<>(  );
        for (PrefixPattern prefixPattern : this.getPrefixPatterns())
        {
            ListObjectsRequest request = new ListObjectsRequest(  );
            request.setPrefix( prefixPattern.getPrefix() );
            request.setBucketName( this.getBucketName() );
            request.setMaxKeys( this.getMaxKeyCount() );

            final PathMatcher matcher = FileSystems.getDefault()
                                                   .getPathMatcher( "glob:" + prefixPattern.getPattern() );

            ObjectListing s3Objects = connection.listObjects( request );

            do
            {
                s3Objects.getObjectSummaries().forEach( summary -> {
                    if (matcher.matches( Paths.get(summary.getKey()) ))
                    {
                        ingestableObjects.add(new ETagKey( summary.getETag(), summary.getKey() ));
                    }
                } );
                s3Objects = connection.listNextBatchOfObjects( s3Objects );
            } while ( !s3Objects.getObjectSummaries().isEmpty() );
        }

        return ingestableObjects;
    }

    /**
     * Generate the full URL to the object in the object store
     * @param key The key of the object in the object store
     * @return The URL to the file in the object store
     */
    abstract String getKeyURL(final String key);

    /**
     * Gets the maximum number of object records that may be retrieved at once
     * <br></br><br></br>
     * A larger number results in less requests being made, though it comes at the
     * cost of more server activity and a larger data transfer.
     * @return The maximum number of object records to retrieve
     */
    abstract int getMaxKeyCount();

    /**
     * @return an object to step through that will provide all possible prefixes and patterns used to look for data
     */
    abstract Iterable<PrefixPattern> getPrefixPatterns();

    /**
     * Creates a connection that can be used to get, interrogate, or modify an S3 object store
     * @return A connection to an object store
     */
    private AmazonS3 getConnection()
    {
        // CT: I have no clue what this does (other than make it work)
        ClientConfiguration clientConfig = new ClientConfiguration(  ).withSignerOverride( "S3SignerType" );

        return AmazonS3ClientBuilder.standard()
                                    .withCredentials( this.getCredentials() )
                                    .withEndpointConfiguration( this.getEndpoint() )
                                    .withPathStyleAccessEnabled( this.shouldUsePathStyle() )
                                    .withClientConfiguration( clientConfig )
                                    .build();
    }

    /**
     * Creates the configuration used to specify how to access an object store
     * @return Information about the S3 endpoint that provides access to the object store
     */
    abstract AwsClientBuilder.EndpointConfiguration getEndpoint();

    /**
     * Gets the name of the bucket containing the data of interest
     * @return The name of the bucket to access
     */
    abstract String getBucketName();

    /**
     * Gets the required credentials needed to access a store with the required permissions.
     * <br></br><br></br>
     * Some stores that allow public reading don't require any sort of real credentials; you can get
     * objects anonymously. If you want to modify objects in the store, however, you generally need
     * to offer credentials to prove that you're allowed to make said changes. To do so, you'll need
     * to supply basic credentials at the very least.
     * @return The credentials for the object store
     */
    abstract AWSCredentialsProvider getCredentials();

    /**
     * Decides whether the connection should use the path style or not.
     *
     * By default, buckets on S3 servers are accessed via a pattern like
     * "${BUCKET_NAME}.example.com/whatever/content.ext", while others may follow a pattern more like
     * "example.com/${BUCKET_NAME}/whatever/content.ext". Accessing the latter requires that path style be turned on.
     * @return Whether or not to use S3 request path style calculation
     */
    abstract boolean shouldUsePathStyle();
}
