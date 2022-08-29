package wres.io.reading;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.reading.DataSource.DataDisposition.TARBALL;
import static wres.io.reading.DataSource.DataDisposition.UNKNOWN;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.ZippedPIXMLIngest;
import wres.io.data.caching.DatabaseCaches;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * @author Christopher Tubbs
 * Reads source files from an archive and saves their data to the database
 *
 * One-shot save. Closes internal executors at the end of first save() call.
 */
public class ZippedSource implements Source 
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedSource.class);

    /** After ingest, delete files starting with this prefix. */
    public static final String TEMP_FILE_PREFIX = "wres_zipped_source_";

    private final SystemSettings systemSettings;
    private final Database database;
    private final DatabaseCaches caches;
    private final ThreadPoolExecutor readerService;
    private final Queue<Future<List<IngestResult>>> tasks = new LinkedList<>();
    // After submitting N tasks, call get() on one task prior to next submission
    private final CountDownLatch startGettingOnTasks;
    private final Queue<URI> savedFiles = new LinkedList<>();
    private final DatabaseLockManager lockManager;
    private final TimeSeriesIngester timeSeriesIngester;
    private final String absoluteFileName;
    private final DataSource dataSource;
    private final ProjectConfig projectConfig;

    private ThreadPoolExecutor createReaderService( SystemSettings systemSettings )
    {
        ThreadFactory zippedSourceFactory = new BasicThreadFactory.Builder()
                .namingPattern( "ZippedSource Ingest %d" )
                .build();
        BlockingQueue<Runnable>
                zippedSourceQueue = new ArrayBlockingQueue<>( systemSettings.maximumArchiveThreads() );
        ThreadPoolExecutor executor = new ThreadPoolExecutor( systemSettings.maximumArchiveThreads(),
                                                              systemSettings.maximumArchiveThreads(),
                                                              systemSettings.poolObjectLifespan(),
                                                              TimeUnit.MILLISECONDS,
                                                              zippedSourceQueue,
                                                              zippedSourceFactory );

        // Abort policy, but ought never be hit because we throttle submission
        // of tasks to the count of maximum threads and wait to submit another
        // until after one has get() return.
        executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );

        return executor;
    }

    private void addIngestTask ( Callable<List<IngestResult>> task )
    {
        this.addIngestTask( this.readerService.submit(task) );
    }

    private void addIngestTask( Future<List<IngestResult>> result )
    {
        LOGGER.trace( "Added Future {} to this.tasks", result );
        this.tasks.add(result);
        this.startGettingOnTasks.countDown();
    }

    @Override
    public List<IngestResult> save()
    {
        DataSource source = this.getDataSource();

        try
        {
            return issue( source );
        }
        finally
        {
            this.shutdownNow();
        }
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }
    
    private DatabaseCaches getCaches()
    {
        return this.caches;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }
    
    private Future<List<IngestResult>> getIngestTask()
    {
        return tasks.poll();
    }

    /**
     * @return the file name
     */
    
    private URI getFileName()
    {
        return this.getDataSource()
                   .getUri();
    }
    
    /**
     * @return the data source
     */
    private DataSource getDataSource()
    {
        return this.dataSource;
    }
    
    /**
     * @return the project declaration
     */
    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }
    
    /**
     * @return The absolute path of the file to read
     */
    private String getAbsoluteFilename()
    {
        return this.absoluteFileName;
    }
    
    private List<IngestResult> issue( DataSource dataSource )
    {
        List<IngestResult> result;
        String nameInside = "";

        try ( FileInputStream fileStream = new FileInputStream( this.getAbsoluteFilename() );
              GzipCompressorInputStream decompressedFileStream = new GzipCompressorInputStream( fileStream );
              BufferedInputStream bufferedStream = new BufferedInputStream( decompressedFileStream ) )
        {
            nameInside = decompressedFileStream.getMetaData()
                                               .getFilename();
            URI mashupUri = URI.create( this.getFileName() + "/" + nameInside );
            DataSource.DataDisposition disposition = DataSource.detectFormat( bufferedStream,
                                                                              mashupUri );
            // TODO: Split tar code out of this class for plain tar processing
            if ( disposition.equals( TARBALL ) )
            {
                result = readFromTarStream( bufferedStream,
                                            mashupUri );
            }
            else if ( disposition.equals( UNKNOWN ) )
            {
                LOGGER.warn( "Skipping unknown gzipped data from inside {}",
                             dataSource );
                result = Collections.emptyList();
            }
            else
            {
                // Found some other supported type of document, ingest it.
                byte[] content = bufferedStream.readAllBytes();

                // Until all simple readers take @InputStream@, make temp file.
                URI tempFileLocation = this.writeTempFile( nameInside, content );
                DataSource decompressedSource = DataSource.of( disposition,
                                                               dataSource.getSource(),
                                                               dataSource.getContext(),
                                                               dataSource.getLinks(),
                                                               tempFileLocation,
                                                               dataSource.getLeftOrRightOrBaseline() );

                Source reader = ReaderFactory.getReader( this.getTimeSeriesIngester(),
                                                         this.getSystemSettings(),
                                                         this.getDatabase(),
                                                         this.getCaches(),
                                                         this.getProjectConfig(),
                                                         decompressedSource,
                                                         this.getLockManager() );

                result = reader.save();
            }
        }
        catch ( IOException | RuntimeException e )
        {
            if ( nameInside == null )
            {
                nameInside = "";
            }

            throw new PreIngestException( "Failed to process a gzipped source from '"
                                          + this.getDataSource() + "': '"
                                          + nameInside, e );
        }

        LOGGER.debug("Finished parsing '{}'", this.getFileName());
        return result;
    }

    private List<IngestResult> readFromTarStream( InputStream inputStream,
                                                  URI tarName )
    {
        List<IngestResult> result = new ArrayList<>();

        try ( TarArchiveInputStream archive = new TarArchiveInputStream( inputStream );
              BufferedInputStream bufferedArchive = new BufferedInputStream( archive ) )
        {
            TarArchiveEntry archivedSource = archive.getNextTarEntry();

            while ( archivedSource != null )
            {
                if ( archivedSource.isFile() )
                {
                    int bytesRead = processTarEntry( archivedSource,
                                                     bufferedArchive,
                                                     tarName );

                    // The loop can be broken if the end of the file is reached
                    if ( bytesRead == -1 )
                    {
                        break;
                    }
                }

                if ( this.startGettingOnTasks.await( 0, TimeUnit.MILLISECONDS ) )
                {
                    // Ensure that exceptions propagate by calling get after
                    // the first N tasks have been submitted, where N is the
                    // count of simultaneous tasks to be processed.
                    Future<List<IngestResult>> earlierTask = this.getIngestTask();
                    List<IngestResult> zippedResult = earlierTask.get();
                    result.addAll( zippedResult );
                }

                archivedSource = archive.getNextTarEntry();
           }

            Future<List<IngestResult>> ingestTask = this.getIngestTask();

            while (ingestTask != null)
            {
                List<IngestResult> innerResult = ingestTask.get();

                if ( innerResult != null )
                {
                    result.addAll( innerResult );
                }
                else if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "A null value was returned in the "
                                  + "ZippedSource class (1). Task: {}",
                                  ingestTask );
                }

                ingestTask = this.getIngestTask();
            }

            for ( URI filename : this.savedFiles )
            {
                Path path = Paths.get( filename );
                boolean fileRemoved = Files.deleteIfExists( path );

                if (!fileRemoved)
                {
                    LOGGER.debug( "The file '{}' could not be removed after " +
                                  "extracting it for reading.",
                                  filename );
                }
            }
        }
        catch ( ExecutionException | IOException e )
        {
            throw new PreIngestException( "Failed to process a tarball "
                                          + tarName + " within source "
                                          + this.getDataSource(), e );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while ingesting a tarball {} from {}.",
                         tarName, this.getDataSource(), ie );
            Thread.currentThread().interrupt();
        }

        return Collections.unmodifiableList( result );
    }


    private int processTarEntry( TarArchiveEntry source,
                                 InputStream archiveInputStream,
                                 URI tarName )
    {
        int expectedByteCount = (int) source.getSize();
        int bytesRead = 0;
        URI archivedFileName = URI.create( tarName + "/" + source.getName() );
        DataSource.DataDisposition disposition = DataSource.detectFormat( archiveInputStream,
                                                                          archivedFileName );

        if ( disposition == UNKNOWN )
        {
            LOGGER.warn( "Skipping unknown data type in {}", archivedFileName );
            return DataSource.DETECTION_BYTES;
        }

        DataSourceConfig.Source originalSource = this.getDataSource()
                                                     .getSource();

        byte[] content = new byte[expectedByteCount];

        try
        {
            bytesRead = archiveInputStream.read( content, 0, content.length );
            LOGGER.debug( "Read {} bytes of an expected {} bytes from {}.", bytesRead, expectedByteCount, tarName );
        }
        catch ( EOFException eof )
        {
            String message = "The end of the archive entry for: '"
                             + archivedFileName + "' was "
                             + "reached. Data within the archive may have been "
                             + "cut off when creating or moving the archive.";
            // Regarding whether to log-and-continue or to propagate and stop:
            // On the one hand, it might happen on a file that is intended for
            // ingest and therefore can affect primary outputs if not ingested.
            // On the other hand, this is a very specific case of a particular
            // exception when reading a particular file within an archive.
            // But then again, shouldn't the user be notified that the archive
            // is corrupt? And if so, stopping (propagating here) is the
            // clearest way to notify the user of a corrupt input file.
            throw new PreIngestException( message, eof );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read from '"
                                          + archivedFileName + "'", ioe );
        }

        if ( originalSource == null )
        {
            // Demote to debug or trace if null is known as being a normal,
            // usual occurrence that has no potential impact on anything.
            LOGGER.warn( "'{}' is not being ingested because its data source is null",
                         source );
            return bytesRead;
        }

        LOGGER.debug( "The file '{}' will now be read.", archivedFileName );
        WRESCallable<List<IngestResult>> ingest;

        if ( disposition == DataDisposition.XML_PI_TIMESERIES || disposition == DataDisposition.XML_FI_TIMESERIES )
        {
            DataSource innerDataSource = DataSource.of( disposition,
                                                        this.getDataSource()
                                                            .getSource(),
                                                        this.getDataSource()
                                                            .getContext(),
                                                        this.getDataSource()
                                                            .getLinks(),
                                                        archivedFileName,
                                                        this.getDataSource()
                                                            .getLeftOrRightOrBaseline() );
            ingest = new ZippedPIXMLIngest( this.getTimeSeriesIngester(),
                                            innerDataSource,
                                            content );
            ingest.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            ProgressMonitor.increment();
            this.addIngestTask( ingest );
        }
        else
        {
            URI tempFileLocation = this.writeTempFile( archivedFileName.getPath(),
                                                       content );
            this.savedFiles.add( tempFileLocation );
            DataSource innerDataSource = DataSource.of( disposition,
                                                        this.getDataSource()
                                                            .getSource(),
                                                        this.getDataSource()
                                                            .getContext(),
                                                        this.getDataSource()
                                                            .getLinks(),
                                                        tempFileLocation,
                                                        this.getDataSource()
                                                            .getLeftOrRightOrBaseline() );
            ProgressMonitor.increment();

            Source reader = ReaderFactory.getReader( this.getTimeSeriesIngester(),
                                                     this.getSystemSettings(),
                                                     this.getDatabase(),
                                                     this.getCaches(),
                                                     this.getProjectConfig(),
                                                     innerDataSource,
                                                     this.getLockManager() );
            
            this.addIngestTask( () -> reader.save() );
        }

        return bytesRead;
    }


    /**
     * Write to a temporary file (yuck, but sometimes not the worst way).
     * @param endOfName The name to attach to the temp file prefix.
     * @param bytesToWrite The raw data to write.
     * @return the URI of the temp file written.
     */

    private URI writeTempFile( String endOfName,
                               byte[] bytesToWrite )
    {
        File tempFile;

        try
        {            
            tempFile = File.createTempFile( TEMP_FILE_PREFIX,
                                            "_" + endOfName );
            LOGGER.debug( "Created temporary file {}.", endOfName );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to create temporary file for in-archive data named '"
                                          + endOfName + "'", ioe );
        }

        try ( FileOutputStream stream = new FileOutputStream( tempFile ) )
        {
            stream.write( bytesToWrite );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to write to temporary file '"
                                          + tempFile
                                          + "' for in-archive data named '"
                                          + endOfName + "'", ioe );
        }

        return tempFile.toURI();
    }

    private void shutdownNow()
    {
        List<Runnable> incompleteTasks = this.readerService.shutdownNow();

        // An exception should already be propagating if the following is true.
        if ( !incompleteTasks.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Failed to complete {} ingest tasks associated with {}",
                         incompleteTasks.size(),
                         this.getDataSource().getUri() );
        }
    }
    
    /**
     * Constructor.
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings the system settings
     * @param database the database
     * @param caches the caches/ORMs
     * @param projectConfig the project declaration
     * @param dataSource the data source
     * @param lockManager the lock manager
     */

    ZippedSource( TimeSeriesIngester timeSeriesIngester,
                  SystemSettings systemSettings,
                  Database database,
                  DatabaseCaches caches,
                  ProjectConfig projectConfig,
                  DataSource dataSource,
                  DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        
        if( !systemSettings.isInMemory() )
        {
            Objects.requireNonNull( database );
            Objects.requireNonNull( caches );
            Objects.requireNonNull( lockManager );
        }
        
        this.systemSettings = systemSettings;
        this.database = database;
        this.caches = caches;
        this.lockManager = lockManager;
        this.readerService = createReaderService( systemSettings );
        this.startGettingOnTasks = new CountDownLatch( systemSettings.maximumArchiveThreads() );
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
        this.projectConfig = projectConfig;
        this.absoluteFileName = Paths.get( this.getFileName() )
                                               .toAbsolutePath()
                                               .toString();
    }
}
