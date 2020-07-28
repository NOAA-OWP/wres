package wres.io.reading;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.IngestSaver;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.ZippedPIXMLIngest;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * @author Christopher Tubbs
 * Reads source files from an archived and saves their data to the database
 *
 * One-shot save. Closes internal executors at the end of first save() call.
 */
public class ZippedSource extends BasicSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedSource.class);

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ThreadPoolExecutor readerService;
    private final Queue<Future<List<IngestResult>>> tasks = new LinkedList<>();
    // After submitting N tasks, call get() on one task prior to next submission
    private final CountDownLatch startGettingOnTasks;
    private final Queue<URI> savedFiles = new LinkedList<>();
    private final DatabaseLockManager lockManager;

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
        this.addIngestTask(readerService.submit(task));
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
        try
        {
            return issue();
        }
        finally
        {
            this.shutdownNow();
        }
    }

    @Override
    protected Logger getLogger()
    {
        return ZippedSource.LOGGER;
    }

    private Future<List<IngestResult>> getIngestTask()
    {
        return tasks.poll();
    }

	/**
	 * Constructor that sets the filename
     * @param projectConfig the project config causing this ingest
     * @param dataSource details of the data source
     * @param lockManager The lock manager to use.
	 */

    ZippedSource( SystemSettings systemSettings,
                  Database database,
                  DataSources dataSourcesCache,
                  Features featuresCache,
                  Variables variablesCache,
                  Ensembles ensemblesCache,
                  MeasurementUnits measurementUnitsCache,
                  ProjectConfig projectConfig,
                  DataSource dataSource,
                  DatabaseLockManager lockManager )
    {
        super( projectConfig,
               dataSource );
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.lockManager = lockManager;
        this.readerService = createReaderService( systemSettings );
        this.startGettingOnTasks = new CountDownLatch( systemSettings.maximumArchiveThreads() );
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private List<IngestResult> issue()
    {
        List<IngestResult> result = new ArrayList<>();

        TarArchiveEntry archivedSource;

        try ( FileInputStream fileStream = new FileInputStream( this.getAbsoluteFilename() );
              BufferedInputStream bufferedFile = new BufferedInputStream( fileStream );
              GzipCompressorInputStream decompressedFileStream = new GzipCompressorInputStream( bufferedFile );
              TarArchiveInputStream archive = new TarArchiveInputStream( decompressedFileStream ) )
        {
            archivedSource = archive.getNextTarEntry();

            while (archivedSource != null)
            {
                //ProgressMonitor.increment();
                if (archivedSource.isFile())
                {
                    int bytesRead = processFile(archivedSource, archive);

                    // The loop can be broken if the end of the file is reached
                    if (bytesRead == -1)
                    {
                        //ProgressMonitor.completeStep();
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
                //ProgressMonitor.completeStep();
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
                                  + "ZippedSource class (1). See also "
                                  + "Database class? Task: {}", ingestTask );
                }

                ingestTask = this.getIngestTask();
            }

            for ( URI filename : this.savedFiles)
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
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while ingesting a zipped source from {}.",
                         dataSource, ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException | IOException e )
        {
            throw new PreIngestException( "Failed to process a zipped source from "
                                          + dataSource, e );
        }

        LOGGER.debug("Finished parsing '{}'", this.getFilename());
        return Collections.unmodifiableList( result );
    }

    private int processFile(TarArchiveEntry source,
                             TarArchiveInputStream archiveInputStream)
            throws IOException
    {
        int bytesRead = (int)source.getSize();
        URI archivedFileName = this.getFilename().resolve( source.getName() );
        Format sourceType = ReaderFactory.getFiletype( archivedFileName );
        DataSourceConfig.Source originalSource = this.dataSource.getSource();

        byte[] content = new byte[bytesRead];

        try
        {
            bytesRead = archiveInputStream.read( content, 0, content.length );
        }
        catch (EOFException eof)
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
            throw new IngestException( message, eof );
        }

        if ( originalSource == null )
        {
            // Demote to debug or trace if null is known as being a normal,
            // usual occurrence that has no potential impact on anything.
            LOGGER.warn( "'{}' is not being ingested because its data source is null",
                         source );
            return bytesRead;
        }

        DataSource innerDataSource = DataSource.of( dataSource.getSource(),
                                                    dataSource.getContext(),
                                                    dataSource.getLinks(),
                                                    archivedFileName );

        String message = "The file '{}' will now be ingested as a set of ";
        if (ConfigHelper.isForecast( this.getDataSource().getContext() ))
        {
            message += "forecasts.";
        }
        else
        {
            message += "observations.";
        }
        LOGGER.debug( message, archivedFileName );

        WRESCallable<List<IngestResult>> ingest;

        if ( sourceType == Format.PI_XML )
        {
            ingest = new ZippedPIXMLIngest( this.getSystemSettings(),
                                            this.getDatabase(),
                                            this.getDataSourcesCache(),
                                            this.getFeaturesCache(),
                                            this.getVariablesCache(),
                                            this.getEnsemblesCache(),
                                            this.getMeasurementUnitsCache(),
                                            this.getProjectConfig(),
                                            innerDataSource,
                                            content,
                                            this.getLockManager() );
            ingest.setOnComplete( ProgressMonitor.onThreadCompleteHandler());
            ProgressMonitor.increment();
            this.addIngestTask(ingest);
        }
        else
        {
            File tempFile = File.createTempFile( "wres_zipped_source", source.getName() );

            try ( FileOutputStream stream = new FileOutputStream( tempFile ) )
            {
                stream.write(content);
                URI tempFileLocation = tempFile.toURI();
                this.savedFiles.add( tempFileLocation );

                ProgressMonitor.increment();
                Callable<List<IngestResult>> task =
                        IngestSaver.createTask()
                                   .withSystemSettings( this.getSystemSettings() )
                                   .withDatabase( this.getDatabase() )
                                   .withDataSourcesCache( this.getDataSourcesCache() )
                                   .withFeaturesCache( this.getFeaturesCache() )
                                   .withVariablesCache( this.getVariablesCache() )
                                   .withEnsemblesCache( this.getEnsemblesCache() )
                                   .withMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                                   .withProject( this.getProjectConfig() )
                                   .withDataSource( innerDataSource )
                                   .withoutHash()
                                   .withProgressMonitoring()
                                   .build();
                this.addIngestTask(task);
            }
        }

        return bytesRead;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
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
}
