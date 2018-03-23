package wres.io.reading;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.IngestSaver;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.ZippedPIXMLIngest;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 * Reads source files from an archived and saves their data to the database
 */
public class ZippedSource extends BasicSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedSource.class);

    private final ExecutorService readerService = createReaderService();

    private ExecutorService createReaderService()
    {
        int threadCount = ((Double)Math.ceil(SystemSettings.maximumThreadCount() / 10F)).intValue();
        threadCount = Math.max(threadCount, 2);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount,
                                                             threadCount,
                                                             SystemSettings.poolObjectLifespan(),
                                                             TimeUnit.MILLISECONDS,
                                                             new ArrayBlockingQueue<>(threadCount));

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

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
    }

    @Override
    public List<IngestResult> save() throws IOException
    {
        return issue();
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

    private final Queue<Future<List<IngestResult>>> tasks = new LinkedList<>();

    private final Queue<String> savedFiles = new LinkedList<>();

	/**
	 * Constructor that sets the filename
     * @param projectConfig the project config causing this ingest
	 * @param filename The name of the source file
	 */
    ZippedSource ( ProjectConfig projectConfig,
                          String filename )
	{
        super( projectConfig );
	    this.setFilename(filename);
	    this.directoryPath = Paths.get(filename).toAbsolutePath().getParent().toString();
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
                ProgressMonitor.increment();
                if (archivedSource.isFile())
                {
                    int bytesRead = processFile(archivedSource, archive);

                    // The loop can be broken if the end of the file is reached
                    if (bytesRead == -1)
                    {
                        ProgressMonitor.completeStep();
                        break;
                    }
                }

                archivedSource = archive.getNextTarEntry();
                ProgressMonitor.completeStep();
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

            ingestTask = Database.getStoredIngestTask();

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
                                  + "ZippedSource class (2). See also "
                                  + "Database class? Task: {}", ingestTask );
                }

                ingestTask = Database.getStoredIngestTask();
            }

            for (String filename : this.savedFiles)
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
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException | IOException e )
        {
            throw new PreIngestException( "Failed to process a zipped source",
                                          e );
        }

        LOGGER.debug("Finished parsing '{}'", this.getFilename());
        return Collections.unmodifiableList( result );
    }

    private int processFile(TarArchiveEntry source,
                             TarArchiveInputStream archiveInputStream)
            throws IOException
    {
        int bytesRead = (int)source.getSize();
        String archivedFileName = Paths.get(this.directoryPath, source.getName()).toString();
        Format sourceType = ReaderFactory.getFiletype( archivedFileName );
        DataSourceConfig.Source
                originalSource = ConfigHelper.findDataSourceByFilename( this.getDataSourceConfig(), this.getAbsoluteFilename() );

        byte[] content = new byte[bytesRead];

        try
        {
            bytesRead = archiveInputStream.read( content, 0, content.length );
        }
        catch (EOFException eof)
        {
            String message = "The end of the archive entry for: {} was ";
            message += "reached. Data within the archive may have been ";
            message += "cut off when creating or moving the archive.";
            LOGGER.warn(message, archivedFileName);
            LOGGER.warn(Strings.getStackTrace( eof ));
            return -1;
        }

        if ( originalSource == null )
        {
            LOGGER.trace( "'{}' is not being ingested because its data source is null", source );
            return bytesRead;
        }

        Pair<Boolean, String> checkIngest;

        try
        {
            checkIngest =
                    this.shouldIngest( archivedFileName, originalSource, content );
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
            throw new IOException( "The database could not be used to evaluate "
                                   + "whether or not '" + archivedFileName +
                                   "' should be ingested.", e );
        }

        if ( !checkIngest.getLeft() )
        {
            LOGGER.trace( "'{}' is not being ingested because was already found", source );

            if (checkIngest.getRight() == null || checkIngest.getRight().isEmpty())
            {
                LOGGER.debug("A file ('{}') is being added to this project "
                             + "despite it not having a hash.", source);
            }

            // Fake a future, return result immediately.
            Future<List<IngestResult>> ingest =
                    IngestResult.fakeFutureSingleItemListFrom( projectConfig,
                                                               dataSourceConfig,
                                                               checkIngest.getRight() );
            this.addIngestTask( ingest );
            return bytesRead;
        }
        else
        {
            String message = "The file '{}' will now be ingested as a set of ";
            if (ConfigHelper.isForecast( this.getDataSourceConfig() ))
            {
                message += "forecasts.";
            }
            else
            {
                message += "observations.";
            }
            LOGGER.debug( message, archivedFileName );
        }

        WRESCallable<List<IngestResult>> ingest;

        if ( sourceType == Format.PI_XML )
        {
            ingest = new ZippedPIXMLIngest(archivedFileName,
                                           content,
                                           this.getDataSourceConfig(),
                                           originalSource,
                                           this.getSpecifiedFeatures(),
                                           this.getProjectConfig() );

            ingest.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            ProgressMonitor.increment();
            this.addIngestTask(ingest);
        }
        else
        {
            try (FileOutputStream stream = new FileOutputStream(archivedFileName))
            {
                stream.write(content);
                this.savedFiles.add(archivedFileName);
                ingest = new IngestSaver(archivedFileName,
                                         this.getProjectConfig(),
                                         this.getDataSourceConfig(),
                                         originalSource,
                                         this.getSpecifiedFeatures());

                ingest.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                ProgressMonitor.increment();
                Future<List<IngestResult>> task = Executor.submit( ingest );
                this.addIngestTask(task);
            }
        }

        return bytesRead;
    }

    private final String directoryPath;
}
