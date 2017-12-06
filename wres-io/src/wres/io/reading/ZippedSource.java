package wres.io.reading;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.ZippedPIXMLIngest;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 * Reads source files from an archived and saves their data to the database
 */
@Internal(exclusivePackage = "wres.io")
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
        this.tasks.add(result);
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
    @Internal(exclusivePackage = "wres.io")
    public ZippedSource ( ProjectConfig projectConfig,
                          String filename )
	{
        super( projectConfig );
	    this.setFilename(filename);
	    this.directoryPath = Paths.get(filename).toAbsolutePath().getParent().toString();
    }

	@Override
    public List<IngestResult> saveForecast() throws IOException
    {
	    return issue( true );
	}

	@Override
    public List<IngestResult> saveObservation() throws IOException
    {
	    return issue( false );
	}

    private List<IngestResult> issue(boolean isForecast)
    {
        List<IngestResult> result = new ArrayList<>();

        FileInputStream fileStream = null;
        BufferedInputStream bufferedFile = null;
        GzipCompressorInputStream decompressedFileStream = null;
        TarArchiveInputStream archive = null;
        TarArchiveEntry archivedSource;

        try
        {
            fileStream = new FileInputStream(this.getAbsoluteFilename());
            bufferedFile = new BufferedInputStream(fileStream);
            decompressedFileStream = new GzipCompressorInputStream(bufferedFile);
            archive = new TarArchiveInputStream(decompressedFileStream);

            archivedSource = archive.getNextTarEntry();

            while (archivedSource != null)
            {
                ProgressMonitor.increment();
                if (archivedSource.isFile())
                {
                    processFile(archivedSource, archive, isForecast);
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
                boolean fileRemoved = new File(filename).delete();
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
            throw new RuntimeException( "Failure", e );
        }
        finally
        {
            if (archive != null)
            {
                try
                {
                    archive.close();
                }
                catch (IOException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (decompressedFileStream != null)
            {
                try
                {
                    decompressedFileStream.close();
                }
                catch (IOException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (bufferedFile != null)
            {
                try
                {
                    bufferedFile.close();
                }
                catch (IOException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (fileStream != null)
            {
                try {
                    fileStream.close();
                }
                catch (IOException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
        }

        return Collections.unmodifiableList( result );
    }

    private void processFile(TarArchiveEntry source,
                             TarArchiveInputStream archiveInputStream,
                             boolean isForecast)
            throws IOException
    {
        String archivedFileName = Paths.get(this.directoryPath, source.getName()).toString();
        SourceType sourceType = ReaderFactory.getFiletype(archivedFileName);
        DataSourceConfig.Source
                originalSource = ConfigHelper.findDataSourceByFilename( this.getDataSourceConfig(), this.getAbsoluteFilename() );

        byte[] content = new byte[(int)source.getSize()];

        try
        {
            archiveInputStream.read( content, 0, content.length );
        }
        catch (EOFException eof)
        {
            String message = "The end of the archive entry for: {} was ";
            message += "reached. Data within the archive may have been ";
            message += "cut off when creating or moving the archive.";
            LOGGER.error(message, archivedFileName);
            LOGGER.error(Strings.getStackTrace( eof ));
            return;
        }

        if (originalSource == null || !this.shouldIngest( archivedFileName, originalSource, content ) )
        {
            LOGGER.trace( "'{}' is not being ingested because its data already exists within the database." );
            return;
        }
        else
        {
            String message = "The file '{}' will now be ingested as a set of ";
            if (isForecast)
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

        if (sourceType == SourceType.PI_XML)
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

                if (isForecast)
                {
                    ingest = new ForecastSaver(archivedFileName,
                                               this.getProjectConfig(),
                                               this.getDataSourceConfig(),
                                               originalSource,
                                               this.getSpecifiedFeatures());
                }
                else
                {
                    ingest = new ObservationSaver(archivedFileName,
                                                  this.getProjectConfig(),
                                                  this.getDataSourceConfig(),
                                                  originalSource,
                                                  this.getSpecifiedFeatures());
                }

                ingest.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                ProgressMonitor.increment();
                Future<List<IngestResult>> task = Executor.submit( ingest );
                this.addIngestTask(task);
            }
        }
    }

    private final String directoryPath;
}
