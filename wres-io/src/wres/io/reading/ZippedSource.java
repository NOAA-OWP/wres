package wres.io.reading;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.concurrency.*;
import wres.io.config.SystemSettings;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class ZippedSource extends BasicSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedSource.class);

    private ExecutorService readerService = createReaderService();

    private ExecutorService createReaderService()
    {
        Double threadCount = Math.ceil(SystemSettings.maximumThreadCount() / 10F);
        threadCount = Math.max(threadCount, 2.0F);

        return Executors.newFixedThreadPool(threadCount.intValue());
    }

    private void addIngestTask (Runnable task)
    {
        this.addIngestTask(readerService.submit(task));
    }

    private void addIngestTask(Future result)
    {
        this.tasks.add(result);
    }

    private Future getIngestTask()
    {
        return tasks.poll();
    }

    private final Queue<Future> tasks = new LinkedList<>();

    private final Queue<String> savedFiles = new LinkedList<>();

	/**
	 * Constructor that sets the filename
	 * @param filename The name of the source file
	 */
	public ZippedSource (String filename)
	{
	    this.setFilename(filename);
	    this.directoryPath = Paths.get(filename).toAbsolutePath().getParent().toString();
    }

	@Override
	public void saveForecast() throws IOException {
	    issue(true);
	}

	@Override
	public void saveObservation() throws IOException {
	    issue(false);
	}

	private void issue(boolean isForecast)
    {
        FileInputStream fileStream = null;
        BufferedInputStream bufferedFile = null;
        GzipCompressorInputStream decompressedFileStream = null;
        TarArchiveInputStream archive = null;
        TarArchiveEntry archivedSource;

        try {
            fileStream = new FileInputStream(this.getAbsoluteFilename());
            bufferedFile = new BufferedInputStream(fileStream);
            decompressedFileStream = new GzipCompressorInputStream(bufferedFile);
            archive = new TarArchiveInputStream(decompressedFileStream);

            archivedSource = archive.getNextTarEntry();

            while (archivedSource != null)
            {
                if (archivedSource.isFile())
                {
                    processFile(archivedSource, archive, isForecast);
                }

                archivedSource = archive.getNextTarEntry();
            }

            Future ingestTask = this.getIngestTask();
            while (ingestTask != null)
            {
                try {
                    ingestTask.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                ingestTask = this.getIngestTask();
            }

            ingestTask = Database.getStoredIngestTask();

            while (ingestTask != null)
            {
                try {
                    ingestTask.get();
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
                ingestTask = Database.getStoredIngestTask();
            }

            for (String filename : this.savedFiles)
            {
                new File(filename).delete();
            }
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            if (archive != null)
            {
                try {
                    archive.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (decompressedFileStream != null)
            {
                try {
                    decompressedFileStream.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (bufferedFile != null)
            {
                try {
                    bufferedFile.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fileStream != null)
            {
                try {
                    fileStream.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void processFile(TarArchiveEntry source, TarArchiveInputStream archiveInputStream, boolean isForecast) throws IOException
    {
        String archivedFileName = Paths.get(this.directoryPath, source.getName()).toString();
        SourceType sourceType = ReaderFactory.getFiletype(archivedFileName);

        byte[] content = new byte[(int)source.getSize()];
        archiveInputStream.read(content, 0, content.length);
        WRESRunnable ingest;

        if (sourceType == SourceType.PI_XML)
        {
            ingest = new ZippedPIXMLIngest(archivedFileName, content, this.getDataSourceConfig(), this.getSpecifiedFeatures());
            ingest.setOnRun(ProgressMonitor.onThreadStartHandler());
            ingest.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
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
                    ingest = new ForecastSaver(archivedFileName, this.getDataSourceConfig(), this.getSpecifiedFeatures());
                }
                else
                {
                    ingest = new ObservationSaver(archivedFileName, this.getDataSourceConfig(), this.getSpecifiedFeatures());
                }

                ingest.setOnRun(ProgressMonitor.onThreadStartHandler());
                ingest.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                Future task = Executor.execute(ingest);
                this.addIngestTask(task);
            }

        }
    }

    private final String directoryPath;
}
