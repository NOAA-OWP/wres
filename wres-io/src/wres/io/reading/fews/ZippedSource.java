package wres.io.reading.fews;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import wres.io.concurrency.ObservationSaver;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
import wres.io.reading.BasicSource;
import wres.io.reading.ReaderFactory;
import wres.io.reading.SourceType;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

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
        byte[] content;

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
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (ExecutionException e) {
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
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
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

        if (sourceType == SourceType.PI_XML)
        {
            WRESRunnable saver = new WRESRunnable() {
                @Override
                protected void execute () {


                    try (InputStream input = new ByteArrayInputStream(this.content)) {
                        PIXMLReader reader = new PIXMLReader(this.filename,
                                                             input,
                                                             this.isForecast);
                        reader.parse();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                protected String getTaskName () {
                    return "ZippedSource - Saving PIXML - " + this.filename;
                }

                @Override
                protected Logger getLogger () {
                    return ZippedSource.LOGGER;
                }

                public WRESRunnable init(String filename, byte[] content, boolean isForecast)
                {
                    this.filename = filename;
                    this.content = content;
                    this.isForecast = isForecast;
                    return this;
                }

                private byte[] content;
                private String filename;
                private boolean isForecast;
            }.init(archivedFileName, content, isForecast);

            this.addIngestTask(saver);
        }
        else
        {
            FileOutputStream stream = new FileOutputStream(archivedFileName);

            try
            {
                stream.write(content);
                this.savedFiles.add(archivedFileName);

                WRESRunnable saver;

                if (isForecast)
                {
                    saver = new ForecastSaver(archivedFileName, this.getDataSourceConfig());
                }
                else
                {
                    saver = new ObservationSaver(archivedFileName, this.getDataSourceConfig());
                }

                saver.setOnRun(ProgressMonitor.onThreadStartHandler());
                saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                Future task = Executor.execute(saver);
                this.addIngestTask(task);
            }
            finally
            {
                stream.close();
            }

        }
    }

    private final String directoryPath;
}
