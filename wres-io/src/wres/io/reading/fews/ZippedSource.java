package wres.io.reading.fews;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import wres.io.config.SystemSettings;
import wres.io.reading.BasicSource;
import wres.io.reading.XMLReader;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class ZippedSource extends BasicSource {

    private ExecutorService readerService = createReaderService();

    private ExecutorService createReaderService()
    {
        Double threadCount = Math.ceil(SystemSettings.maximumThreadCount() / 10F);
        threadCount = Math.max(threadCount, 2.0F);

        return Executors.newFixedThreadPool(threadCount.intValue());
    }

	/**
	 * Constructor
	 */
	public ZippedSource () {}

	/**
	 * Constructor that sets the filename
	 * @param filename The name of the source file
	 */
	public ZippedSource (String filename)
	{
		this.setFilename(filename);
	}

	@Override
	public void saveForecast() throws IOException {
		XMLReader sourceReader = new PIXMLReader(this.getFilename());
		sourceReader.parse();
	}

	@Override
	public void saveObservation() throws IOException {
		XMLReader sourceReader = new PIXMLReader(this.getAbsoluteFilename(), false);
		sourceReader.parse();
	}

	private void issue(boolean isForecast)
    {
        try {
            FileInputStream fileStream = new FileInputStream(this.getAbsoluteFilename());
            BufferedInputStream bufferedFile = new BufferedInputStream(fileStream);
            GzipCompressorInputStream decompressedFileStream = new GzipCompressorInputStream(bufferedFile);
            TarArchiveInputStream archive = new TarArchiveInputStream(decompressedFileStream);

            TarArchiveEntry archivedSource = archive.getNextTarEntry();

            while (archivedSource != null)
            {
                if (archivedSource.isFile())
                {
                    processFile(archivedSource, isForecast);
                }
                else
                {
                    processDirectory(archivedSource, isForecast);
                }

                archivedSource = archive.getNextTarEntry();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processDirectory(TarArchiveEntry source, boolean isForecast)
    {
        if (source.isDirectory())
        {
            TarArchiveEntry[] directoryEntries = source.getDirectoryEntries();

            for (int directoryIndex = 0; directoryIndex < directoryEntries.length; ++directoryIndex)
            {
                if (directoryEntries[directoryIndex].isFile())
                {
                    processFile(directoryEntries[directoryIndex], isForecast);
                }
                else if (directoryEntries[directoryIndex].isDirectory())
                {
                    processDirectory(directoryEntries[directoryIndex], isForecast);
                }
            }
        }
    }

    private void processFile(TarArchiveEntry source, boolean isForecast)
    {
        if (source.getName().toLowerCase().endsWith(".xml"))
        {
            // TODO: Should this be multithreaded in some way?
            PIXMLReader sourceReader = new PIXMLReader(source.getFile(), isForecast);
        }
    }
}
