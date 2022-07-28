package wres.io.reading;

import java.util.List;

import wres.io.ingesting.IngestResult;

/**
 * Saves or ingests a time-series data source.
 * 
 * @author James Brown
 */

public interface Source
{
    /**
     * Saves a time-series data source
     * @return the record of data saved
     * @throws ReadException if the reading fails for any reason
     */
    List<IngestResult> save();
}
