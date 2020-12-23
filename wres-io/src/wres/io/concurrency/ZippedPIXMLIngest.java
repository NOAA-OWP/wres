package wres.io.concurrency;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Created by ctubbs on 7/19/17.
 */
public final class ZippedPIXMLIngest extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ZippedPIXMLIngest.class);

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final byte[] content;
    private final DatabaseLockManager lockManager;

    public ZippedPIXMLIngest ( SystemSettings systemSettings,
                               Database database,
                               DataSources dataSourcesCache,
                               Features featuresCache,
                               Variables variablesCache,
                               Ensembles ensemblesCache,
                               MeasurementUnits measurementUnitsCache,
                               ProjectConfig projectConfig,
                               DataSource dataSource,
                               byte[] content,
                               DatabaseLockManager lockManager )
    {
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.content = Arrays.copyOf( content, content.length );
        this.lockManager = lockManager;
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

    @Override
    public List<IngestResult> execute() throws IOException
    {
        try ( InputStream input = new ByteArrayInputStream( this.content ) )
        {
            PIXMLReader reader = new PIXMLReader( this.getSystemSettings(),
                                                  this.getDatabase(),
                                                  this.getFeaturesCache(),
                                                  this.getVariablesCache(),
                                                  this.getEnsemblesCache(),
                                                  this.getMeasurementUnitsCache(),
                                                  this.projectConfig,
                                                  this.dataSource,
                                                  input,
                                                  this.lockManager );
            reader.parse();
            return reader.getIngestResults();
        }
    }

    @Override
    protected Logger getLogger()
    {
        return ZippedPIXMLIngest.LOGGER;
    }

}
