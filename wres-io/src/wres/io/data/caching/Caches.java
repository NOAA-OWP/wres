package wres.io.data.caching;

import java.util.Objects;

import wres.config.generated.ProjectConfig;
import wres.io.utilities.Database;

/**
 * A small container of database caches/ORMs. The individual caches are built on construction and are accessible.
 * 
 * @author James Brown
 */

public class Caches
{
    /** Cache of data sources. */
    private final DataSources dataSourcesCache;
    /** Cache of features. */
    private final Features featuresCache;
    /** Cache of time scales. */
    private final TimeScales timeScalesCache;
    /** Cache of ensemble metadata. */
    private final Ensembles ensemblesCache;
    /** Cache of measurement units. */
    private final MeasurementUnits measurementUnitsCache;

    /**
     * Creates an instance.
     * @param database the database, required
     * @param projectConfig the project declaration, required
     * @return an instance of the caches
     * @throws NullPointerException if either input is null
     */

    public static Caches of( Database database, ProjectConfig projectConfig )
    {
        return new Caches( database, projectConfig );
    }

    /**
     * @return the data sources cache
     */
    public DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
    }

    /**
     * @return the features cache
     */
    public Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    /**
     * @return the time scales cache
     */
    public TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    /**
     * @return the ensembles cache
     */
    public Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    /**
     * @return the measurement units cache
     */
    public MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    /**
     * Hidden constructor.
     * @param database the database, required
     * @param projectConfig the project declaration, required
     * @throws NullPointerException if either input is null
     */
    private Caches( Database database, ProjectConfig projectConfig )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getPair() );
        
        this.dataSourcesCache = new DataSources( database );
        this.featuresCache = new Features( database, projectConfig.getPair()
                                                                  .getGridSelection() );
        this.timeScalesCache = new TimeScales( database );
        this.ensemblesCache = new Ensembles( database );
        this.measurementUnitsCache = new MeasurementUnits( database );
    }
}
