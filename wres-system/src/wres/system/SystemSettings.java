package wres.system;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

/**
 * The cache for all configured system settings
 */
@XmlRootElement( name = "wresconfig" )
@XmlAccessorType( XmlAccessType.NONE )
@Value
@NoArgsConstructor
@AllArgsConstructor
@Builder( toBuilder = true )
public class SystemSettings
{
    @Builder.Default
    @XmlElement( name = "database" )
    DatabaseSettings databaseConfiguration = null;
    @Builder.Default
    @XmlElement( name = "use_database" )
    boolean useDatabase = false;
    @Builder.Default
    @XmlElement( name = "maximum_thread_count" )
    int maximumThreadCount = 10;
    @Builder.Default
    @XmlElement( name = "pool_object_lifespan" )
    int poolObjectLifespan = 30000;
    @Builder.Default
    @XmlElement( name = "fetch_size" )
    int fetchSize = 100;
    @Builder.Default
    @XmlElement( name = "maximum_copies" )
    int maximumCopies = 200;
    @Builder.Default
    @XmlElement( name = "netcdf_cache_period" )
    int netcdfCachePeriod = 90;
    @Builder.Default
    @XmlElement( name = "minimum_cached_netcdf" )
    int minimumCachedNetcdf = 100;
    @Builder.Default
    @XmlElement( name = "maximum_cached_netcdf" )
    int maximumCachedNetcdf = 200;
    @Builder.Default
    @XmlElement( name = "hard_netcdf_cache_limit" )
    int hardNetcdfCacheLimit = 0;
    @Builder.Default
    @XmlElement( name = "netcdf_store_path" )
    String netcdfStorePath = "systests/data/";
    @Builder.Default
    @XmlElement( name = "maximum_archive_threads" )
    Integer maximumArchiveThreads = null;
    @Builder.Default
    @XmlElement( name = "maximum_web_client_threads" )
    int maximumWebClientThreads = 3;
    @Builder.Default
    @XmlElement( name = "maximum_nwm_ingest_threads" )
    int maximumNwmIngestThreads = 6;
    @Builder.Default
    @XmlElement( name = "maximum_pool_threads" )
    int maximumPoolThreads = 6;
    @Builder.Default
    @XmlElement( name = "maximum_slicing_threads" )
    int maximumSlicingThreads = 1;
    @Builder.Default
    @XmlElement( name = "maximum_metric_threads" )
    int maximumMetricThreads = 1;
    @Builder.Default
    @XmlElement( name = "maximum_product_threads" )
    int maximumProductThreads = 3;
    @Builder.Default
    @XmlElement( name = "maximum_read_threads" )
    int maximumReadThreads = 10;
    @Builder.Default
    @XmlElement( name = "maximum_ingest_threads" )
    int maximumIngestThreads = 7;
    @Builder.Default
    @XmlElement( name = "maximum_sampling_uncertainty_threads" )
    int maximumSamplingUncertaintyThreads = 6;

    /** The minimum number of singleton features per evaluation at which feature-batched retrieval is triggered. **/
    @Builder.Default
    @XmlElement( name = "feature_batch_threshold" )
    int featureBatchThreshold = 10;
    /** The number of features contained within each feature batch when feature-batched retrieval is conducted. **/
    @Builder.Default
    @XmlElement( name = "feature_batch_size" )
    int featureBatchSize = 50;

    /**
     * Creates and returns a copy of the system settings with any PII or BII redacted. This should be used to publish
     * information about runtime settings (e.g., in logging).
     *
     * @return the redacted system settings
     */

    public SystemSettings redacted()
    {
        //Remove password information from the System Settings
        DatabaseSettings.DatabaseSettingsBuilder databaseBuilder =
                this.getDatabaseConfiguration()
                    .toBuilder();
        databaseBuilder.password( "[REDACTED]" );
        return this.toBuilder()
                   .databaseConfiguration( databaseBuilder.build() )
                   .build();
    }

    /**
     * Returns a redacted map of system properties that do not expose an PII or BII. All sensitive properties, including
     * the property names are removed. Always use this method when publishing (e.g., logging) information about system
     * properties.
     *
     * @return the redacted system properties
     */

    public Map<String, String> redactedSystemProperties()
    {
        // Order the property names for consistency.
        Properties properties = System.getProperties();
        Map<String, String> returnMe = new TreeMap<>();

        properties.forEach( ( key, value ) -> {
            String propertyName = key.toString();
            String lowerCaseName = propertyName.toLowerCase();
            // Avoid printing passphrases or passwords or passes of any kind,
            // avoid printing full classpath, ignore separators, ignore printers
            // and ignore some extraneous sun/oracle directories
            if ( !lowerCaseName.contains( "pass" )
                 && !lowerCaseName.contains( "class.path" )
                 && !lowerCaseName.contains( "separator" )
                 && !lowerCaseName.startsWith( "sun" )
                 && !lowerCaseName.contains( "user.country" )
                 && !lowerCaseName.startsWith( "java.vendor" )
                 && !lowerCaseName.startsWith( "java.e" )
                 && !lowerCaseName.startsWith( "java.vm.specification" )
                 && !lowerCaseName.startsWith( "java.specification" )
                 && !lowerCaseName.contains( "printer" )
                 && !lowerCaseName.contains( "key" ) )
            {
                returnMe.put( propertyName, System.getProperty( propertyName ) );
            }

            // Allow some names to be passed through that are less sensitive and help to provide some context, but
            // redact the details

            // GitHub #258. Signal that the rate limiting key has been passed through, but do not report the key
            if ( propertyName.equals( "wres.nwisApiKey" ) )
            {
                returnMe.put( "wres.nwisApiKey", "[REDACTED]" );
            }
        } );

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Dummy class to allow javadoc task to find the builder created by lombok.
     */
    public static class SystemSettingsBuilder {}  // NOSONAR
}
