package wres.config.xml;

import com.google.protobuf.DoubleValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.yaml.DeclarationFactory;
import wres.statistics.generated.Threshold;

import java.io.IOException;
import java.io.Reader;
import java.io.Serial;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Helps read files of Comma Separated Values (CSV).
 *
 * @author James Brown
 */

public class CsvThresholdReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CsvThresholdReader.class );

    /** Directory to use when building relative paths. */
    private static final Path DATA_DIRECTORY = Path.of( System.getProperty( "user.dir" ) );

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into
     * a {@link Map} whose keys are {@link String} and whose values comprise a {@link Set} of {@link Threshold}.
     *
     * @param threshold the threshold configuration
     * @param unit the (optional) existing measurement unit associated with the threshold values; if null, equal to
     *            the evaluation units
     * @return a map of thresholds by feature identifier
     * @throws IOException if the source cannot be read
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if any of the thresholds are invalid
     */

    public static Map<String, Set<Threshold>> readThresholds( ThresholdsConfig threshold,
                                                              String unit )
            throws IOException
    {
        Objects.requireNonNull( threshold, "Specify a non-null source of thresholds to read." );

        ThresholdsConfig.Source nextSource = ( ThresholdsConfig.Source ) threshold.getCommaSeparatedValuesOrSource();

        // Pre-validate path
        if ( Objects.isNull( nextSource.getValue() ) )
        {
            throw new MetricConfigException( threshold,
                                             "Specify a non-null path to read for the external "
                                             + "source of thresholds." );
        }
        // Validate format
        if ( nextSource.getFormat() != ThresholdFormat.CSV )
        {
            throw new MetricConfigException( threshold,
                                             "Unsupported source format for thresholds '"
                                             + nextSource.getFormat()
                                             + "'" );
        }

        // Missing value?
        Double missingValue = null;

        if ( Objects.nonNull( nextSource.getMissingValue() ) )
        {
            missingValue = Double.parseDouble( nextSource.getMissingValue() );
        }

        // Path
        // See #59422
        // Construct a path using the SystemSetting wres.dataDirectory when
        // the specified source is not absolute.
        URI uri = nextSource.getValue();
        Path commaSeparated;

        if ( !uri.isAbsolute() )
        {
            commaSeparated = DATA_DIRECTORY.resolve( uri.getPath() );
            LOGGER.debug( "Transformed relative URI {} to Path {}.",
                          uri,
                          commaSeparated );
        }
        else
        {
            commaSeparated = Paths.get( uri );
        }

        // Condition: default to greater
        Threshold.ThresholdOperator operator = Threshold.ThresholdOperator.GREATER;
        if ( Objects.nonNull( threshold.getOperator() ) )
        {
            operator = ProjectConfigs.getThresholdOperator( threshold.getOperator() );
        }

        // Data type: default to left
        Threshold.ThresholdDataType dataType = Threshold.ThresholdDataType.LEFT;
        if ( Objects.nonNull( threshold.getApplyTo() ) )
        {
            dataType = Threshold.ThresholdDataType.valueOf( threshold.getApplyTo()
                                                                     .name() );
        }

        ThresholdDataTypes dataTypes = new ThresholdDataTypes( dataType, threshold.getType(), operator );

        return CsvThresholdReader.readThresholds( commaSeparated,
                                                  dataTypes,
                                                  missingValue,
                                                  unit );
    }

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into
     * a {@link Map} whose keys are {@link String} and whose values comprise a {@link Set} of {@link Threshold}.
     *
     * @param commaSeparated the path to the comma separated values
     * @param dataTypes the threshold data types
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unit the (optional) existing measurement units associated with the threshold values; if null, equal to
     *            the evaluation units
     * @return a map of thresholds by feature
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if one or more features failed with expected problems, such as
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that
     *            are invalid (e.g. probability thresholds that are out-of-bounds).
     */

    private static Map<String, Set<Threshold>> readThresholds( Path commaSeparated,
                                                               ThresholdDataTypes dataTypes,
                                                               Double missingValue,
                                                               String unit )
            throws IOException
    {
        Map<String, Set<Threshold>> returnMe = new TreeMap<>();
        Set<String> duplicates = new TreeSet<>();

        // Feature count
        int totalFeatures = 0;

        ThresholdExceptions ex = new ThresholdExceptions( new TreeSet<>(),
                                                          new TreeSet<>(),
                                                          new TreeSet<>(),
                                                          new TreeSet<>() );

        // Read the input
        try ( Reader reader = Files.newBufferedReader( commaSeparated, StandardCharsets.UTF_8 );
              CSVReader csvReader = new CSVReader( reader ) )
        {
            String[] line;
            String[] header;
            String[] labels = null;
            while ( Objects.nonNull( line = csvReader.readNext() ) )
            {
                // Skip any empty lines
                if ( line.length == 0 || line[0].isBlank() )
                {
                    LOGGER.debug( "Discovered an empty line in threshold source: {}.", commaSeparated );
                    continue;
                }

                if ( line[0].toLowerCase()
                            .contains( "locationid" ) )
                {
                    header = line;

                    // locationId allowed without labels
                    if ( header.length > 1 )
                    {
                        labels = Arrays.copyOfRange( header, 1, header.length );
                    }
                }
                else
                {
                    String nextFeature = line[0].stripLeading();

                    Set<Threshold> thresholds =
                            CsvThresholdReader.getAllThresholdsForOneFeatureAndSaveExceptions( dataTypes,
                                                                                               labels,
                                                                                               line,
                                                                                               missingValue,
                                                                                               unit,
                                                                                               ex );

                    CsvThresholdReader.addThresholds( returnMe, nextFeature, thresholds, duplicates, commaSeparated );

                    totalFeatures++;
                }
            }
        }
        catch ( CsvValidationException e )
        {
            throw new IOException( "Encountered an error while reading " + commaSeparated + ".", e );
        }

        // Warn about duplicates
        if ( LOGGER.isWarnEnabled() && !duplicates.isEmpty() )
        {
            LOGGER.warn( "While reading thresholds from {}, encountered {} duplicate feature(s) whose thresholds will "
                         + "be pooled together. The duplicate feature(s) were: {}.",
                         commaSeparated,
                         duplicates.size(),
                         duplicates );
        }

        // Propagate any exceptions that were caught to avoid drip-feeding
        CsvThresholdReader.throwExceptionIfOneOrMoreFailed( totalFeatures,
                                                            commaSeparated,
                                                            ex );

        return returnMe;
    }

    /**
     * Adds the thresholds to the store and records duplicates where they occur.
     * @param thresholdStore the map of thresholds to update
     * @param nextFeature the next feature name
     * @param thresholds the thresholds to add
     * @param duplicates the duplicates to record
     * @param thresholdPath the path to the thresholds to help with error messaging
     */

    private static void addThresholds( Map<String, Set<Threshold>> thresholdStore,
                                       String nextFeature,
                                       Set<Threshold> thresholds,
                                       Set<String> duplicates,
                                       Path thresholdPath )
    {
        if ( !thresholds.isEmpty() )
        {
            // Duplicate?
            if ( thresholdStore.containsKey( nextFeature ) )
            {
                // Threshold names are not allowed in this context
                Set<String> labels = thresholdStore.get( nextFeature )
                                                   .stream()
                                                   .map( Threshold::getName )
                                                   .filter( next -> !next.isBlank() )
                                                   .collect( Collectors.toSet() );

                if ( !labels.isEmpty() )
                {
                    throw new IllegalArgumentException( "While reading thresholds from "
                                                        + thresholdPath
                                                        + ", encountered duplicate thresholds for feature "
                                                        + nextFeature
                                                        + " with duplicate threshold labels, namely: "
                                                        + labels
                                                        + ". This is not allowed. Please remove the duplicate features "
                                                        + "by declaring all thresholds once for each feature." );
                }

                thresholdStore.get( nextFeature )
                              .addAll( thresholds );

                duplicates.add( nextFeature );
            }
            else
            {
                thresholdStore.put( nextFeature, thresholds );
            }
        }
    }

    /**
     * Mutates the input map, reading all thresholds for one feature and increments any exceptions encountered.
     *
     * @param dataTypes the threshold data types
     * @param labels the optional labels (may be null)
     * @param featureThresholds the next set of thresholds to process for a given feature, including the feature label
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unit the threshold value unit
     * @param ex the incrementing exceptions encountered to avoid drip-feeding
     * @return the thresholds for one feature
     */

    private static Set<Threshold> getAllThresholdsForOneFeatureAndSaveExceptions( ThresholdDataTypes dataTypes,
                                                                                  String[] labels,
                                                                                  String[] featureThresholds,
                                                                                  Double missingValue,
                                                                                  String unit,
                                                                                  ThresholdExceptions ex )
    {
        String nextFeature = featureThresholds[0].stripLeading();

        try
        {
            return CsvThresholdReader.getAllThresholdsForOneFeature( dataTypes,
                                                                     labels,
                                                                     featureThresholds,
                                                                     missingValue,
                                                                     unit );
        }
        // Catch expected exceptions and propagate finally to avoid drip-feeding
        catch ( LabelInconsistencyException e )
        {
            ex.featuresThatFailedWithLabelInconsistency.add( nextFeature );
        }
        catch ( AllThresholdsMissingException e )
        {
            ex.featuresThatFailedWithAllThresholdsMissing.add( nextFeature );
        }
        catch ( NumberFormatException e )
        {
            ex.featuresThatFailedWithNonNumericInput.add( nextFeature );
        }
        catch ( IllegalArgumentException e )
        {
            ex.featuresThatFailedWithOtherWrongInput.add( nextFeature );
        }

        return Collections.emptySet();
    }

    /**
     * Mutates the input map, reading all thresholds for one feature.
     *
     * @param dataType the threshold data types
     * @param labels the optional labels (may be null)
     * @param featureThresholds the next set of thresholds to process for a given feature, including the feature label
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unit the threshold value unit
     * @throws NullPointerException if the featureThresholds is null
     * @throws LabelInconsistencyException if the number of labels is inconsistent with the number of thresholds
     * @throws NumberFormatException if one of the thresholds was not a number
     * @throws AllThresholdsMissingException if all thresholds are missing values
     * @return the thresholds for one feature
     */

    private static Set<Threshold> getAllThresholdsForOneFeature( ThresholdDataTypes dataType,
                                                                 String[] labels,
                                                                 String[] featureThresholds,
                                                                 Double missingValue,
                                                                 String unit )
    {

        Objects.requireNonNull( featureThresholds );

        if ( Objects.nonNull( labels ) && featureThresholds.length - 1 != labels.length )
        {
            throw new LabelInconsistencyException( "One or more lines contained a different number "
                                                   + "of thresholds than labels." );
        }

        // Threshold type: default to probability
        boolean isProbability = true;
        if ( Objects.nonNull( dataType.thresholdType() ) )
        {
            isProbability = dataType.thresholdType()
                                    .name()
                                    .toLowerCase()
                                    .contains( "probability" );
        }

        return CsvThresholdReader.getThresholds( Arrays.copyOfRange( featureThresholds,
                                                                     1,
                                                                     featureThresholds.length ),
                                                 labels,
                                                 isProbability,
                                                 dataType.operator(),
                                                 dataType.thresholdDataType(),
                                                 missingValue,
                                                 unit );
    }

    /**
     * Generates a {@link Set} of {@link Threshold} from the input string.
     *
     * @param input the comma separated input string
     * @param labels a set of labels (as many as thresholds) or null
     * @param isProbability is true to build probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unit the threshold unit
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold
     *            or all threshold values match the missingValue
     * @throws NumberFormatException if the strings cannot be parsed to numbers
     * @throws AllThresholdsMissingException if all thresholds are missing values
     */

    private static Set<Threshold> getThresholds( String[] input,
                                                 String[] labels,
                                                 boolean isProbability,
                                                 Threshold.ThresholdOperator condition,
                                                 Threshold.ThresholdDataType dataType,
                                                 Double missingValue,
                                                 String unit )
    {
        Objects.requireNonNull( input, "Specify a non-null input in order to read the thresholds." );

        Set<Threshold> returnMe = new LinkedHashSet<>();

        // Define possibly null labels for iteration
        String[] iterateLabels = labels;
        if ( Objects.isNull( labels ) )
        {
            iterateLabels = new String[input.length];
        }

        // Iterate through the thresholds

        // Count the number of missing thresholds
        int missingCount = 0;

        for ( int i = 0; i < input.length; i++ )
        {
            double threshold = Double.parseDouble( input[i] );

            // Label without leading whitespace
            String nextLabel = iterateLabels[i];
            if ( Objects.nonNull( nextLabel ) )
            {
                nextLabel = nextLabel.stripLeading();
            }

            // Non-missing value?
            if ( Objects.isNull( missingValue ) || Math.abs( missingValue - threshold ) > .00000001 )
            {
                Threshold canonical = CsvThresholdReader.getThreshold( threshold,
                                                                       isProbability,
                                                                       condition,
                                                                       dataType,
                                                                       nextLabel,
                                                                       unit );

                returnMe.add( canonical );
            }
            else
            {
                missingCount++;
            }
        }

        // Cannot have all missing thresholds
        if ( missingCount == input.length && Objects.nonNull( missingValue ) )
        {
            throw new AllThresholdsMissingException( "All thresholds matched the missing value of '"
                                                     + missingValue
                                                     + "'" );
        }

        return returnMe;
    }

    /**
     * Throws an exception if one or more of the inputs contains elements, and decorates the exception with the type of 
     * failure, based on the input.
     *
     * @param totalFeatures the total number of features processed
     * @param pathToThresholds the path to the CSV thresholds-by-feature
     * @param ex the threshold exceptions
     * @throws IllegalArgumentException if one or more features failed
     */

    private static void throwExceptionIfOneOrMoreFailed( int totalFeatures,
                                                         Path pathToThresholds,
                                                         ThresholdExceptions ex )
    {
        StringJoiner exceptionMessage = new StringJoiner( " " );

        int failCount = ex.featuresThatFailedWithLabelInconsistency.size()
                        + ex.featuresThatFailedWithAllThresholdsMissing.size()
                        + ex.featuresThatFailedWithNonNumericInput.size()
                        + ex.featuresThatFailedWithOtherWrongInput.size();

        if ( failCount > 0 )
        {
            exceptionMessage.add( "When processing thresholds by feature, " + failCount
                                  + " of "
                                  + totalFeatures
                                  + " features contained in '"
                                  + pathToThresholds
                                  + "' failed with exceptions, as follows." );
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !ex.featuresThatFailedWithLabelInconsistency.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with an inconsistency between "
                                  + "the number of labels and the number of thresholds: "
                                  + ex.featuresThatFailedWithLabelInconsistency
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !ex.featuresThatFailedWithAllThresholdsMissing.isEmpty() )
        {
            exceptionMessage.add( "    These features failed because "
                                  + "all thresholds matched the missing value: "
                                  + ex.featuresThatFailedWithAllThresholdsMissing
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );
        }

        if ( !ex.featuresThatFailedWithNonNumericInput.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with non-numeric input: "
                                  + ex.featuresThatFailedWithNonNumericInput
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !ex.featuresThatFailedWithOtherWrongInput.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with invalid input for the threshold type: "
                                  + ex.featuresThatFailedWithOtherWrongInput
                                  + "." );
        }

        // Throw exception if required
        if ( exceptionMessage.length() > 0 )
        {
            throw new IllegalArgumentException( exceptionMessage.toString() );
        }
    }

    /**
     * Creates a canonical threshold from the input.
     * @param threshold the threshold
     * @param isProbability whether the threshold is a probability
     * @param condition the condition
     * @param dataType the data type
     * @param name the threshold name
     * @param unit the value unit
     * @return the threshold
     */

    private static Threshold getThreshold( double threshold,
                                           boolean isProbability,
                                           Threshold.ThresholdOperator condition,
                                           Threshold.ThresholdDataType dataType,
                                           String name,
                                           String unit )
    {
        Threshold.Builder canonical = Threshold.newBuilder()
                                               .setOperator( condition )
                                               .setDataType( dataType );

        // Probability threshold?
        if ( isProbability )
        {
            if ( Double.NEGATIVE_INFINITY != threshold
                 && ( threshold < 0.0 || threshold > 1.0 ) )
            {
                throw new IllegalArgumentException( "The threshold probability is out of bounds [0,1]: "
                                                    + threshold );
            }

            // Cannot have LESS_THAN on the lower bound
            if ( Math.abs( threshold - 0.0 ) < .00000001 && condition == Threshold.ThresholdOperator.LESS )
            {
                throw new IllegalArgumentException( "Cannot apply a threshold operator of '<' to the lower bound "
                                                    + "probability of 0.0." );
            }
            // Cannot have GREATER_THAN on the upper bound
            if ( Math.abs( threshold - 1.0 ) < .00000001
                 && condition == Threshold.ThresholdOperator.GREATER )
            {
                throw new IllegalArgumentException( "Cannot apply a threshold operator of '>' to the upper bound "
                                                    + "probability of 1.0." );
            }

            canonical.setLeftThresholdProbability( DoubleValue.of( threshold ) );
        }
        else
        {
            canonical.setLeftThresholdValue( DoubleValue.of( threshold ) );
        }

        if ( Objects.nonNull( name ) )
        {
            canonical.setName( name );
        }

        if ( Objects.nonNull( unit ) )
        {
            canonical.setThresholdValueUnits( unit );
        }

        return canonical.build();
    }

    /**
     * Used to identify an exception that originates from all thresholds matching the missing
     * value.
     */

    private static class AllThresholdsMissingException extends IllegalArgumentException
    {
        @Serial
        private static final long serialVersionUID = -7265565803593007243L;

        /**
         * Creates an instance.
         * @param message the message
         */
        public AllThresholdsMissingException( String message )
        {
            super( message );
        }
    }

    /**
     * Used to identify an exception that originates from an inconsistency between the number
     * of labels and the number of thresholds.
     */
    private static class LabelInconsistencyException extends IllegalArgumentException
    {
        @Serial
        private static final long serialVersionUID = 4507239538788881616L;

        /**
         * Creates an instance.
         * @param message the message
         */
        public LabelInconsistencyException( String message )
        {
            super( message );
        }
    }

    /**
     * Rather than drip-feeding failures, collect all expected failure types, which
     * are IllegalArgumentException and NumberFormatException and propagate at the end. Propagate all unexpected
     * failure types immediately.
     *
     * @param featuresThatFailedWithLabelInconsistency features that failed with an inconsistency between labels
     * @param featuresThatFailedWithAllThresholdsMissing features that failed with all missing values
     * @param featuresThatFailedWithNonNumericInput features that failed with non-numeric input
     * @param featuresThatFailedWithOtherWrongInput features that failed with other wrong input identified by
     *                                              an IllegalArgumentException
     */
    private record ThresholdExceptions( Set<String> featuresThatFailedWithLabelInconsistency,
                                        Set<String> featuresThatFailedWithAllThresholdsMissing,
                                        Set<String> featuresThatFailedWithNonNumericInput,
                                        Set<String> featuresThatFailedWithOtherWrongInput ) {}

    /**
     * Collection of threshold enumerations.
     * @param thresholdDataType the threshold data type
     * @param thresholdType the threshold type
     * @param operator the threshold operator
     */
    private record ThresholdDataTypes( Threshold.ThresholdDataType thresholdDataType,
                                       ThresholdType thresholdType,
                                       Threshold.ThresholdOperator operator ) {}

    /**
     * Do not construct.
     */

    private CsvThresholdReader()
    {
    }
}
