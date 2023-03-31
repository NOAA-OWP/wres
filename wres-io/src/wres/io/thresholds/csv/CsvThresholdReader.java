package wres.io.thresholds.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import wres.config.MetricConfigException;

import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdException;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.exceptions.AllThresholdsMissingException;
import wres.io.thresholds.exceptions.LabelInconsistencyException;
import wres.system.SystemSettings;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.DoubleUnaryOperator;
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

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into
     * a {@link Map} whose keys are {@link String} and whose values comprise a {@link Set} of {@link ThresholdOuter}.
     *
     * @param systemSettings the system settings used to help resolve a path to thresholds
     * @param threshold the threshold configuration
     * @param units the (optional) existing measurement units associated with the threshold values; if null, equal to
     *            the evaluation units
     * @param unitMapper a measurement unit mapper
     * @return a map of thresholds by feature identifier
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws ThresholdException if one or more features failed with expected problems, such as
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that
     *            are invalid (e.g. probability thresholds that are out-of-bounds).
     */

    public static Map<String, Set<ThresholdOuter>> readThresholds( SystemSettings systemSettings,
                                                                   ThresholdsConfig threshold,
                                                                   MeasurementUnit units,
                                                                   UnitMapper unitMapper )
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

        //Path TODO: permit web thresholds.
        // See #59422
        // Construct a path using the SystemSetting wres.dataDirectory when
        // the specified source is not absolute.
        URI uri = nextSource.getValue();
        Path commaSeparated;

        if ( !uri.isAbsolute() )
        {
            commaSeparated = systemSettings.getDataDirectory()
                                           .resolve( uri.getPath() );
            LOGGER.debug( "Transformed relative URI {} to Path {}.",
                          uri,
                          commaSeparated );
        }
        else
        {
            commaSeparated = Paths.get( uri );
        }

        // Condition: default to greater
        Operator operator = Operator.GREATER;
        if ( Objects.nonNull( threshold.getOperator() ) )
        {
            operator = ThresholdsGenerator.getThresholdOperator( threshold );
        }

        // Data type: default to left
        ThresholdDataType dataType = ThresholdDataType.LEFT;
        if ( Objects.nonNull( threshold.getApplyTo() ) )
        {
            dataType = ThresholdsGenerator.getThresholdDataType( threshold.getApplyTo() );
        }

        ThresholdDataTypes dataTypes = new ThresholdDataTypes( dataType, threshold.getType(), operator );

        return CsvThresholdReader.readThresholds( commaSeparated,
                                                  dataTypes,
                                                  missingValue,
                                                  units,
                                                  unitMapper );
    }

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into
     * a {@link Map} whose keys are {@link String} and whose values comprise a {@link Set} of {@link ThresholdOuter}.
     *
     * @param commaSeparated the path to the comma separated values
     * @param dataTypes the threshold data types
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the (optional) existing measurement units associated with the threshold values; if null, equal to
     *            the evaluation units
     * @param unitMapper a measurement unit mapper
     * @return a map of thresholds by feature
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if one or more features failed with expected problems, such as
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that
     *            are invalid (e.g. probability thresholds that are out-of-bounds).
     */

    private static Map<String, Set<ThresholdOuter>> readThresholds( Path commaSeparated,
                                                                    ThresholdDataTypes dataTypes,
                                                                    Double missingValue,
                                                                    MeasurementUnit units,
                                                                    UnitMapper unitMapper )
            throws IOException
    {
        Map<String, Set<ThresholdOuter>> returnMe = new TreeMap<>();
        Set<String> duplicates = new TreeSet<>();

        // Feature count
        int totalFeatures = 0;

        // Internal unit mapper
        InnerUnitMapper innerUnitMapper = InnerUnitMapper.of( units, unitMapper );

        ThresholdExceptions ex = new ThresholdExceptions( new TreeSet<>(),
                                                          new TreeSet<>(),
                                                          new TreeSet<>(),
                                                          new TreeSet<>() );

        // Read the input
        try ( Reader reader = Files.newBufferedReader( commaSeparated, StandardCharsets.UTF_8 );
              CSVReader csvReader = new CSVReader( reader ) )
        {
            String[] line;
            String[] header = null;
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

                    Set<ThresholdOuter> thresholds =
                            CsvThresholdReader.getAllThresholdsForOneFeatureAndSaveExceptions( dataTypes,
                                                                                               labels,
                                                                                               line,
                                                                                               missingValue,
                                                                                               innerUnitMapper,
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

    private static void addThresholds( Map<String, Set<ThresholdOuter>> thresholdStore,
                                       String nextFeature,
                                       Set<ThresholdOuter> thresholds,
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
                                                   .map( ThresholdOuter::getLabel )
                                                   .filter( next -> !next.isBlank() )
                                                   .collect( Collectors.toSet() );

                if ( !labels.isEmpty() )
                {
                    throw new ThresholdException( "While reading thresholds from "
                                                  + thresholdPath
                                                  + ", encountered duplicate thresholds for feature "
                                                  + nextFeature
                                                  + " with duplicate threshold labels, namely: "
                                                  + labels
                                                  + ". This is not allowed. Please remove the duplicate features by "
                                                  + "declaring all thresholds once for each feature." );
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
     * @param unitMapper a mapper for the measurement units
     * @param ex the incrementing exceptions encountered to avoid drip-feeding
     * @return the thresholds for one feature
     */

    private static Set<ThresholdOuter> getAllThresholdsForOneFeatureAndSaveExceptions( ThresholdDataTypes dataTypes,
                                                                                       String[] labels,
                                                                                       String[] featureThresholds,
                                                                                       Double missingValue,
                                                                                       InnerUnitMapper unitMapper,
                                                                                       ThresholdExceptions ex )
    {
        String nextFeature = featureThresholds[0].stripLeading();

        try
        {
            return CsvThresholdReader.getAllThresholdsForOneFeature( dataTypes,
                                                                     labels,
                                                                     featureThresholds,
                                                                     missingValue,
                                                                     unitMapper );
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
        catch ( ThresholdException | IllegalArgumentException e )
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
     * @param unitMapper a mapper for the measurement units
     * @throws NullPointerException if the featureThresholds is null
     * @throws LabelInconsistencyException if the number of labels is inconsistent with the number of thresholds
     * @throws NumberFormatException if one of the thresholds was not a number
     * @throws AllThresholdsMissingException if all thresholds are missing values
     * @return the thresholds for one feature
     */

    private static Set<ThresholdOuter> getAllThresholdsForOneFeature( ThresholdDataTypes dataType,
                                                                      String[] labels,
                                                                      String[] featureThresholds,
                                                                      Double missingValue,
                                                                      InnerUnitMapper unitMapper )
    {

        Objects.requireNonNull( featureThresholds );

        if ( Objects.nonNull( labels ) && featureThresholds.length - 1 != labels.length )
        {
            throw new LabelInconsistencyException( "One or more lines contained a different number "
                                                   + "of thresholds than labels." );
        }

        // Threshold type: default to probability
        ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
        if ( Objects.nonNull( dataType.thresholdType() ) )
        {
            thresholdType = ThresholdsGenerator.getThresholdGroup( dataType.thresholdType() );
        }

        // Default to probability
        boolean isProbability = thresholdType == ThresholdConstants.ThresholdGroup.PROBABILITY;

        return CsvThresholdReader.getThresholds( Arrays.copyOfRange( featureThresholds,
                                                                     1,
                                                                     featureThresholds.length ),
                                                 labels,
                                                 isProbability,
                                                 dataType.operator(),
                                                 dataType.thresholdDataType(),
                                                 missingValue,
                                                 unitMapper );
    }

    /**
     * Generates a {@link Set} of {@link ThresholdOuter} from the input string.
     *
     * @param input the comma separated input string
     * @param labels a set of labels (as many as thresholds) or null
     * @param isProbability is true to build probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unitMapper a mapper for the measurement units
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold
     *            or all threshold values match the missingValue
     * @throws NumberFormatException if the strings cannot be parsed to numbers
     * @throws AllThresholdsMissingException if all thresholds are missing values
     */

    private static Set<ThresholdOuter> getThresholds( String[] input,
                                                      String[] labels,
                                                      boolean isProbability,
                                                      Operator condition,
                                                      ThresholdDataType dataType,
                                                      Double missingValue,
                                                      InnerUnitMapper unitMapper )
    {
        Objects.requireNonNull( input, "Specify a non-null input in order to read the thresholds." );

        Set<ThresholdOuter> returnMe = new TreeSet<>();

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

                // Probability thresholds
                if ( isProbability )
                {
                    returnMe.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                         condition,
                                                                         dataType,
                                                                         nextLabel,
                                                                         unitMapper.getDesiredMeasurementUnit() ) );
                }
                // Ordinary thresholds
                else
                {
                    double thresholdInDesiredUnits = unitMapper.getValueInDesiredUnits( threshold );

                    returnMe.add( ThresholdOuter.of( OneOrTwoDoubles.of( thresholdInDesiredUnits ),
                                                     condition,
                                                     dataType,
                                                     nextLabel,
                                                     unitMapper.getDesiredMeasurementUnit() ) );
                }
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
     * @throws ThresholdException if one or more features failed
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
            throw new ThresholdException( exceptionMessage.toString() );
        }
    }

    /**
     * Inner units mapper.
     */

    private static class InnerUnitMapper
    {
        /**
         * Existing measurement units.
         */
        private final MeasurementUnit existingUnit;

        /**
         * A general unit mapper.
         */

        private final UnitMapper generalUnitMapper;

        /**
         * A mapper to create desired measurement units.
         */
        private final DoubleUnaryOperator specificUnitMapper;

        /**
         * Create an instance.
         * @param existingUnits the existing measurement unit
         * @param desiredUnitMapper a mapper to create desired measurement units
         * @return an inner mapper
         */

        private static InnerUnitMapper of( MeasurementUnit existingUnits, UnitMapper desiredUnitMapper )
        {
            return new InnerUnitMapper( existingUnits, desiredUnitMapper );
        }

        /**
         * Returns the desired measurement unit.
         * @return the desired measurement unit
         */

        private MeasurementUnit getDesiredMeasurementUnit()
        {
            return MeasurementUnit.of( this.generalUnitMapper.getDesiredMeasurementUnitName() );
        }

        /**
         * Maps an input value in the existing measurement units to the desired units. If the existing units are
         * unknown, the input value is returned.
         *
         * @param valueInExistingUnits a threshold value in existing units
         * @return the thresholds value in desired units
         */

        private double getValueInDesiredUnits( double valueInExistingUnits )
        {
            // No existing units, desired units assumed
            if ( Objects.isNull( this.existingUnit ) )
            {
                return valueInExistingUnits;
            }

            return this.specificUnitMapper.applyAsDouble( valueInExistingUnits );
        }

        /**
         * Create an instance.
         * @param existingUnit the existing measurement unit
         * @param generalUnitMapper a mapper to create desired measurement units
         */

        private InnerUnitMapper( MeasurementUnit existingUnit, UnitMapper generalUnitMapper )
        {
            this.existingUnit = existingUnit;
            this.generalUnitMapper = generalUnitMapper;

            if ( Objects.nonNull( this.existingUnit ) )
            {
                this.specificUnitMapper = generalUnitMapper.getUnitMapper( existingUnit.toString() );
            }
            else
            {
                this.specificUnitMapper = null;
            }
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
    private record ThresholdDataTypes( ThresholdConstants.ThresholdDataType thresholdDataType,
                                       ThresholdType thresholdType,
                                       ThresholdConstants.Operator operator ) {}

    /**
     * Do not construct.
     */

    private CsvThresholdReader()
    {
    }
}
