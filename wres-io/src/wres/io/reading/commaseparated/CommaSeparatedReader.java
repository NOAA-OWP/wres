package wres.io.reading.commaseparated;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.Dimension;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Helps read files of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class CommaSeparatedReader
{

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into 
     * a {@link Map} whose keys are {@link FeaturePlus} and whose values comprise a {@link Set} of {@link Threshold}. 
     * 
     * @param commaSeparated the source of comma separated values
     * @param isProbability is true to read probability thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the optional units associated with the threshold values
     * @return a map of thresholds by feature
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws NumberFormatException if the strings cannot be parsed to numbers
     */

    public static Map<FeaturePlus, Set<Threshold>> readThresholds( Path commaSeparated,
                                                                   boolean isProbability,
                                                                   Operator condition,
                                                                   ThresholdDataType dataType,
                                                                   Double missingValue,
                                                                   Dimension units )
            throws IOException
    {
        Objects.requireNonNull( commaSeparated, "Specify a non-null source of comma separated thresholds to read." );

        Objects.requireNonNull( condition, "Specify a non-null condition in order to read the thresholds." );
        
        Objects.requireNonNull( condition, "Specify a non-null data type in order to read the thresholds." );
        
        Map<FeaturePlus, Set<Threshold>> returnMe = new TreeMap<>();

        // Read the input
        try ( BufferedReader input = Files.newBufferedReader( commaSeparated, StandardCharsets.UTF_8 ) )
        {
            String firstLine = input.readLine();
            String[] header = firstLine.split( "\\s*(,)\\s*" );
            String[] labels = null;

            // Header?
            // Yes: process it
            if ( header.length > 0 && "locationId".equalsIgnoreCase( header[0] ) )
            {
                // locationId allowed without labels
                if ( header.length > 1 )
                {
                    labels = Arrays.copyOfRange( header, 1, header.length );
                }
            }
            // No: add the first row
            else
            {
                CommaSeparatedReader.readAllThresholdsForOneFeature( returnMe,
                                                                     isProbability,
                                                                     condition,
                                                                     dataType,
                                                                     labels,
                                                                     firstLine,
                                                                     missingValue,
                                                                     units );
            }

            // Process each feature
            String nextLine;
            while ( Objects.nonNull( nextLine = input.readLine() ) )
            {
                CommaSeparatedReader.readAllThresholdsForOneFeature( returnMe,
                                                                     isProbability,
                                                                     condition,
                                                                     dataType,
                                                                     labels,
                                                                     nextLine,
                                                                     missingValue,
                                                                     units );

            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new IOException( "While processing '" + commaSeparated + "': ", e );
        }

        return returnMe;
    }

    /**
     * Mutates the input map, reading all thresholds for one feature.
     * 
     * @param mutate the map of thresholds to mutate
     * @param isProbability is true to read probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param labels the optional labels (may be null)
     * @param nextInputFeature the next set of thresholds to process
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the optional units associated with the threshold values
     * @throws IllegalArgumentException if the number of labels is inconsistent with the number of thresholds or
     *            the threshold content is inconsistent with the specified type of threshold
     */

    private static void readAllThresholdsForOneFeature( Map<FeaturePlus, Set<Threshold>> mutate,
                                                        boolean isProbability,
                                                        Operator condition,
                                                        ThresholdDataType dataType,
                                                        String[] labels,
                                                        String nextInputFeature,
                                                        Double missingValue,
                                                        Dimension units )
            throws IOException
    {
        // Ignore empty lines
        if ( !nextInputFeature.isEmpty() )
        {
            String[] next = nextInputFeature.split( "\\s*(,)\\s*" );
            if ( Objects.nonNull( labels ) && next.length - 1 != labels.length )
            {
                throw new IllegalArgumentException( "One or more lines contained a different number of thresholds than "
                                                    + "labels." );
            }
            // Feature maps on locationId only              
            Feature feature = new Feature( null, null, null, null, null, next[0], null, null, null, null, null, null, null );
            FeaturePlus nextFeature = FeaturePlus.of( feature );
            mutate.put( nextFeature, CommaSeparatedReader.getThresholds( Arrays.copyOfRange( next, 1, next.length ),
                                                                         labels,
                                                                         isProbability,
                                                                         condition,
                                                                         dataType,
                                                                         missingValue,
                                                                         units ) );
        }
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
     * @param units the optional units associated with the threshold values
     * @throws NullPointerException if the input or condition is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold     
     * @throws NumberFormatException if the strings cannot be parsed to numbers                  
     */

    private static Set<Threshold> getThresholds( String[] input,
                                                 String[] labels,
                                                 boolean isProbability,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 Double missingValue,
                                                 Dimension units )
    {
        Objects.requireNonNull( input, "Specify a non-null input in order to read the thresholds." );

        Objects.requireNonNull( condition, "Specify a non-null condition in order to read the thresholds." );

        Objects.requireNonNull( condition, "Specify a non-null data type in order to read the thresholds." );
        
        Set<Threshold> returnMe = new TreeSet<>();

        // Define possibly null labels for iteration 
        String[] iterateLabels = labels;
        if ( Objects.isNull( labels ) )
        {
            iterateLabels = new String[input.length];
        }

        // Iterate through the thresholds
        for ( int i = 0; i < input.length; i++ )
        {
            double threshold = Double.parseDouble( input[i] );

            // Non-missing value?
            if ( Objects.isNull( missingValue ) || Math.abs( missingValue - threshold ) > .00000001 )
            {

                // Probability thresholds
                if ( isProbability )
                {
                    returnMe.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                      condition,
                                                                      dataType,
                                                                      iterateLabels[i],
                                                                      units ) );
                }
                // Ordinary thresholds
                else
                {
                    returnMe.add( Threshold.of( OneOrTwoDoubles.of( threshold ),
                                                           condition,
                                                           dataType,
                                                           iterateLabels[i],
                                                           units ) );
                }
            }
        }

        return returnMe;
    }

    /**
     * Do not construct.
     */

    private CommaSeparatedReader()
    {
    }

}
