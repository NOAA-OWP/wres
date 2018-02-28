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
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;

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
     * @return a map of thresholds by feature
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws NumberFormatException if the strings cannot be parsed to numbers
     */

    public static Map<FeaturePlus, Set<Threshold>> readThresholds( Path commaSeparated,
                                                                   boolean isProbability,
                                                                   Operator condition )
            throws IOException
    {
        Objects.requireNonNull( commaSeparated, "Specify a non-null source of comma separated thresholds to read." );

        Objects.requireNonNull( condition, "Specify non-null condition in order to obtain the thresholds." );

        Map<FeaturePlus, Set<Threshold>> returnMe = new TreeMap<>();

        // Read the input
        try ( BufferedReader input = Files.newBufferedReader( commaSeparated, StandardCharsets.UTF_8 ) )
        {
            String firstLine = input.readLine();
            String[] header = firstLine.split( "\\s*(,)\\s*" );
            String[] labels = null;
            try
            {
                // Header?
                // Yes: process it
                if ( header.length > 0 && "locationId".equalsIgnoreCase( header[0] ) )
                {
                    // locationId allowed without labels
                    if( header.length > 1 )
                    {
                        labels = Arrays.copyOfRange( header, 1, header.length );
                    }
                }
                // No: add the first row
                else
                {
                    readAllThresholdsForOneFeature( returnMe,
                                                    isProbability,
                                                    condition,
                                                    labels,
                                                    firstLine );
                }

                // Process each feature
                String nextLine;
                while ( Objects.nonNull( nextLine = input.readLine() ) )
                {
                    readAllThresholdsForOneFeature( returnMe,
                                                    isProbability,
                                                    condition,
                                                    labels,
                                                    nextLine );

                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new IOException( "While processing '" + commaSeparated + "': ", e );
            }
        }

        return returnMe;
    }

    /**
     * Mutates the input map, reading all thresholds for one feature.
     * 
     * @param mutate the map of thresholds to mutate
     * @param isProbability is true to read probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param labels the optional labels (may be null)
     * @param nextInputFeature the next set of thresholds to process
     * @throws IllegalArgumentException if the number of labels is inconsistent with the number of thresholds or
     *            the threshold content is inconsistent with the specified type of threshold
     */

    private static void readAllThresholdsForOneFeature( Map<FeaturePlus, Set<Threshold>> mutate,
                                                        boolean isProbability,
                                                        Operator condition,
                                                        String[] labels,
                                                        String nextInputFeature )
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
            Feature feature = new Feature( null, null, null, next[0], null, null, null, null, null, null );
            FeaturePlus nextFeature = FeaturePlus.of( feature );
            mutate.put( nextFeature, getThresholds( Arrays.copyOfRange( next, 1, next.length ),
                                                    labels,
                                                    isProbability,
                                                    condition ) );
        }
    }

    /**
     * Generates a {@link Set} of {@link Threshold} from the input string. 
     * 
     * @param input the comma separated input string
     * @param labels a set of labels (as many as thresholds) or null
     * @param isProbability is true to build probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @throws NullPointerException if the input or condition is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold     
     * @throws NumberFormatException if the strings cannot be parsed to numbers                  
     */

    private static Set<Threshold> getThresholds( String[] input,
                                                 String[] labels,
                                                 boolean isProbability,
                                                 Operator condition )
    {
        Objects.requireNonNull( input, "Specify non-null input in order to obtain the thresholds." );

        Objects.requireNonNull( condition, "Specify non-null condition in order to obtain the thresholds." );

        Set<Threshold> returnMe = new TreeSet<>();

        DataFactory factory = DefaultDataFactory.getInstance();

        // Define possibly null labels for iteration 
        String[] iterateLabels = labels;
        if ( Objects.isNull( labels ) )
        {
            iterateLabels = new String[input.length];
        }

        // Iterate through the thresholds
        for ( int i = 0; i < input.length; i++ )
        {
            // Probability thresholds
            if ( isProbability )
            {
                returnMe.add( factory.ofProbabilityThreshold( Double.valueOf( input[i] ),
                                                              condition,
                                                              iterateLabels[i] ) );
            }
            // Ordinary thresholds
            else
            {
                returnMe.add( factory.ofThreshold( Double.valueOf( input[i] ), condition, iterateLabels[i] ) );
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
