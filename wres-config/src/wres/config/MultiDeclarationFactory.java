package wres.config;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.xml.ProjectConfigPlus;
import wres.config.xml.Validation;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.DeclarationValidator;
import wres.config.yaml.components.EvaluationDeclaration;

/**
 * A transition class that accepts a declaration string formatted in either the old (XML) or new (YAML) declaration
 * language and produces an immutable binding for the new declaration language. In other words, migrates the old
 * declaration as needed.
 *
 * @author James Brown
 */

public class MultiDeclarationFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MultiDeclarationFactory.class );

    /** Default data directory. */
    private static final Path DATA_DIRECTORY = Path.of( System.getProperty( "user.dir" ) );

    /**
     * <p>Deserializes a string into a POJO. The string may be any of:
     *
     * <ol>
     * <li>A path to a file that contains a string formatted in the old (XML) declaration language;</li>
     * <li>A path to a file that contains a string formatted in the new (YAML) declaration language;</li>
     * <li>A string formatted in the old (XML) declaration language; or</li>
     * <li>A string formatted in the new (YAML) declaration language.</li>
     * </ol>
     *
     * <p>Performs validation against the relevant schema in all cases. Optionally performs interpolation of missing
     * declaration, followed by validation of the interpolated declaration. Interpolation is performed with
     * {@link DeclarationInterpolator#interpolate(EvaluationDeclaration, boolean)} and validation is performed with
     * {@link DeclarationValidator#validate(EvaluationDeclaration, boolean)}, both with notifications on.
     *
     * @param declarationOrPath the string containing declaration or a path
     * @param fileSystem a file system to use when reading a path, optional
     * @param interpolateAndValidate is true to validate the declaration and interpolate missing declaration
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws FileNotFoundException if the input string is a path, but the path points to a missing file
     * @throws DeclarationException if the declaration is invalid
     * @throws NullPointerException if the input string is null
     */

    public static EvaluationDeclaration from( String declarationOrPath,
                                              FileSystem fileSystem,
                                              boolean interpolateAndValidate ) throws IOException
    {
        Objects.requireNonNull( declarationOrPath );

        String declarationString = declarationOrPath;

        // Default origin
        String origin = "command line argument";

        // Use the default file system when none was supplied
        if ( Objects.isNull( fileSystem ) )
        {
            fileSystem = FileSystems.getDefault();
        }

        // Does the path point to a readable file?
        if ( DeclarationUtilities.isReadableFile( fileSystem, declarationOrPath ) )
        {
            Path path = fileSystem.getPath( declarationOrPath );
            declarationString = Files.readString( path );
            origin = path.toString();
            LOGGER.info( "Discovered a path to a declaration string: {}", path );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a declaration string to read:{}{}", System.lineSeparator(), declarationString );
        }

        // Now we have the string, detect the type of declaration
        MediaType detectedMediaType;

        try ( InputStream inputStream = new ByteArrayInputStream( declarationString.getBytes() ) )
        {
            Metadata metadata = new Metadata();
            TikaConfig tikaConfig = new TikaConfig();
            Detector detector = tikaConfig.getDetector();
            detectedMediaType = detector.detect( inputStream, metadata );

            LOGGER.debug( "The detected MIME type of the declaration string was {} and the subtype was {}.",
                          detectedMediaType.getType(),
                          detectedMediaType.getSubtype() );
        }
        catch ( TikaException e )
        {
            throw new IOException( "Failed to detect the MIME type of the declaration string: " + declarationString );
        }

        if ( "application".equals( detectedMediaType.getType() )
             && "xml".equals( detectedMediaType.getSubtype() ) )
        {
            return MultiDeclarationFactory.fromOld( declarationString, interpolateAndValidate, origin );
        }

        // Must be the new language, else invalid
        return DeclarationFactory.from( declarationString, fileSystem, interpolateAndValidate );
    }

    /**
     * Returns a declaration POJO from a string formatted in the old (XML) declaration language.
     * @param declarationString the declaration string
     * @param interpolateAndValidate is true to validate the declaration and interpolate missing declaration
     * @param origin the origin of the declaration
     * @return an evaluation declaration
     * @throws IOException if the declaration string could not be (de)serialized
     * @throws DeclarationException if the declaration string is invalid
     */

    private static EvaluationDeclaration fromOld( String declarationString,
                                                  boolean interpolateAndValidate,
                                                  String origin )
            throws IOException
    {
        LOGGER.warn( "Detected a declaration string that is formatted with the old (XML) declaration language. "
                     + "This language has been marked deprecated and may be removed without warning in a future "
                     + "release. It is recommended that you declare your evaluation using the new (YAML) "
                     + "declaration language." );

        // Log the declaration string: see issue #56900
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "The old-style declaration string is:{}{}", System.lineSeparator(), declarationString.trim() );
        }

        ProjectConfigPlus project = ProjectConfigPlus.from( declarationString, origin );

        // Validate the project against its own schema, if required
        if ( interpolateAndValidate )
        {
            if ( Validation.isProjectValid( DATA_DIRECTORY, project ) )
            {
                LOGGER.info( "Successfully validated the declaration in: {}.", project );
            }
            else
            {
                throw new DeclarationException( "Discovered invalid declaration in: "
                                                + project
                                                + "." );
            }
        }

        // Suggest a migrated declaration string to use
        EvaluationDeclaration migrated = DeclarationFactory.from( project.getProjectConfig() );

        if ( LOGGER.isInfoEnabled() )
        {
            String migratedString = DeclarationFactory.from( migrated );
            LOGGER.info( "Here is a migrated declaration that uses the new declaration language (this may not be "
                         + "the most succinct way to declare your evaluation in the new language, but it can be "
                         + "used directly or adjusted as required):{}{}{}{}",
                         System.lineSeparator(),
                         "---",
                         System.lineSeparator(),
                         migratedString );
        }

        // Finally, interpolate any missing declaration for internal use
        if( interpolateAndValidate )
        {
            migrated = DeclarationInterpolator.interpolate( migrated, true );
        }

        return migrated;
    }

    /**
     * Do not construct.
     */

    private MultiDeclarationFactory()
    {
    }
}
