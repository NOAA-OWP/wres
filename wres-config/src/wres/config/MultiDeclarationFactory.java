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

import org.apache.commons.lang3.tuple.Pair;
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
import wres.config.yaml.DeclarationMigrator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.DeclarationValidator;
import wres.config.yaml.components.EvaluationDeclaration;

/**
 * A transition class that accepts a declaration string formatted in either the old (XML) or new (YAML) declaration
 * language and produces an immutable binding for the new declaration language. In other words, migrates the old
 * declaration as needed. This class should be removed and the {@link DeclarationFactory} used directly once the old
 * (XML) declaration language has been removed.
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
     * <p>Deserializes a declaration string or path into a POJO. The string may be any of:
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
     * {@link DeclarationInterpolator#interpolate(EvaluationDeclaration)} and validation is performed with
     * {@link DeclarationValidator#validate(EvaluationDeclaration, boolean)}, both with notifications on.
     *
     * @param declarationOrPath the string containing declaration or a path
     * @param fileSystem a file system to use when reading a path, optional
     * @param interpolate is true to interpolate any missing declaration from the other declaration supplied
     * @param validate is true to validate the declaration
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema or path could not be read
     * @throws FileNotFoundException if the input string is a path, but the path points to a missing file
     * @throws DeclarationException if the declaration is invalid
     * @throws NullPointerException if the input string is null
     */

    public static EvaluationDeclaration from( String declarationOrPath,
                                              FileSystem fileSystem,
                                              boolean interpolate,
                                              boolean validate ) throws IOException
    {
        Objects.requireNonNull( declarationOrPath );

        Pair<String, String> declarationPair =
                MultiDeclarationFactory.getDeclarationStringAndOrigin( declarationOrPath, fileSystem );

        String declarationString = declarationPair.getLeft();
        String origin = declarationPair.getRight();

        // Now we have the string, detect the type of declaration
        MediaType detectedMediaType = MultiDeclarationFactory.getMediaType( declarationString );

        if ( MultiDeclarationFactory.isOldDeclarationString( detectedMediaType, declarationString ) )
        {
            return MultiDeclarationFactory.fromOld( declarationString, interpolate, validate, origin );
        }

        // Must be the new language, else invalid
        return DeclarationFactory.from( declarationString, fileSystem, interpolate, validate );
    }

    /**
     * Returns a declaration string and inferred origin from either a declaration string or path, reading from the path
     * as needed.
     * @param declarationOrPath the string containing declaration or a path
     * @param fileSystem a file system to use when reading a path, optional
     * @return the declaration string
     * @throws IOException if a path could not be read
     */

    public static String getDeclarationString( String declarationOrPath,
                                               FileSystem fileSystem ) throws IOException
    {
        return MultiDeclarationFactory.getDeclarationStringAndOrigin( declarationOrPath, fileSystem )
                                      .getLeft();
    }

    /**
     * Determines whether the supplied string is a path that contains a declaration string or a declaration string
     * itself, else unrecognized. The string may be formatted in either of the two declaration languages. Performs a
     * shallow analysis only and does not fully parse or validate the string against its schema.
     *
     * @param declarationOrPath the declaration or path
     * @return whether the input is a readable declaration or declaration string
     * @throws NullPointerException if the input is null
     */

    public static boolean isDeclarationPathOrString( String declarationOrPath )
    {
        Objects.requireNonNull( declarationOrPath );

        try
        {
            FileSystem fileSystem = FileSystems.getDefault();
            Pair<String, String> declarationPair =
                    MultiDeclarationFactory.getDeclarationStringAndOrigin( declarationOrPath, fileSystem );
            String declarationString = declarationPair.getLeft();

            MediaType detectedMediaType = MultiDeclarationFactory.getMediaType( declarationString );

            // XML? Then considered a declaration string
            if ( MultiDeclarationFactory.isOldDeclarationString( detectedMediaType, declarationString ) )
            {
                return true;
            }

            // New-style declaration?
            return DeclarationFactory.isDeclarationString( declarationString );
        }
        catch ( IOException e )
        {
            String message = "Encountered an exception while inspecting the declaration or path: "
                             + declarationOrPath;
            LOGGER.warn( message, e );
            return false;
        }
    }

    /**
     * Returns a declaration string and inferred origin from either a declaration string or path, reading from the path
     * as needed.
     * @param declarationOrPath the string containing declaration or a path
     * @param fileSystem a file system to use when reading a path, optional
     * @return the declaration string and origin in that order
     * @throws IOException if a path could not be read
     */

    private static Pair<String, String> getDeclarationStringAndOrigin( String declarationOrPath,
                                                                       FileSystem fileSystem ) throws IOException
    {
        String declarationString = declarationOrPath;

        // Default origin
        String origin = "declaration string";

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
            LOGGER.info( "Discovered a path to a declaration string: {}", origin );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a declaration string to read:{}{}", System.lineSeparator(), declarationString );
        }

        return Pair.of( declarationString, origin );
    }

    /**
     * Determines whether the declaration string is an old-style declaration string.
     * @param mediaType the media type associated with the declaration string
     * @param declarationString the declaration string
     * @return whether the string is an old-style declaration string
     * @throws NullPointerException if either input is null
     */

    private static boolean isOldDeclarationString( MediaType mediaType, String declarationString )
    {
        Objects.requireNonNull( mediaType );
        Objects.requireNonNull( declarationString );

        // Permissive check because a string without <?xml version="1.0" encoding="UTF-8"?> will still parse correctly,
        // even though the content type will not be detected correctly. The first check deals with that scenario and
        // the second check deals with a correctly detected content type.
        return declarationString.startsWith( "<project" )
               || ( "application".equals( mediaType.getType() )
                    && "xml".equals( mediaType.getSubtype() ) )
               || declarationString.trim()
                                   .startsWith( "<?xml" ); // When tika fails
    }

    /**
     * Inspects the string for MIME type.
     * @param declarationString the declaration
     * @return the media type
     * @throws IOException if the content could not be parsed for detection
     */

    private static MediaType getMediaType( String declarationString ) throws IOException
    {
        try ( InputStream inputStream = new ByteArrayInputStream( declarationString.getBytes() ) )
        {
            Metadata metadata = new Metadata();
            TikaConfig tikaConfig = new TikaConfig();
            Detector detector = tikaConfig.getDetector();
            MediaType detectedMediaType = detector.detect( inputStream, metadata );

            LOGGER.debug( "The detected MIME type of the declaration string was {} and the subtype was {}.",
                          detectedMediaType.getType(),
                          detectedMediaType.getSubtype() );

            return detectedMediaType;
        }
        catch ( TikaException e )
        {
            throw new IOException( "Failed to detect the MIME type of the declaration string: " + declarationString );
        }
    }

    /**
     * Returns a declaration POJO from a string formatted in the old (XML) declaration language.
     * @param declarationString the declaration string
     * @param interpolate is true to interpolate any missing declaration from the other declaration supplied
     * @param validate is true to validate the declaration
     * @param origin the origin of the declaration
     * @return an evaluation declaration
     * @throws IOException if the declaration string could not be (de)serialized
     * @throws DeclarationException if the declaration string is invalid
     */

    private static EvaluationDeclaration fromOld( String declarationString,
                                                  boolean interpolate,
                                                  boolean validate,
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
        if ( validate )
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
        EvaluationDeclaration migrated = DeclarationMigrator.from( project.getProjectConfig(), false );

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

        // Validate the migrated declaration. This exposes the declaration to two layers of validation, first against
        // the old schema and business logic, now against the new schema and business logic
        if ( validate )
        {
            DeclarationValidator.validate( migrated, true );
        }

        // Interpolate any missing declaration for internal use
        if ( interpolate )
        {
            migrated = DeclarationInterpolator.interpolate( migrated );
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
