package wres.io.reading;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.io.config.LeftOrRightOrBaseline;

public class DataSource
{
    /**
     * The context in which this source is declared.
     */

    private final DataSourceConfig context;

    /**
     * The source to load and link.
     */

    private final DataSourceConfig.Source source;

    /**
     * Additional links; may be empty, in which case, link only to
     * its own {@link #context}.
     */

    private final Set<LeftOrRightOrBaseline> links;

    /**
     * URI of the source.
     */

    private final URI uri;

    /**
     * Create a data source to load into <code>wres.Source</code>, with optional links to
     * create in <code>wres.ProjectSource</code>. If the source is used only once in the
     * declaration, there will be no additional links and the set of links should be empty.
     * The evaluated path to the source may not match the URI within the source, because
     * the path has been evaluated. For example, evaluation means to decompose a
     * source directory into separate paths to each file that must be loaded. Each
     * file has a separate {@link DataSource}. For each of those decomposed paths, there
     * is only one {@link DataSourceConfig.Source}.
     *
     * @param source the source to load
     * @param context the context in which the source appears
     * @param links the optional links to create
     * @param uri the uri for the source
     * @throws NullPointerException if any input is null
     * @return The newly created DataSource.
     */

    public static DataSource of( DataSourceConfig.Source source,
                                 DataSourceConfig context,
                                 Set<LeftOrRightOrBaseline> links,
                                 URI uri )
    {
        return new DataSource( source, context, links, uri );
    }

    /**
     * Create a source.
     * @param source the source
     * @param context the context in which the source appears
     * @param links the links
     * @param uri the uri
     */

    private DataSource( DataSourceConfig.Source source,
                        DataSourceConfig context,
                        Set<LeftOrRightOrBaseline> links,
                        URI uri )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( context );
        Objects.requireNonNull( links );
        Objects.requireNonNull( uri );

        this.source = source;
        this.context = context;
        this.links = Collections.unmodifiableSet( links );
        this.uri = uri;
    }

    /**
     * Returns the type of link to create.
     *
     * @return the type of link
     */

    public Set<LeftOrRightOrBaseline> getLinks()
    {
        // Rendered immutable on construction
        return this.links;
    }

    /**
     * Returns the data source.
     *
     * @return the source
     */

    public DataSourceConfig.Source getSource()
    {
        return this.source;
    }

    /**
     * Returns the data source path.
     *
     * @return the path
     */

    public URI getUri()
    {
        return this.uri;
    }
    
    /**
     * Returns <code>true</code> if the data source path is not null, otherwise
     * <code>false</code>. The path may not be available for some services,
     * for which the system knows the path.
     *
     * @return true if the path is available, otherwise false
     */

    boolean hasSourcePath()
    {
        return Objects.nonNull( this.getUri() );
    }

    /**
     * Returns the context in which the source appears.
     *
     * @return the context
     */

    public DataSourceConfig getContext()
    {
        return this.context;
    }

    /**
     * Returns the variable specified for this source, null if unspecified
     * @return the variable
     */
    public DataSourceConfig.Variable getVariable()
    {
        return this.getContext()
                   .getVariable();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof DataSource ) )
        {
            return false;
        }

        DataSource in = (DataSource) o;

        return in.source.equals( this.source )
               && in.getLinks().equals( this.getLinks() )
               && in.getContext().equals( this.getContext() )
               && in.getUri().equals( this.getUri() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.source, this.getLinks(), this.getContext(), this.getUri() );
    }

    @Override
    public String toString()
    {
        // Improved for #63493
        
        StringJoiner joiner = new StringJoiner( ";", "(", ")" );

        joiner.add( "URI: '" + this.getUri() + "'" );
        joiner.add( " Type: '" + this.getContext().getType() + "'" );

        if ( Objects.nonNull( this.getSource().getFormat() ) )
        {
            joiner.add( " Format: '" + this.getSource().getFormat() + "'" );
        }

        if ( !this.getLinks().isEmpty() )
        {
            joiner.add( " Links to other contexts: '" + this.getLinks() );
        }

        return joiner.toString();
    }

    /**
     * Return a copy of this except with a different DataSourceConfig
     * @param dataSourceConfig the DataSourceConfig to use.
     * @return a DataSource exactly the same as this, but with context from args
     */
    DataSource withContext( DataSourceConfig dataSourceConfig )
    {
        return DataSource.of( this.getSource(),
                              dataSourceConfig,
                              this.getLinks(),
                              this.getUri() );
    }
}
