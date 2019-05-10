package wres.io.reading;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;

class DataSource
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
     * Optional evaluated path.
     */

    private Path path;

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
     * @param path the evaluated path to the source, which may be null for a service source
     * @throws NullPointerException if any input is null
     */

    static DataSource of( DataSourceConfig.Source source,
                          DataSourceConfig context,
                          Set<LeftOrRightOrBaseline> links,
                          Path path )
    {
        return new DataSource( source, context, links, path );
    }

    /**
     * Create a source.
     * @param source the source
     * @param context the context in which the source appears
     * @param links the links
     * @param path the optional path
     */

    private DataSource( DataSourceConfig.Source source,
                        DataSourceConfig context,
                        Set<LeftOrRightOrBaseline> links,
                        Path path )
    {
        Objects.requireNonNull( source );

        Objects.requireNonNull( context );

        Objects.requireNonNull( links );

        this.source = source;
        this.context = context;
        this.links = Collections.unmodifiableSet( links );
        this.path = path;
    }

    /**
     * Returns the type of link to create.
     *
     * @return the type of link
     */

    Set<LeftOrRightOrBaseline> getLinks()
    {
        // Rendered immutable on construction
        return this.links;
    }

    /**
     * Returns the data source.
     *
     * @return the source
     */

    DataSourceConfig.Source getSource()
    {
        return this.source;
    }

    /**
     * Returns the data source path.
     *
     * @return the path
     */

    Path getSourcePath()
    {
        return this.path;
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
        return Objects.nonNull( this.path );
    }

    /**
     * Returns the context in which the source appears.
     *
     * @return the context
     */

    DataSourceConfig getContext()
    {
        return this.context;
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
               && in.getSourcePath().equals( this.getSourcePath() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.source, this.getLinks(), this.getContext(), this.getSourcePath() );
    }

    @Override
    public String toString()
    {
        // Improved for #63493
        
        StringJoiner joiner = new StringJoiner( ";", "(", ")" );

        joiner.add( "Path: '" + this.getSourcePath() + "'" );
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

}
