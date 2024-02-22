package wres.reading.waterml;

import java.io.Serial;
import java.io.Serializable;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A response.
 */
@XmlRootElement
public class Response implements Serializable
{
    @Serial
    private static final long serialVersionUID = 5433948668020324606L;
    /** Name. */
    private String name;
    /** Declared type. */
    private String declaredType;
    /** Scope. */
    private String scope;
    /** Value. */
    private ResponseValue value;
    /** The nil status. */
    private Boolean nil;
    /** Whether there is global scope. */
    private Boolean globalScope;
    /** Whether the type is substituted. */
    private Boolean typeSubstituted;

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * @param name the name
     */
    public void setName( String name )
    {
        this.name = name;
    }

    /**
     * @return the declared type
     */
    public String getDeclaredType()
    {
        return declaredType;
    }

    /**
     * Sets the declared type.
     * @param declaredType the declared type
     */
    public void setDeclaredType( String declaredType )
    {
        this.declaredType = declaredType;
    }

    /**
     * @return the scope
     */
    public String getScope()
    {
        return scope;
    }

    /**
     * Sets the scope.
     * @param scope the scope
     */
    public void setScope( String scope )
    {
        this.scope = scope;
    }

    /**
     * @return the value
     */
    public ResponseValue getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     * @param value the value
     */
    public void setValue( ResponseValue value )
    {
        this.value = value;
    }

    /**
     * @return the nil status
     */
    public Boolean getNil()
    {
        return nil;
    }

    /**
     * Sets the nil status.
     * @param nil the nil status
     */
    public void setNil( Boolean nil )
    {
        this.nil = nil;
    }

    /**
     * @return the global scope
     */
    public Boolean getGlobalScope()
    {
        return globalScope;
    }

    /**
     * Sets the global scope.
     * @param globalScope the global scope
     */
    public void setGlobalScope( Boolean globalScope )
    {
        this.globalScope = globalScope;
    }

    /**
     * @return the type substitution status
     */
    public Boolean getTypeSubstituted()
    {
        return typeSubstituted;
    }

    /**
     * Sets the type substitution status.
     * @param typeSubstituted the type substitution status
     */
    public void setTypeSubstituted( Boolean typeSubstituted )
    {
        this.typeSubstituted = typeSubstituted;
    }
}
