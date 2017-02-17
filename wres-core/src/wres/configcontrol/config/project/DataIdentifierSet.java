/**
 * 
 */
package wres.configcontrol.config.project;

// Java net dependencies
import java.util.*;

// WRES dependencies
import wres.util.*;
import wres.configcontrol.config.*;

/**
 * A set of {@link DataIdentifier} in insertion order. Use {@link this#fluentAdd(DataIdentifier)} to add
 * {@link DataIdentifier} fluently.
 * 
 * @author james.brown@hydrosolved.com
 */
@SuppressWarnings("serial")
public final class DataIdentifierSet extends LinkedHashSet<DataIdentifier> implements DeepCopy<DataIdentifierSet> {

	/**
	 * A unique identifier for the set of data identifiers.
	 */

	private Identifier id = null;

	/**
	 * Construct with a default identifier.
	 */

	public DataIdentifierSet() {
		super();
		id = new Identifier(Identifier.CONFIGURATION_IDENTIFIER, System.currentTimeMillis() + "");
	}

	/**
	 * Construct a set of data identifiers with an initial identifier.
	 * 
	 * @param id
	 *            the Identifier
	 * @throws ConfigurationException
	 *             if the identifier is null
	 */

	public DataIdentifierSet(Identifier id) {
		super();
		if (id == null) {
			throw new ConfigurationException(
					"Specify a non-null identifier from which to construct the set of data identifiers.");
		}
		if (!id.contains(Identifier.CONFIGURATION_IDENTIFIER)) {
			throw new ConfigurationException("Specify a configuration identifier.");
		}
		this.id = id;
	}

	@Override
	public boolean equals(Object o) {
		return o != null && o instanceof DataIdentifierSet && ((DataIdentifierSet) o).id.equals(id)
				&& ((LinkedHashSet<DataIdentifier>) this).hashCode() == o.hashCode();
	}

	@Override
	public int hashCode() {
		int returnMe = id.hashCode();
		for (DataIdentifier d : this) {
			returnMe += d.hashCode();
		}
		return returnMe;
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("<").append(id).append(": ");
		for (DataIdentifier s : this) {
			b.append(s).append(", ");
		}
		int length = b.length();
		b.delete(length - 2, length);
		b.append(">");
		return b.toString();
	}

	/**
	 * Fluent interface to add a sequence of {@link DataIdentifier}.
	 * 
	 * @param id
	 *            the data identifier
	 * @return this
	 */

	public DataIdentifierSet fluentAdd(DataIdentifier id) {
		add(id);
		return this;
	}

	/**
	 * Returns the identifier associated with the set of identifiers.
	 * 
	 * @return the identifier
	 */
	public Identifier getID() {
		return id;
	}

	@Override
	public DataIdentifierSet deepCopy() {
		DataIdentifierSet d = new DataIdentifierSet(id);
		d.addAll(this); // DataIdentifier are immutable
		return d;
	}

}
