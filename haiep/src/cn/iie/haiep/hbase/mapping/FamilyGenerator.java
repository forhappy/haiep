package cn.iie.haiep.hbase.mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Construct hbase column family generator.
 * @author forhappy
 * Date: 2012-4-3, 6:05 PM.
 */
public class FamilyGenerator {
	
	/**
	 * @return the index
	 */
	public int getIndex() {
		return index;
	}

	/**
	 * @param index the index to set
	 */
	public void setIndex(int index) {
		this.index = index;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the qualifiers
	 */
	public List<QualifierGenerator> getQualifiers() {
		return qualifiers;
	}

	/**
	 * Default constructor.
	 */
	public FamilyGenerator() {
		index = -1;
		qualifiers = new ArrayList<QualifierGenerator>();
	}
	
	/**
	 * @param name The name of a family.
	 */
	public FamilyGenerator(String name) {
		super();
		this.name = name;
		this.index = -1;
		qualifiers = new ArrayList<QualifierGenerator>();
	}

	/**
	 * Add a new qualifier schema to a family.
	 * @param qualifer The qualifier that is to added.
	 */
	public void addQualifier(QualifierGenerator qualifer) {
		qualifiers.add(qualifer);
	}
	
	/**
	 * Add a set qualifiers schema to a family.
	 * @param qualifiers The qualifiers that are to added.
	 */
	public void addQaulifiers(List<QualifierGenerator> qualifiers) {
		qualifiers.addAll(qualifiers);
	}
	
	/**
	 * Delete a qualifier.
	 * @param qualifier The qualifier to be deleted.
	 */
	public void deleteQualifier(QualifierGenerator qualifier) {
		qualifiers.remove(qualifier);
	}
	
	/**
	 * Get a qualifier by the name user provided.
	 * @param name The qualifier name. 
	 * @return The qualifier that match the name.
	 */
	public QualifierGenerator getQualifierByName(String name) {
		while (next()) {
			if (current().getName().equalsIgnoreCase(name))
				return qualifiers.get(index);
		}
		return null;
	}
	
	/**
	 * Get a qualifier by the original column user provided.
	 * @param name The original column name. 
	 * @return The qualifier that match the original column.
	 */
	public QualifierGenerator getQualifierByOrigColumn(String origColumn) {
		while (next()) {
			if (current().getName().equalsIgnoreCase(origColumn))
				return qualifiers.get(index);
		}
		return null;
	}
	
	/**
	 * Check if there exists another FamilyGenerator object 
	 * in the List<QualifierGenerator> qualifiers.
	 * @return True if it indeed exist a FamilyGenerator object, 
	 * otherwise return false.
	 */
	public Boolean next() {
		if (qualifiers.isEmpty()) return false;
		if (index < qualifiers.size() - 1) {
			index++;
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Return the current FamilyGenerator object pointed by index,
	 * if List<QualifierGenerator> qualifiers is empty, return null.
	 * @return
	 */
	public QualifierGenerator current() {
		if (qualifiers.isEmpty()) return null;
		return qualifiers.get(index);
	}
	
	/**
	 * The name of a family.
	 */
	private String name = null;
	
	/**
	 * Index that specify the current qualifier.
	 */
	private int index = -1;
	
	/**
	 * List of qualifiers within the family.
	 */
	List<QualifierGenerator> qualifiers = null;
}