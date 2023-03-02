package simpledb.common;

import simpledb.storage.DbFile;

/**
 * The CatalogTable class stores all the information of a table, such as DbFile, name, and pkeyField.
 * This is then used in Catalog class to store all the tables in the database.
 * Most of the Catalog methods use CatalogTable methods to get the information of a table.
 */
public class CatalogTable {
    private DbFile file;
    private String name;
    private String primaryKey;

    public CatalogTable(DbFile file, String name, String primaryKey) {
		this.file = file;
		this.name = name;
		this.primaryKey = primaryKey;
	}

	public DbFile getCatalogFile() {
		return this.file;
	}

	public String getCatalogName() {
		return this.name;
	}

	public String getCatalogPrimaryKey() {
		return this.primaryKey;
	}
}