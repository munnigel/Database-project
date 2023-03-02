package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and fieldAr must have the same length");
        }

        this.tdItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }

        this.tdItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            this.tdItems.add(new TDItem(typeAr[i], null));
        }
    }

 
    // Extra constructor to create a TupleDesc from a TDItem ArrayList This is for merge method.
     
    private TupleDesc(ArrayList<TDItem> tditems) {
        if (tditems == null || tditems.isEmpty()) {
            throw new IllegalArgumentException("The TDItem ArrayList is empty.");
        }

        this.tdItems = new ArrayList<>(tditems);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.tdItems.size()) {
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return this.tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
         for (TDItem field : this.tdItems) {
            int indexOfField = this.tdItems.indexOf(field);
            if (field.fieldName != null) {
                if (field.fieldName.equals(name)) {
                    return indexOfField;
                }
            } else {
                if (name == null) {
                    return indexOfField;
                }
            }
        }

        throw new NoSuchElementException("No field with a matching name was found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;

        for (int i = 0; i < this.tdItems.size(); i++) {
            size += this.tdItems.get(i).fieldType.getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
   public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> combinedItems = new ArrayList<>();

        combinedItems.addAll(td1.tdItems);
        combinedItems.addAll(td2.tdItems);

        return new TupleDesc(combinedItems);
    }
 

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // make sure o is of TupleDesc type
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        // make sure o has the same numFields
        if (this.numFields() != ((TupleDesc) o).numFields()) {
            return false;
        }

        // check field types
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldType(i) != ((TupleDesc) o).getFieldType(i)) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String result = "";
        for (int i = 0; i < this.numFields(); i++) {
            result += this.getFieldType(i) + "(" + this.getFieldName(i) + "), ";
        }
        // remove last ", "
        result = result.substring(0, result.length() - 2);
        return result;
    }
}
