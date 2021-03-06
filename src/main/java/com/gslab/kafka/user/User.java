/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.gslab.kafka.user;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class User extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.gslab.kafka.user\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence firstName;
  @Deprecated public java.lang.CharSequence lastName;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public User() {}

  /**
   * All-args constructor.
   */
  public User(java.lang.CharSequence firstName, java.lang.CharSequence lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return firstName;
    case 1: return lastName;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: firstName = (java.lang.CharSequence)value$; break;
    case 1: lastName = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'firstName' field.
   */
  public java.lang.CharSequence getFirstName() {
    return firstName;
  }

  /**
   * Sets the value of the 'firstName' field.
   * @param value the value to set.
   */
  public void setFirstName(java.lang.CharSequence value) {
    this.firstName = value;
  }

  /**
   * Gets the value of the 'lastName' field.
   */
  public java.lang.CharSequence getLastName() {
    return lastName;
  }

  /**
   * Sets the value of the 'lastName' field.
   * @param value the value to set.
   */
  public void setLastName(java.lang.CharSequence value) {
    this.lastName = value;
  }

  /** Creates a new User RecordBuilder */
  public static com.gslab.kafka.user.User.Builder newBuilder() {
    return new com.gslab.kafka.user.User.Builder();
  }
  
  /** Creates a new User RecordBuilder by copying an existing Builder */
  public static com.gslab.kafka.user.User.Builder newBuilder(com.gslab.kafka.user.User.Builder other) {
    return new com.gslab.kafka.user.User.Builder(other);
  }
  
  /** Creates a new User RecordBuilder by copying an existing User instance */
  public static com.gslab.kafka.user.User.Builder newBuilder(com.gslab.kafka.user.User other) {
    return new com.gslab.kafka.user.User.Builder(other);
  }
  
  /**
   * RecordBuilder for User instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
    implements org.apache.avro.data.RecordBuilder<User> {

    private java.lang.CharSequence firstName;
    private java.lang.CharSequence lastName;

    /** Creates a new Builder */
    private Builder() {
      super(com.gslab.kafka.user.User.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.gslab.kafka.user.User.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.firstName)) {
        this.firstName = data().deepCopy(fields()[0].schema(), other.firstName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.lastName)) {
        this.lastName = data().deepCopy(fields()[1].schema(), other.lastName);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing User instance */
    private Builder(com.gslab.kafka.user.User other) {
            super(com.gslab.kafka.user.User.SCHEMA$);
      if (isValidValue(fields()[0], other.firstName)) {
        this.firstName = data().deepCopy(fields()[0].schema(), other.firstName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.lastName)) {
        this.lastName = data().deepCopy(fields()[1].schema(), other.lastName);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'firstName' field */
    public java.lang.CharSequence getFirstName() {
      return firstName;
    }
    
    /** Sets the value of the 'firstName' field */
    public com.gslab.kafka.user.User.Builder setFirstName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.firstName = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'firstName' field has been set */
    public boolean hasFirstName() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'firstName' field */
    public com.gslab.kafka.user.User.Builder clearFirstName() {
      firstName = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'lastName' field */
    public java.lang.CharSequence getLastName() {
      return lastName;
    }
    
    /** Sets the value of the 'lastName' field */
    public com.gslab.kafka.user.User.Builder setLastName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.lastName = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'lastName' field has been set */
    public boolean hasLastName() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'lastName' field */
    public com.gslab.kafka.user.User.Builder clearLastName() {
      lastName = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public User build() {
      try {
        User record = new User();
        record.firstName = fieldSetFlags()[0] ? this.firstName : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.lastName = fieldSetFlags()[1] ? this.lastName : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
