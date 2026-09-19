/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.quartz.utils;

/**
 * An implementation of <code>Map</code> that wraps another <code>Map</code> and flags itself
 * 'dirty' when it is modified, enforces that all keys are Strings.
 */
public class StringKeyDirtyFlagMap extends DirtyFlagMap<String, Object> {
  private static final long serialVersionUID = -9076749120524952280L;

  public StringKeyDirtyFlagMap() {
    super();
  }

  public StringKeyDirtyFlagMap(int initialCapacity) {
    super(initialCapacity);
  }

  public StringKeyDirtyFlagMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return getWrappedMap().hashCode();
  }

  /** Get a copy of the Map's String keys in an array of Strings. */
  public String[] getKeys() {
    return keySet().toArray(new String[size()]);
  }

  /** Adds the given <code>int</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, int value) {
    super.put(key, value);
  }

  /** Adds the given <code>long</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, long value) {
    super.put(key, value);
  }

  /** Adds the given <code>float</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, float value) {
    super.put(key, value);
  }

  /** Adds the given <code>double</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, double value) {
    super.put(key, value);
  }

  /** Adds the given <code>boolean</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, boolean value) {
    super.put(key, value);
  }

  /** Adds the given <code>char</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, char value) {
    super.put(key, value);
  }

  /** Adds the given <code>String</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  public void put(String key, String value) {
    super.put(key, value);
  }

  /** Adds the given <code>Object</code> value to the <code>StringKeyDirtyFlagMap</code>. */
  @Override
  public Object put(String key, Object value) {
    return super.put((String) key, value);
  }

  /**
   * Retrieve the identified <code>int</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not an Integer.
   */
  public int getInt(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Integer) return (Integer) obj;
      return Integer.parseInt((String) obj);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not an Integer.");
    }
  }

  /**
   * Retrieve the identified <code>long</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a Long.
   */
  public long getLong(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Long) return (Long) obj;
      return Long.parseLong((String) obj);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a Long.");
    }
  }

  /**
   * Retrieve the identified <code>float</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a Float.
   */
  public float getFloat(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Float) return (Float) obj;
      return Float.parseFloat((String) obj);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a Float.");
    }
  }

  /**
   * Retrieve the identified <code>double</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a Double.
   */
  public double getDouble(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Double) return (Double) obj;
      return Double.parseDouble((String) obj);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a Double.");
    }
  }

  /**
   * Retrieve the identified <code>boolean</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a Boolean.
   */
  public boolean getBoolean(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Boolean) return (Boolean) obj;
      return Boolean.parseBoolean((String) obj);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a Boolean.");
    }
  }

  /**
   * Retrieve the identified <code>char</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a Character.
   */
  public char getChar(String key) {
    Object obj = get(key);

    try {
      if (obj instanceof Character) return (Character) obj;
      return ((String) obj).charAt(0);
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a Character.");
    }
  }

  /**
   * Retrieve the identified <code>String</code> value from the <code>StringKeyDirtyFlagMap</code>.
   *
   * @throws ClassCastException if the identified object is not a String.
   */
  public String getString(String key) {
    Object obj = get(key);

    try {
      return (String) obj;
    } catch (Exception e) {
      throw new ClassCastException("Identified object is not a String.");
    }
  }
}
