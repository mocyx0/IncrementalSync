package org.pangolin.xuzhe.positiveorder;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ubuntu on 17-6-19.
 */
public class MyLong2IntHashMap {
    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * An empty table instance to share when the table is not inflated.
     */
    static final Entry[] EMPTY_TABLE = {};

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    transient Entry[] table = EMPTY_TABLE;

    public static final int DEQUE_SIZE = 200000;
    Deque<Entry> removedEntry = new ArrayDeque<>(DEQUE_SIZE);
    /**
     * The number of key-value mappings contained in this map.
     */
    transient int size;

    /**
     * The next size value at which to resize (capacity * load factor).
     *
     * @serial
     */
    // If table == EMPTY_TABLE then this is the initial capacity at which the
    // table will be created when inflated.
    int threshold;

    /**
     * The load factor for the hash table.
     *
     * @serial
     */
    final float loadFactor;

    /**
     * The default threshold of map capacity above which alternative hashing is
     * used for String keys. Alternative hashing reduces the incidence of
     * collisions due to weak hash code calculation for String keys.
     * <p/>
     * This value may be overridden by defining the system property
     * {@code jdk.map.althashing.threshold}. A property value of {@code 1}
     * forces alternative hashing to be used at all times whereas
     * {@code -1} value ensures that alternative hashing is never used.
     */
    static final int ALTERNATIVE_HASHING_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

    /**
     * holds values which can't be initialized until after VM is booted.
     */
    private static class Holder {

        /**
         * Table capacity above which to switch to use alternative hashing.
         */
        static final int ALTERNATIVE_HASHING_THRESHOLD;

        static {
            String altThreshold = java.security.AccessController.doPrivileged(
                    new sun.security.action.GetPropertyAction(
                            "jdk.map.althashing.threshold"));

            int threshold;
            try {
                threshold = (null != altThreshold)
                        ? Integer.parseInt(altThreshold)
                        : ALTERNATIVE_HASHING_THRESHOLD_DEFAULT;

                // disable alternative hashing if -1
                if (threshold == -1) {
                    threshold = Integer.MAX_VALUE;
                }

                if (threshold < 0) {
                    throw new IllegalArgumentException("value must be positive integer.");
                }
            } catch (IllegalArgumentException failed) {
                throw new Error("Illegal value for 'jdk.map.althashing.threshold'", failed);
            }

            ALTERNATIVE_HASHING_THRESHOLD = threshold;
        }
    }

    /**
     * A randomizing value associated with this instance that is applied to
     * hash code of keys to make hash collisions harder to find. If 0 then
     * alternative hashing is disabled.
     */
    transient int hashSeed = 0;

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and load factor.
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor      the load factor
     * @throws IllegalArgumentException if the initial capacity is negative
     *                                  or the load factor is nonpositive
     */
    public MyLong2IntHashMap(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);

        this.loadFactor = loadFactor;
        threshold = initialCapacity;
        init();
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and the default load factor (0.75).
     *
     * @param initialCapacity the initial capacity.
     * @throws IllegalArgumentException if the initial capacity is negative.
     */
    public MyLong2IntHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the default initial capacity
     * (16) and the default load factor (0.75).
     */
    public MyLong2IntHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    private static int roundUpToPowerOf2(int number) {
        // assert number >= 0 : "number must be non-negative";
        return number >= MAXIMUM_CAPACITY
                ? MAXIMUM_CAPACITY
                : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    /**
     * Inflates the table.
     */
    private void inflateTable(int toSize) {
        // Find a power of 2 >= toSize
        int capacity = roundUpToPowerOf2(toSize);

        threshold = (int) Math.min(capacity * loadFactor, MAXIMUM_CAPACITY + 1);
        table = new Entry[capacity];
        initHashSeedAsNeeded(capacity);
    }

    // internal utilities

    /**
     * Initialization hook for subclasses. This method is called
     * in all constructors and pseudo-constructors (clone, readObject)
     * after HashMap has been initialized but before any entries have
     * been inserted.  (In the absence of this method, readObject would
     * require explicit knowledge of subclasses.)
     */
    void init() {
    }

    /**
     * Initialize the hashing mask value. We defer initialization until we
     * really need it.
     */
    final boolean initHashSeedAsNeeded(int capacity) {
        boolean currentAltHashing = hashSeed != 0;
        boolean useAltHashing = sun.misc.VM.isBooted() &&
                (capacity >= Holder.ALTERNATIVE_HASHING_THRESHOLD);
        boolean switching = currentAltHashing ^ useAltHashing;
        if (switching) {
            hashSeed = useAltHashing
                    ? randomHashSeed(this)
                    : 0;
        }
        return switching;
    }

    public static int randomHashSeed(Object instance) {
        int seed;
        if (sun.misc.VM.isBooted()) {
            seed = ThreadLocalRandom.current().nextInt();
        } else {
            // lower quality "random" seed value--still better than zero and not
            // not practically reversible.
            int hashing_seed[] = {
                    System.identityHashCode(MyLong2ObjHashMap.class),
                    System.identityHashCode(instance),
                    System.identityHashCode(Thread.currentThread()),
                    (int) Thread.currentThread().getId(),
                    (int) (System.currentTimeMillis() >>> 2), // resolution is poor
                    (int) (System.nanoTime() >>> 5), // resolution is poor
                    (int) (Runtime.getRuntime().freeMemory() >>> 4) // alloc min
            };

            seed = murmur3_32(0, hashing_seed, 0, hashing_seed.length);
        }

        // force to non-zero.
        return (0 != seed) ? seed : 1;
    }

    public static int murmur3_32(int seed, int[] data, int offset, int len) {
        int h1 = seed;

        int off = offset;
        int end = offset + len;

        // body
        while (off < end) {
            int k1 = data[off++];

            k1 *= 0xcc9e2d51;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= 0x1b873593;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail (always empty, as body is always 32-bit chunks)

        // finalization

        h1 ^= len * (Integer.SIZE / Byte.SIZE);

        // finalization mix force all bits of a hash block to avalanche
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Retrieve object hash code and applies a supplemental hash function to the
     * result hash, which defends against poor quality hash functions.  This is
     * critical because HashMap uses power-of-two length hash tables, that
     * otherwise encounter collisions for hashCodes that do not differ
     * in lower bits. Note: Null keys always map to hash 0, thus index 0.
     */
    final int hash(long k) {
//        int h = hashSeed;
//        h ^= k;
//        h ^= (k >> 32);
//
//        // This function ensures that hashCodes that differ only by
//        // constant multiples at each bit position have a bounded
//        // number of collisions (approximately 8 at default load factor).
//        h ^= (h >>> 20) ^ (h >>> 12);
//        return h ^ (h >>> 7) ^ (h >>> 4);
        return (int)k;
    }

    /**
     * Returns index for hash code h.
     */
    static int indexFor(int h, int length) {
        // assert Integer.bitCount(length) == 1 : "length must be a non-zero power of 2";
        return h & (length - 1);
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    public int size() {
        return size;
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     * <p>
     * <p>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     *
     * @see #put(long, int)
     */
    public int get(long key) {
        Entry entry = getEntry(key);

        return null == entry ? -1 : entry.getValue();
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param key The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    public boolean containsKey(long key) {
        return getEntry(key) != null;
    }

    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for the key.
     */
    final Entry getEntry(long key) {
        if (size == 0) {
            return null;
        }

        int hash = hash(key);
        for (Entry e = table[indexFor(hash, table.length)];
             e != null;
             e = e.next) {
            long k;
            if (e.hash == hash &&
                    ((k = e.key) == key || (key == k)))
                return e;
        }
        return null;
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * (A <tt>null</tt> return can also indicate that the map
     * previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    public int put(long key, int value) {
        if (table == EMPTY_TABLE) {
            inflateTable(threshold);
        }
        int hash = hash(key);
        int i = indexFor(hash, table.length);
        for (Entry e = table[i]; e != null; e = e.next) {
            long k;
            if (e.hash == hash && ((k = e.key) == key || key == k)) {
                int oldValue = e.value;
                e.value = value;
                return oldValue;
            }
        }

        addEntry(hash, key, value, i);
        return -1;
    }


    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its threshold.
     * <p>
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but sets threshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *                    must be greater than current capacity unless current
     *                    capacity is MAXIMUM_CAPACITY (in which case value
     *                    is irrelevant).
     */
    void resize(int newCapacity) {
        Entry[] oldTable = table;
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }

        Entry[] newTable = new Entry[newCapacity];
        transfer(newTable, initHashSeedAsNeeded(newCapacity));
        table = newTable;
        threshold = (int) Math.min(newCapacity * loadFactor, MAXIMUM_CAPACITY + 1);
    }

    /**
     * Transfers all entries from current table to newTable.
     */
    void transfer(Entry[] newTable, boolean rehash) {
        System.out.println(rehash);
        int newCapacity = newTable.length;
        for (Entry e : table) {
            while (null != e) {
                Entry next = e.next;
                if (rehash) {
                    e.hash = hash(e.key);
                }
                int i = indexFor(e.hash, newCapacity);
                e.next = newTable[i];
                newTable[i] = e;
                e = next;
            }
        }
    }


    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    public void clear() {
        Arrays.fill(table, null);
        size = 0;
    }


    static class Entry {
        long key;
        int value;
        Entry next;
        int hash;

        /**
         * Creates new entry.
         */
        Entry(int h, long k, int v, Entry n) {
            value = v;
            next = n;
            key = k;
            hash = h;
        }

        public void reset() {
//            this.value = -1;
            this.next = null;
        }

        public void update(int h, long k, int v, Entry n) {
            value = v;
            next = n;
            key = k;
            hash = h;
        }

        public final long getKey() {
            return key;
        }

        public final int getValue() {
            return value;
        }

        public final int setValue(int newValue) {
            int oldValue = value;
            value = newValue;
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry e = (Entry) o;
            Object k1 = getKey();
            Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2)))
                    return true;
            }
            return false;
        }

        public final int hashCode() {
            return Objects.hashCode(getKey()) ^ Objects.hashCode(getValue());
        }

        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    /**
     * Adds a new entry with the specified key, value and hash code to
     * the specified bucket.  It is the responsibility of this
     * method to resize the table if appropriate.
     * <p>
     * Subclass overrides this to alter the behavior of put method.
     */
    void addEntry(int hash, long key, int value, int bucketIndex) {
        if ((size >= threshold) && (null != table[bucketIndex])) {
//            resize(2 * table.length);
            hash = hash(key);
            bucketIndex = indexFor(hash, table.length);
        }

        createEntry(hash, key, value, bucketIndex);
    }

    /**
     * Like addEntry except that this version is used when creating entries
     * as part of Map construction or "pseudo-construction" (cloning,
     * deserialization).  This version needn't worry about resizing the table.
     * <p>
     * Subclass overrides this to alter the behavior of HashMap(Map),
     * clone, and readObject.
     */
    void createEntry(int hash, long key, int value, int bucketIndex) {
        Entry e = table[bucketIndex];
        Entry newE = removedEntry.pollLast();
        if(newE == null) {
            newE = new Entry(hash, key, value, e);
        } else {
            newE.update(hash, key, value, e);
        }
        table[bucketIndex] = newE;
        size++;
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    public int remove(long key) {
        Entry e = removeEntryForKey(key);
        if(e != null && removedEntry.size() < DEQUE_SIZE) {
            e.reset();
            removedEntry.push(e);
        }
        return (e == null ? -1 : e.value);
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     */
    final Entry removeEntryForKey(long key) {
        if (size == 0) {
            return null;
        }
        int hash = (key == -1) ? 0 : hash(key);
        int i = indexFor(hash, table.length);
        Entry prev = table[i];
        Entry e = prev;

        while (e != null) {
            Entry next = e.next;
            long k;
            if (e.hash == hash &&
                    ((k = e.key) == key || (key != -1 && key == k))) {
                size--;
                if (prev == e)
                    table[i] = next;
                else
                    prev.next = next;
                return e;
            }
            prev = e;
            e = next;
        }

        return e;
    }

    // These methods are used when serializing HashSets
    int capacity() {
        return table.length;
    }

    float loadFactor() {
        return loadFactor;
    }

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
//        HashMap map = new HashMap(15000000);
//        for(long i = 0; i < 10000000; i++) {
//            map.put(i, (int)i);
//        }
        long end = System.currentTimeMillis();
        System.out.println(end-begin);
//        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(map)));
//        Iterator it = map.entrySet().iterator();
//        Object a = it.next();
//        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(a)));
//        a = it.next();
//        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(a)));
//        a = it.next();
//        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(a)));
//        a = it.next();
//        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(a)));
        begin = System.currentTimeMillis();
        MyLong2IntHashMap myMap = new MyLong2IntHashMap(10000000);
        for(long i = 0; i < 10000000; i++) {
            myMap.put(i, (int)i);
        }
        end = System.currentTimeMillis();
        System.out.println(end-begin);
        begin = System.currentTimeMillis();
        for(long i = 0; i < 10000000; i++) {
            int value = myMap.get(i);
            if(value != i)
                System.out.println(i + ":" + value);;
        }
        end = System.currentTimeMillis();
        System.out.println(end-begin);
    }

}
