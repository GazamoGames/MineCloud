package com.mongodb;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author DarkSeraphim
 * TODO: unreflect/unmethodhandle since we should be in the same classloader
 */
public class ScopedDBCollection extends DBCollection {

    private static final MethodHandles.Lookup IMPL_LOOKUP;

    private static final MethodHandle BULK_WRITE_OPERATION_INIT;

    private static final MethodHandle BULK_WRITE_OPERATION_SETLIST;

    static {
        try {
            Field field = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
            field.setAccessible(true);
            IMPL_LOOKUP = (MethodHandles.Lookup) field.get(null);
            MethodType type = MethodType.methodType(void.class, boolean.class, DBCollection.class);
            BULK_WRITE_OPERATION_INIT = IMPL_LOOKUP.findConstructor(BulkWriteOperation.class, type);
            type = MethodType.methodType(BulkWriteResult.class, boolean.class, List.class, WriteConcern.class, DBEncoder.class);
            BULK_WRITE_OPERATION_SETLIST = IMPL_LOOKUP.findSetter(BulkWriteOperation.class, "requests", List.class);
        } catch (ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final DBCollection delegate;

    private final DBObject scope;

    public ScopedDBCollection(DBCollection delegate, DBObject scope) {
        super(delegate.getDB(), delegate.getName());
        this.delegate = delegate;
        this.scope = scope;
    }

    private Method doapply;

    protected DBObject injectScope(DBObject object) {
        // Using keySet because it's cached, and doesn't copy (unlike toMap())
        if (object != null && this.scope.keySet().size() > 0) {
            object.putAll(this.scope);
        }
        return object;
    }

    @Override
    public WriteResult insert(List<DBObject> list, WriteConcern writeConcern, DBEncoder dbEncoder) {
        list.forEach(this::injectScope);
        return this.delegate.insert(list, writeConcern, dbEncoder);
    }

    @Override
    public WriteResult update(DBObject query, DBObject update, boolean upsert, boolean multi, WriteConcern concern, DBEncoder encoder) {
        this.injectScope(query);
        this.injectScope(update);
        return this.delegate.update(query, update, upsert, multi, concern, encoder);
    }

    @Override
    @Deprecated
    public void doapply(DBObject dbObject) {
        try {
            if (this.doapply == null) {
                this.doapply = this.delegate.getClass().getMethod("doapply", DBObject.class);
                this.doapply.setAccessible(true);
            }
            this.doapply.invoke(this.delegate, dbObject);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public WriteResult remove(DBObject query, WriteConcern writeConcern, DBEncoder dbEncoder) {
        this.injectScope(query);
        return delegate.remove(query, writeConcern, dbEncoder);
    }

    @Override
    QueryResultIterator find(DBObject query, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPreference, DBDecoder encoder) {
        return this.delegate.find(this.injectScope(query), fields, numToSkip, batchSize, limit, options, readPreference, encoder);
    }

    @Override
    QueryResultIterator find(DBObject query, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPreference, DBDecoder decoder, DBEncoder encoder) {
        return this.delegate.find(this.injectScope(query), fields, numToSkip, batchSize, limit, options, readPreference, decoder, encoder);
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert, long maxTime, TimeUnit maxTimeUnit) {
        this.injectScope(query);
        this.injectScope(update);
        return delegate.findAndModify(query, fields, sort, remove, update, returnNew, upsert, maxTime, maxTimeUnit);
    }

    @Override
    public void createIndex(DBObject keys, DBObject options, DBEncoder encoder) {
        this.delegate.createIndex(keys, options, encoder);
    }

    @Override
    public DBCursor find(DBObject ref) {
        this.injectScope(ref);
        return delegate.find(ref);
    }

    @Override
    public DBCursor find(DBObject ref, DBObject keys) {
        this.injectScope(ref);
        return delegate.find(ref, keys);
    }

    @Override
    public DBCursor find() {
        DBObject ref = new BasicDBObject();
        this.injectScope(ref);
        return this.find(ref);
    }

    @Override
    public DBObject findOne(DBObject query, DBObject fields, DBObject orderBy, ReadPreference readPref) {
        this.injectScope(query);
        return this.delegate.findOne(query, fields, orderBy, readPref);
    }

    @Override
    public long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs) {
        this.injectScope(query);
        return delegate.getCount(query, fields, limit, skip, readPrefs);
    }

    @Override
    public DBCollection rename(String newName, boolean dropTarget) {
        return new ScopedDBCollection(this.delegate.rename(newName, dropTarget), this.scope);
    }

    @Override
    public DBObject group(GroupCommand cmd, ReadPreference readPrefs) {
        DBObject obj = cmd.toDBObject();
        DBObject keys = (DBObject) obj.get("key");
        DBObject condition = (DBObject) obj.get("cond");
        DBObject initial = (DBObject) obj.get("initial");
        String reduce = (String) obj.get("$reduce");
        String finalize = (String) obj.get("finalize");
        return this.delegate.group(new GroupCommand(this.delegate, keys, condition, initial, reduce, finalize), readPrefs);
    }

    @Override
    @Deprecated
    public DBObject group(DBObject args) {
        throw new UnsupportedOperationException("Try something not deprecated");
    }

    @Override
    public List distinct(String key, DBObject query, ReadPreference readPrefs) {
        this.injectScope(query);
        return this.delegate.distinct(key, query, readPrefs);
    }

    @Override
    public MapReduceOutput mapReduce(MapReduceCommand command) {
        this.injectScope(command.getQuery());
        return delegate.mapReduce(command);
    }

    @Override
    @Deprecated
    public MapReduceOutput mapReduce(DBObject command) {
        throw new UnsupportedOperationException("Try something not deprecated");
    }

    @Override
    public AggregationOutput aggregate(List<DBObject> pipeline, ReadPreference readPreference) {
        pipeline.add(0, new BasicDBObject().append("$match", this.injectScope(new BasicDBObject())));
        return delegate.aggregate(pipeline, readPreference);
    }

    @Override
    public Cursor aggregate(List<DBObject> list, AggregationOptions aggregationOptions, ReadPreference readPreference) {
        list.add(0, new BasicDBObject().append("$match", this.injectScope(new BasicDBObject())));
        return this.delegate.aggregate(list, aggregationOptions, readPreference);
    }

    @Override
    public CommandResult explainAggregate(List<DBObject> pipeline, AggregationOptions options){
        pipeline.add(0, new BasicDBObject().append("$match", this.injectScope(new BasicDBObject())));
        return this.delegate.explainAggregate(pipeline, options);
    }

    @Override
    public List<Cursor> parallelScan(ParallelScanOptions parallelScanOptions) {
        return this.delegate.parallelScan(parallelScanOptions);
    }

    private BulkWriteOperation overrideRequestsList(BulkWriteOperation op) throws Throwable{
        BULK_WRITE_OPERATION_SETLIST.invoke(op, new ArrayList<WriteRequest>() {
            @Override
            public boolean add(WriteRequest request) {
                switch (request.getType()) {
                    case INSERT:
                        ScopedDBCollection.this.injectScope(((InsertRequest) request).getDocument());
                        break;
                    case UPDATE:
                    case REPLACE:
                        ScopedDBCollection.this.injectScope(((ModifyRequest) request).getQuery());
                        ScopedDBCollection.this.injectScope(((ModifyRequest) request).getUpdateDocument());
                        break;
                    case REMOVE:
                        ScopedDBCollection.this.injectScope(((RemoveRequest) request).getQuery());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown WriteRequest type " + request.getType().name());
                }
                return super.add(request);
            }
        });
        return op;
    }

    @Override
    public BulkWriteOperation initializeOrderedBulkOperation() {
        try {
            return overrideRequestsList((BulkWriteOperation) BULK_WRITE_OPERATION_INIT.invoke(true, this.delegate));
        } catch (Throwable ex) {
            throw new MongoException("Failed to initialize BulkWriteOperation on ScopedDBCollection", ex);
        }
    }

    @Override
    public BulkWriteOperation initializeUnorderedBulkOperation() {
        try {
            return overrideRequestsList((BulkWriteOperation) BULK_WRITE_OPERATION_INIT.invoke(false, this.delegate));
        } catch (Throwable ex) {
            throw new MongoException("Failed to initialize BulkWriteOperation on ScopedDBCollection", ex);
        }
    }

    @Override
    public BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> requests, WriteConcern writeConcern, DBEncoder encoder) {
        /*requests.forEach(request -> {
            switch (request.getType()) {
                case INSERT:
                    ScopedDBCollection.this.injectScope(((InsertRequest) request).getDocument());
                    break;
                case UPDATE:
                case REPLACE:
                    ScopedDBCollection.this.injectScope(((ModifyRequest) request).getQuery());
                    ScopedDBCollection.this.injectScope(((ModifyRequest) request).getUpdateDocument());
                    break;
                case REMOVE:
                    ScopedDBCollection.this.injectScope(((RemoveRequest) request).getQuery());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown WriteRequest type " + request.getType().name());
            }
        });
        try {
            return (BulkWriteResult) BULK_WRITE_OPERATION_EXECUTE.invokeExact(this.delegate, ordered, requests, writeConcern, encoder);
        } catch (Throwable t) {
            throw new MongoException("Failed to execute BulkWriteOperation on ScopedDBCollection", t);
        }*/
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DBObject> getIndexInfo() {
        return this.delegate.getIndexInfo();
    }

    @Override
    public void dropIndex(DBObject keys) {
        this.delegate.dropIndex(keys);
    }

    @Override
    public void dropIndex(String indexName) {
        this.delegate.dropIndex(indexName);
    }

    @Override
    public CommandResult getStats() {
        return this.delegate.getStats();
    }

    @Override
    public boolean isCapped() {
        return this.delegate.isCapped();
    }

    @Override
    public DBCollection getCollection(String n) {
        return new ScopedDBCollection(this.delegate.getCollection(n), this.scope);
    }

    @Override
    public String getName() {
        return this.delegate.getName();
    }

    @Override
    public String getFullName() {
        return this.delegate.getFullName();
    }

    @Override
    public DB getDB() {
        return this.delegate.getDB();
    }

    @Override
    public int hashCode() {
        return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this.delegate.equals(o);
    }

    @Override
    public String toString() {
        return this.delegate.toString();
    }

    @Override
    public void setObjectClass(Class c) {
        this.delegate.setObjectClass(c);
    }

    @Override
    public Class getObjectClass() {
        return this.delegate.getObjectClass();
    }

    @Override
    public void setInternalClass(String path, Class c) {
        this.delegate.setInternalClass(path, c);
    }

    @Override
    public void setWriteConcern(WriteConcern writeConcern) {
        this.delegate.setWriteConcern(writeConcern);
    }

    @Override
    public WriteConcern getWriteConcern() {
        return this.delegate.getWriteConcern();
    }

    @Override
    public void setReadPreference(ReadPreference preference) {
        this.delegate.setReadPreference(preference);
    }

    @Override
    public ReadPreference getReadPreference() {
        return this.delegate.getReadPreference();
    }

    @Override
    @Deprecated
    public void slaveOk() {
        this.delegate.slaveOk();
    }

    @Override
    public void addOption(int option) {
        this.delegate.addOption(option);
    }

    @Override
    public void setOptions(int options) {
        this.delegate.setOptions(options);
    }

    @Override
    public void resetOptions() {
        this.delegate.resetOptions();
    }

    @Override
    public int getOptions() {
        return this.delegate.getOptions();
    }

    @Override
    public void setDBDecoderFactory(DBDecoderFactory fact) {
        this.delegate.setDBDecoderFactory(fact);
    }

    @Override
    public DBDecoderFactory getDBDecoderFactory() {
        return this.delegate.getDBDecoderFactory();
    }

    @Override
    public void setDBEncoderFactory(DBEncoderFactory fact) {
        this.delegate.setDBEncoderFactory(fact);
    }

    @Override
    public DBEncoderFactory getDBEncoderFactory(){
        return this.delegate.getDBEncoderFactory();
    }
}
