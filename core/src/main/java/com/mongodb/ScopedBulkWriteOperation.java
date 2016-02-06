package com.mongodb;

/**
 * @author DarkSeraphim.
 */
public class ScopedBulkWriteOperation extends BulkWriteOperation {

    private final ScopedDBCollection scopedCollection;

    ScopedBulkWriteOperation(boolean ordered, ScopedDBCollection collection) {
        super(ordered, collection);
        this.scopedCollection = collection;
    }

    @Override
    void addRequest(WriteRequest request) {
        switch (request.getType()) {
            case INSERT:
                this.scopedCollection.injectScope(((InsertRequest) request).getDocument());
                break;
            case UPDATE:
            case REPLACE:
                this.scopedCollection.injectScope(((ModifyRequest) request).getQuery());
                this.scopedCollection.injectScope(((ModifyRequest) request).getUpdateDocument());
                break;
            case REMOVE:
                this.scopedCollection.injectScope(((RemoveRequest) request).getQuery());
                break;
            default:
                throw new UnsupportedOperationException("Unknown WriteRequest type " + request.getType().name());
        }
        super.addRequest(request);
    }
}
