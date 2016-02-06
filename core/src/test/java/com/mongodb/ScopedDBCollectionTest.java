package com.mongodb;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TODO: test scope
 * @author DarkSeraphim.
 */
public class ScopedDBCollectionTest {

    @AllArgsConstructor
    @NoArgsConstructor
    private class Reference<T> {
        @Setter
        @Getter
        private T value;
    }

    private static final Map<Class<?>, Supplier<?>> PRIMITIVE_SUPPLIER = new HashMap<Class<?>, Supplier<?>>() {{
        put(boolean.class, () -> true);
        put(byte.class, () -> 0);
        put(short.class, () -> 0);
        put(char.class, () -> '\0');
        put(int.class, () -> 0);
        put(long.class, () -> 0);
        put(float.class, () -> 0);
        put(double.class, () -> 0);
    }};


    private static final Set<String> EXCLUSIONS = new HashSet<>(Arrays.asList("equals", "hashCode", "setHintFields", "dropIndexes", "drop"));

    @Test
    public void delegacyTest() throws ReflectiveOperationException {
        CommandResult result = mock(CommandResult.class);
        DB db = mock(DB.class);
        when(db.command(Mockito.any(DBObject.class))).thenReturn(result);
        DBCollection delegate = mock(DBCollection.class);
        when(delegate.getDB()).thenReturn(db);
        when(delegate.getName()).thenReturn("test");
        when(delegate.rename(anyString(), anyBoolean())).thenReturn(delegate);
        when(delegate.getCollection(anyString())).thenReturn(delegate);
        Reference<Boolean> called = new Reference<>();
        ScopedDBCollection decorator = new ScopedDBCollection(delegate, new BasicDBObject());
        ScopedDBCollection collection = mock(ScopedDBCollection.class, (Answer) invocation -> {
            if ("checkReadOnly".equals(invocation.getMethod().getName())) {
                return false;
            }
            if (invocation.getMethod().getDeclaringClass() == ScopedDBCollection.class) {
                called.setValue(true);
                return invocation.getMethod().invoke(decorator, invocation.getArguments());
            }
            return invocation.callRealMethod();
        });

        for (Method method : DBCollection.class.getMethods()) {
            if (method.isAnnotationPresent(Deprecated.class) || EXCLUSIONS.contains(method.getName()) || method.getDeclaringClass() == Object.class) {
                continue;
            }
//            System.out.println(method.toString());
            called.setValue(false);
            Class<?>[] paramTypes = method.getParameterTypes();
            Object[] params = new Object[paramTypes.length];
            for (int i = 0; i < paramTypes.length; i++) {
                boolean array = paramTypes[i].isArray();
                Class<?> old = paramTypes[i];
                if (array) {
                    paramTypes[i] = paramTypes[i].getComponentType();
                }
                if (paramTypes[i].isPrimitive()) {
                    params[i] = PRIMITIVE_SUPPLIER.get(paramTypes[i]).get();
                } else {
                    if (paramTypes[i].isEnum()) {
                        params[i] = paramTypes[i].getEnumConstants()[0];
                    } else if (Modifier.isFinal(paramTypes[i].getModifiers())) {
                        if (paramTypes[i] == Class.class) {
                            paramTypes[i] = Object.class;
                        } else {
                            params[i] = paramTypes[i].newInstance();
                        }
                    } else {
                        params[i] = PowerMockito.mock(paramTypes[i]);
                    }
                    if (params[i] instanceof GroupCommand) {
                        when(((GroupCommand) params[i]).toDBObject()).thenReturn(new BasicDBObject());
                    }
                }
                if (array) {
                    Object[] data = (Object[]) Array.newInstance(paramTypes[i], 1);
                    paramTypes[i] = old;
                    data[0] = params[i];
                    params[i] = data;
                }
            }
            method.invoke(collection, params);
            assertTrue("Method " + method.toString() + " did not call ScopedDBCollection", called.getValue());
        }
    }

    @Test
    public void testBulk() {
        CommandResult result = mock(CommandResult.class);
        DB db = mock(DB.class);
        when(db.command(Mockito.any(DBObject.class))).thenReturn(result);
        DBCollection delegate = mock(DBCollection.class);
        when(delegate.getDB()).thenReturn(db);
        when(delegate.getName()).thenReturn("test");
        when(delegate.rename(anyString(), anyBoolean())).thenReturn(delegate);
        when(delegate.getCollection(anyString())).thenReturn(delegate);
        DBCollection collection = new ScopedDBCollection(delegate, new BasicDBObject("foo", "bar"));
        when(delegate.initializeUnorderedBulkOperation()).then(invocationOnMock -> {
            Constructor ctor = BulkWriteOperation.class.getDeclaredConstructor(boolean.class, DBCollection.class);
            ctor.setAccessible(true);
            return ctor.newInstance(false, collection);
        });
        OngoingStubbing<BulkWriteResult> stub = when(delegate.executeBulkWriteOperation(anyBoolean(), any(List.class), any(WriteConcern.class), any(DBEncoder.class)));
        stub.then(invocationOnMock -> {
            ((List<WriteRequest>) invocationOnMock.getArguments()[1]).forEach(request -> {
                switch (request.getType())
                {
                    case INSERT:
                        this.testScope(((InsertRequest) request).getDocument());
                        break;
                    case UPDATE:
                    case REPLACE:
                        this.testScope(((ModifyRequest) request).getQuery());
                        this.testScope(((ModifyRequest) request).getUpdateDocument());
                        break;
                    case REMOVE:
                        this.testScope(((RemoveRequest) request).getQuery());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown WriteRequest type " + request.getType().name());
                }
            });
            return mock(BulkWriteResult.class);
        });
        BulkWriteOperation op = collection.initializeUnorderedBulkOperation();
        DBObject add = new BasicDBObject("data", 1).append("foo", "butts");
        DBObject find = new BasicDBObject("a", "b");
        op.find(find).replaceOne(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).upsert().update(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).upsert().replaceOne(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).upsert().updateOne(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).update(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).updateOne(add);
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).remove();
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.find(find).removeOne();
        op.execute();
        op = collection.initializeUnorderedBulkOperation();
        op.insert(add);
        op.execute();
    }

    private void testScope(DBObject object) {
        assertEquals("Scope missing or overwritten", "bar", object.get("foo"));
    }
}
