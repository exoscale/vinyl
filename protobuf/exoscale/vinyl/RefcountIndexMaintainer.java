package exoscale.vinyl;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class RefcountIndexMaintainer extends StandardIndexMaintainer {

    public static byte[] EMPTY_VALUE = new byte[]{};
    public static byte[] LITTLE_ENDIAN_INT64_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0};

    private final Subspace refCountSubspace;
    private final Subspace zeroSubspace;

    protected RefcountIndexMaintainer(IndexMaintainerState state) {
        super(state);
        refCountSubspace = getIndexSubspace().subspace(Tuple.from("refcount"));
        zeroSubspace = getIndexSubspace().subspace(Tuple.from("zero"));
    }

    public Object toRecord(FDBIndexableRecord<? extends Message> indexableRecord) {
        String type = indexableRecord.getRecordType().getName();
        return switch (type) {
            case "Invoice" -> Demostore.Invoice.newBuilder().mergeFrom(indexableRecord.getRecord()).build();
            case "Object" -> Demostore.Object.newBuilder().mergeFrom(indexableRecord.getRecord()).build();
            default -> throw new IllegalArgumentException("Unsupported record type");
        };
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(FDBIndexableRecord<M> oldRecord, FDBIndexableRecord<M> newRecord) {
        Transaction transaction = state.transaction;
        if (newRecord != null && oldRecord == null) {
            Object record = toRecord(newRecord);
            if (record instanceof Demostore.Invoice) {
                Demostore.Invoice invoice = (Demostore.Invoice) record;
                invoice.getLinesList().forEach(e -> {
                    increment(transaction, e.getProduct());
                });
            } else {
                Demostore.Object object = (Demostore.Object) record;
                increment(transaction, object.getBucket());
            }
        } else if (newRecord == null && oldRecord != null) {
            Object record = toRecord(oldRecord);
            if (record instanceof Demostore.Invoice) {
                Demostore.Invoice invoice = (Demostore.Invoice) record;
                for (Demostore.InvoiceLine e : invoice.getLinesList()) {
                    decrement(transaction, e.getProduct());
                }
            } else {
                Demostore.Object object = (Demostore.Object) record;
                decrement(transaction, object.getBucket());
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private void increment(Transaction transaction, Object keyObject) {
        try {
            byte[] key = refCountSubspace.pack(keyObject);
            transaction.mutate(MutationType.ADD, key, LITTLE_ENDIAN_INT64_ONE);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private void decrement(Transaction transaction, Object keyObject) {
        try {
            byte[] key = refCountSubspace.pack(keyObject);
            byte[] value = transaction.get(key).get();
            long newRefcount = ByteArrayUtil.decodeInt(value) - 1;
            if (newRefcount == 0) {
                transaction.clear(refCountSubspace.pack(keyObject));
                transaction.set(zeroSubspace.pack(keyObject), EMPTY_VALUE);
            } else {
                transaction.set(key, ByteArrayUtil.encodeInt(newRefcount));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Nonnull
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType indexScanType, @Nonnull TupleRange tupleRange, byte[] continuation, @Nonnull ScanProperties scanProperties) {
        RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(getIndexSubspace()).setContext(this.state.context).setRange(tupleRange).setContinuation(continuation).setScanProperties(scanProperties).build();
        return keyValues.map((kv) -> {
            this.state.store.countKeyValue(FDBStoreTimer.Counts.LOAD_INDEX_KEY, FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, kv);
            return this.unpackKeyValue(kv);
        });
    }

    @Override
    public boolean isIdempotent() {
        return false;
    }

    @Nonnull
    @Override
    protected IndexEntry unpackKeyValue(@Nonnull KeyValue kv) {
        if (Arrays.equals(kv.getValue(), EMPTY_VALUE)) {
            return new IndexEntry(this.state.index, SplitHelper.unpackKey(getIndexSubspace(), kv), Tuple.from(0));
        } else {
            return new IndexEntry(this.state.index, SplitHelper.unpackKey(getIndexSubspace(), kv), Tuple.from(ByteArrayUtil.decodeInt(kv.getValue())));
        }
    }
}
