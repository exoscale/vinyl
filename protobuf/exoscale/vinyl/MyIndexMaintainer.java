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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyIndexMaintainer extends StandardIndexMaintainer {

    public static byte[] EMPTY_VALUE = new byte[]{};
    public static byte[] LITTLE_ENDIAN_INT64_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0};

    private final Subspace indexSubspace;
    private final Subspace refCountSubspace;
    private final Subspace zeroSubspace;

    protected MyIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.indexSubspace = indexSubspace(state.indexSubspace);
        refCountSubspace = indexSubspace.subspace(Tuple.from("refcount"));
        zeroSubspace = indexSubspace.subspace(Tuple.from("zero"));
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(FDBIndexableRecord<M> oldRecord, FDBIndexableRecord<M> newRecord) {
        // https://github.com/FoundationDB/fdb-record-layer/blob/e5ba9cd0027b300b346e543cf013e410675d25a4/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/IndexingBase.java#L885
        System.out.println("Udpdating!");
        Transaction transaction = state.transaction;
        if(newRecord != null && oldRecord == null) {
            Demostore.Invoice invoice = (Demostore.Invoice)newRecord.getRecord();
            invoice.getLinesList().forEach(e -> {
                try {
                    byte[] key = refCountSubspace.pack(e.getProduct());
                    transaction.mutate(MutationType.ADD, key, LITTLE_ENDIAN_INT64_ONE);
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });
        } else if(newRecord == null && oldRecord != null) {
            Demostore.Invoice invoice = Demostore.Invoice.newBuilder().mergeFrom(oldRecord.getRecord()).build();
            for (Demostore.InvoiceLine e : invoice.getLinesList()) {
                try {
                    byte[] key = refCountSubspace.pack(e.getProduct());
                    byte[] value = transaction.get(key).get();
                    long newRefcount = ByteArrayUtil.decodeInt(value) - 1;
                    if (newRefcount == 0) {
                        transaction.clear(refCountSubspace.pack(e.getProduct()));
                        transaction.set(zeroSubspace.pack(e.getProduct()), EMPTY_VALUE);
                    } else {
                        transaction.set(key, ByteArrayUtil.encodeInt(newRefcount));
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Nonnull
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType indexScanType, @Nonnull TupleRange tupleRange, byte[] continuation, @Nonnull ScanProperties scanProperties) {
        RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(indexSubspace).setContext(this.state.context).setRange(tupleRange).setContinuation(continuation).setScanProperties(scanProperties).build();
        return keyValues.map((kv) -> {
            this.state.store.countKeyValue(FDBStoreTimer.Counts.LOAD_INDEX_KEY, FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, kv);
            return this.unpackKeyValue(kv);
        });
    }

    @Override
    public boolean isIdempotent() {
        return false;
    }

    private Subspace indexSubspace(Subspace subspace) {
        Tuple tupleSubspace = Tuple.fromBytes(subspace.getKey());
        List<Object> subSubspace = tupleSubspace.getItems().stream().limit(tupleSubspace.getItems().size() - 1).collect(Collectors.toUnmodifiableList());
        String base = tupleSubspace.getString(tupleSubspace.getItems().size()-1).split("#")[0];
        return new Subspace(Tuple.from(Stream.concat(subSubspace.stream(), Stream.of(base)).toArray()));
    }

    @Nonnull
    @Override
    public Subspace getIndexSubspace() {
        return indexSubspace;
    }

    @Nonnull
    @Override
    protected IndexEntry unpackKeyValue(@Nonnull Subspace subspace, @Nonnull KeyValue kv) {
        return unpackKeyValue(kv);
    }

    @Nonnull
    @Override
    protected IndexEntry unpackKeyValue(@Nonnull KeyValue kv) {
        if(Arrays.equals(kv.getValue(), EMPTY_VALUE)) {
            return new IndexEntry(this.state.index, SplitHelper.unpackKey(indexSubspace, kv), Tuple.from(0));
        } else {
            return new IndexEntry(this.state.index, SplitHelper.unpackKey(indexSubspace, kv), Tuple.from(ByteArrayUtil.decodeInt(kv.getValue())));
        }
    }
}
