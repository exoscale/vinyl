package exoscale.vinyl;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class MyIndexMaintainer extends StandardIndexMaintainer {

    protected MyIndexMaintainer(IndexMaintainerState state) {
        super(state);
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(FDBIndexableRecord<M> oldRecord, FDBIndexableRecord<M> newRecord) {
        System.out.println(state.indexSubspace.toString());
        return CompletableFuture.completedFuture(null);
    }

    @Nonnull
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType indexScanType, @Nonnull TupleRange tupleRange, byte[] bytes, @Nonnull ScanProperties scanProperties) {
        return null;
    }
}
