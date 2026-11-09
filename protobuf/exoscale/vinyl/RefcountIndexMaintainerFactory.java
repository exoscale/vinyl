package exoscale.vinyl;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;

@AutoService(IndexMaintainerFactory.class)
public class RefcountIndexMaintainerFactory implements IndexMaintainerFactory {

    private static final List<String> TYPES = Collections.singletonList("refcount");

    @Override
    public Iterable<String> getIndexTypes() {
        return TYPES;
    }

    @Override
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index);
    }

    @Override
    public IndexMaintainer getIndexMaintainer(IndexMaintainerState state) {
        return new RefcountIndexMaintainer(state);
    }
}
