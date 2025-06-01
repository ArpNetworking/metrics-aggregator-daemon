package com.arpnetworking.metrics.mad.sources.ratelimiter;

public class AcceptListStoreUpdater implements Updater {
    private final AcceptListStore _store;
    private final AcceptListSink _sink;
    private final AcceptListSource _source;

    public AcceptListStoreUpdater(final AcceptListStore store, final AcceptListSink sink, final AcceptListSource source) {
        _store = store;
        _sink = sink;
        _source = source;
    }

    @Override
    public void run() {
        _sink.updateAcceptList(_store.updateAndReturnCurrent(_source.getAcceptList()));
    }
}
