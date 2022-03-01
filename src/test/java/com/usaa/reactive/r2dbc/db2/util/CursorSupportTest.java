package com.usaa.reactive.r2dbc.db2.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Cursor;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.subscriber.TestSubscriber;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class CursorSupportTest {
    private static final int FETCH_SIZE = 42;

    @Mock
    private RowSet<Row> rowSet;

    @Test
    public void testIterateCursor() {
        MockCursor cursor = new MockCursor();
        TestSubscriber<RowSet<Row>> subscriber = TestSubscriber.builder().initialRequest(0).build();
        // Adapt the cursor to a Flux
        Flux<RowSet<Row>> flux = CursorSupport.iterateCursor(cursor, FETCH_SIZE);
        // Nothing should happen until we subscribe to the flux
        assertEquals(0, cursor.getReadCalledCount(), "Cursor shouldn't have been called since we haven't yet subbscribed to the Flux");
        assertFalse(cursor.isReadPending(), "There shouldn't be a pending read");
        // Subscribing to the flux should kick off the first read - not because our subscriber issued a request, but
        // because the Flux.concat() in CursorSupport was allowed to prefetch 1 item
        flux.subscribe(subscriber);
        assertEquals(1, cursor.getReadCalledCount(), "Read should have been called with the subbscription");
        assertEquals(FETCH_SIZE, cursor.getLastReadFetchCount(), "Incorrect fetch size passed to cursor");
        assertTrue(cursor.isReadPending(), "Read should be pending");
        // Now, let's complete the read
        cursor.completePendingRead(rowSet, true);
        // But no items should have been received by the subscriber yet (remember, we haven't yet signalled any demand)
        assertThat("No demand has been signalled, Flux should not have called onNext() yet",
                subscriber.getReceivedOnNext(), empty());
        // And we shouldn't have started the next read yet, since we are experiencing backpressure from the subscriber
        assertEquals(1, cursor.getReadCalledCount(), "Next read shouldn't have started yet");
        assertFalse(cursor.isReadPending(), "Next read shouldn't have started yet");
        // Ok, now we'll issue the request for the first item
        subscriber.request(1);
        // And we should have it now
        assertThat("Flux should have called onNext() with the first item",
                subscriber.getReceivedOnNext(), hasSize(1));
        // Now that we've signalled demand and consumed the result of the first read, the second call will have been made
        assertEquals(2, cursor.getReadCalledCount(), "Next read should have been triggered");
        assertEquals(FETCH_SIZE, cursor.getLastReadFetchCount(), "Incorrect fetch size passed to cursor");
        assertTrue(cursor.isReadPending(), "Read should be pending");
        // Let's go ahead and signal more demand (so we're ready for the second item). This will test the code path
        // where the 3rd read will be triggered immediately when the 2nd completes
        subscriber.request(1);
        // Now, let's complete the 2nd read
        cursor.completePendingRead(rowSet, true);
        // We should see the item immediately
        assertThat("Flux should have called onNext() with the second item",
                subscriber.getReceivedOnNext(), hasSize(2));
        // And the 3rd read should already be cooking
        assertEquals(3, cursor.getReadCalledCount(), "Next read should have been triggered");
        assertEquals(FETCH_SIZE, cursor.getLastReadFetchCount(), "Incorrect fetch size passed to cursor");
        assertTrue(cursor.isReadPending(), "Read should be pending");
        // Complete the 3rd read, indicating that there are no more items
        cursor.completePendingRead(rowSet, false);
        // We haven't signalled demand for the 3rd item yet, so we shouldn't see it yet
        assertThat("No additional demand has been signalled, Flux should not have called onNext() yet",
                subscriber.getReceivedOnNext(), hasSize(2));
        // And here's the demand for that 3rd and final item
        subscriber.request(1);
        // So here it comes
        assertThat("Flux should have called onNext() with the third item",
                subscriber.getReceivedOnNext(), hasSize(3));
        // But this time, we won't have called read again because hasMore was false
        assertEquals(3, cursor.getReadCalledCount(), "Cursor has no more items, another read should not have been triggered");
        assertFalse(cursor.isReadPending(), "Read should not be pending");
        // In fact, we should have been provided with the completion signal
        assertTrue(subscriber.isTerminatedComplete(), "Subscriber should be complete");
        // And hopefully we didn't violate the Reactive Streams spec anywhere along the way
        assertThat("Flux violated Reactive Streams protocol",
                subscriber.getProtocolErrors(), empty());
    }


    private static class MockCursor implements Cursor {
        @Getter
        private Promise<RowSet<Row>> readPromise;
        @Getter
        private int lastReadFetchCount = -1;
        @Getter
        private int readCalledCount = 0;
        @Getter
        private int closeCalledCount = 0;
        @Getter
        private Promise<Void> closePromise;

        @Setter
        private boolean hasMore = false;
        @Setter
        private boolean isClosed = false;

        public boolean isReadPending() {
            return readPromise != null;
        }

        public void completePendingRead(RowSet<Row> rowSet, boolean hasMore) {
            if (!isReadPending()) {
                throw new IllegalStateException("No read is currently pending");
            }
            setHasMore(hasMore);
            // We have to null out the promise from the class var before completing the promise, since completing it may trigger another read
            Promise<RowSet<Row>> temp = this.readPromise;
            this.readPromise = null;
            temp.complete(rowSet);
        }

        public boolean isClosePending() {
            return closePromise != null;
        }

        public void completePendingClose() {
            if (!isClosePending()) {
                throw new IllegalStateException("No read is currently pending");
            }
            // We have to null out the promise from the class var before completing the promise, since completing it may trigger side effects like another close
            Promise<Void> temp = this.closePromise;
            this.closePromise = null;
            temp.complete();
        }

        @Override
        public Future<RowSet<Row>> read(int count) {
            if (isReadPending()) {
                throw new IllegalStateException("Another read is still pending");
            }
            readCalledCount++;
            readPromise = Promise.promise();
            lastReadFetchCount = count;
            return readPromise.future();
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public Future<Void> close() {
            if (isClosePending()) {
                throw new IllegalStateException("Close is still pending");
            }
            closeCalledCount++;
            closePromise = Promise.promise();
            return closePromise.future();
        }

        // These handler-based methods just delegate to their future-based counterparts
        @Override
        public void read(int count, Handler<AsyncResult<RowSet<Row>>> handler) {
            Future<RowSet<Row>> fut = read(count);
            if (handler != null) {
                fut.onComplete(handler);
            }
        }

        @Override
        public void close(Handler<AsyncResult<Void>> handler) {
            Future<Void> fut = close();
            if (handler != null) {
                fut.onComplete(handler);
            }
        }
    }
}
