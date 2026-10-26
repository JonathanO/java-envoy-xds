package net.woaf.jono.envoy.testing.utils;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A gRPC server interceptor that records every xDS stream, as a {@link Conversation} each.
 */
public class ConversationLogger implements ServerInterceptor {

    private final List<Conversation> conversations = Collections.synchronizedList(new ArrayList<>());

    /** Records nothing yet. */
    public ConversationLogger() {
    }


    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {
        Conversation conversation = new Conversation();
        conversations.add(conversation);
        ConversationCallInterceptor listener = new ConversationCallInterceptor(conversation);
        return listener.interceptCall(call, headers, next);
    }

    /**
     * {@return the streams so far, oldest first}
     */
    public ImmutableList<Conversation> getConversations() {
        return ImmutableList.copyOf(conversations);
    }

    /**
     * One stream's messages, in order: requests from Envoy and our responses.
     */
    public static class Conversation {

        @GuardedBy("this")
        private final List<Object> messages = new ArrayList<>();
        @GuardedBy("this")
        private boolean isFinished = false;
        @GuardedBy("this")
        private long lastMessageNanos = System.nanoTime();

        /** Creates an empty conversation. */
        public Conversation() {
        }

        /** The stream was cancelled. */
        public synchronized void onCancel() {
            isFinished = true;
        }

        /** The stream completed. */
        public synchronized void onComplete() {
            isFinished = true;
        }

        /**
         * Envoy sent a request.
         * @param request the request
         */
        public synchronized void onRequest(Object request) {
            messages.add(request);
            lastMessageNanos = System.nanoTime();
        }

        /**
         * We sent a response.
         * @param response the response
         */
        public synchronized void onResponse(Object response) {
            messages.add(response);
            lastMessageNanos = System.nanoTime();
        }

        /**
         * {@return how many messages so far}
         */
        public synchronized int size() {
            return messages.size();
        }

        /**
         * Whether nothing has been sent either way for a while.
         * @param period how long
         * @return true if the last message is at least that old
         */
        public synchronized boolean quietFor(Duration period) {
            return System.nanoTime() - lastMessageNanos >= period.toNanos();
        }

        /**
         * {@return a copy of the messages so far}
         */
        public synchronized List<Object> getMessages() {
            return ImmutableList.copyOf(messages);
        }

        @Override
        public synchronized String toString() {
            return "Conversation(finished=" + isFinished + ", messages=" + messages + ")";
        }

        /**
         * {@return whether the stream has ended}
         */
        public synchronized boolean isFinished() {
            return isFinished;
        }
    }

    private static class ConversationCallInterceptor {

        private final Conversation conversation;

        ConversationCallInterceptor(Conversation conversation) {
            this.conversation = conversation;
        }

        <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

            var interceptingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                @Override
                public void sendMessage(RespT message) {
                    conversation.onResponse(message);
                    super.sendMessage(message);
                }
            };

            Listener<ReqT> listener = next.startCall(interceptingCall, headers);

            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                @Override
                public void onCancel() {
                    conversation.onCancel();
                    super.onCancel();
                }

                @Override
                public void onComplete() {
                    conversation.onComplete();
                    super.onComplete();
                }

                @Override
                public void onMessage(ReqT message) {
                    conversation.onRequest(message);
                    super.onMessage(message);
                }
            };
        }

    }

}