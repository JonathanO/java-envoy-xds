package net.woaf.jono.envoy.testing.utils;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Easy GRPC interceptors: just override the relevant onSomething methods.
 */
public abstract class SimpleGRpcInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(SimpleGRpcInterceptor.class);

    /** For subclasses. */
    protected SimpleGRpcInterceptor() {
    }

    /**
     * A message went past.
     * @param callRole our side of the call
     * @param dir whether we received or sent it
     * @param message the message
     */
    protected void onMessage(CallRole callRole, MessageDirection dir, Object message) {
        // noop
    }

    /**
     * A call started.
     * @param callRole our side of the call
     * @param method the method called
     */
    protected void onCallStart(CallRole callRole, MethodDescriptor<?, ?> method) {
        // noop
    }

    /**
     * A server call started; by default passes it on as {@link #onCallStart(CallRole, MethodDescriptor)}.
     * @param call the call
     */
    protected void onCallStart(ServerCall<?, ?> call) {
        onCallStart(CallRole.SERVER, call.getMethodDescriptor());
    }

    /**
     * A call ended.
     * @param callRole our side of the call
     * @param method the method called
     */
    protected void onCallEnd(CallRole callRole, MethodDescriptor<?, ?> method) {
        // noop
    }

    /**
     * A server call ended; by default passes it on as {@link #onCallEnd(CallRole, MethodDescriptor)}.
     * @param call the call
     */
    protected void onCallEnd(ServerCall<?, ?> call) {
        onCallEnd(CallRole.SERVER, call.getMethodDescriptor());
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            onCallStart(call);
        } catch (Exception e) {
            log.error("Error while calling onCallStart()", e);
        }

        var interceptingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void sendMessage(RespT message) {
                try {
                    SimpleGRpcInterceptor.this.onMessage(CallRole.SERVER, MessageDirection.OUT, message);
                } catch (Exception e) {
                    log.error("Error while calling onMessage()", e);
                }

                super.sendMessage(message);
            }
        };

        var listener = next.startCall(interceptingCall, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onCancel() {
                try {
                    SimpleGRpcInterceptor.this.onCallEnd(call);
                } catch (Exception e) {
                    log.error("Error while calling onCallEnd()", e);
                }

                super.onCancel();
            }

            @Override
            public void onComplete() {
                try {
                    SimpleGRpcInterceptor.this.onCallEnd(call);
                } catch (Exception e) {
                    log.error("Error while calling onCallEnd()", e);
                }

                super.onComplete();
            }

            @Override
            public void onMessage(ReqT message) {
                try {
                    SimpleGRpcInterceptor.this.onMessage(CallRole.SERVER, MessageDirection.IN, message);
                } catch (Exception e) {
                    log.error("Error while calling onMessage()", e);
                }

                super.onMessage(message);
            }
        };
    }

    /** Which side of a call we are. */
    public enum CallRole {
        /** We made the call. */
        CLIENT,
        /** We received it. */
        SERVER
    }

    /** Which way a message went. */
    public enum MessageDirection {
        /** We received it. */
        IN,
        /** We sent it. */
        OUT
    }
}