package com.personal.easy.rabbit.connection;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * <p>
 * A single connection factory provides ONE SINGLE connection to a RabbitMQ
 * message broker via TCP.
 * </p>
 *
 * <p>
 * It is recommended by the RabbitMQ documentation (v2.7) to use one single
 * connection within a client and to use one channel for every client thread.
 * </p>
 *
 */
public class SingleConnectionFactory extends ConnectionFactory {

    private enum State {

        /** The factory has never established a connection so far. **/
        NEVER_CONNECTED,

        /**
         * The factory has established a connection in the past but the
         * connection was lost and the factory is currently trying to
         * reestablish the connection.
         */
        CONNECTING,

        /**
         * The factory has established a connection that is currently alive and
         * that can be retrieved.
         */
        CONNECTED,

        /**
         * The factory and its underlying connection are closed and the factory
         * cannot be used to retrieve new connections.
         */
        CLOSED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleConnectionFactory.class);

    public static final int CONNECTION_HEARTBEAT_IN_SEC = 3;

    public static final int CONNECTION_TIMEOUT_IN_MS = 1000;

    public static final int CONNECTION_ESTABLISH_INTERVAL_IN_MS = 500;

    ShutdownListener connectionShutdownListener;

    List<ConnectionListener> connectionListeners;

    volatile Connection connection;

    volatile State state = State.NEVER_CONNECTED;

    private ExecutorService executorService = null;

    private final Object operationOnConnectionMonitor = new Object();

    /** construct method **/
    public SingleConnectionFactory() {
        super();
        setRequestedHeartbeat(CONNECTION_HEARTBEAT_IN_SEC);
        setConnectionTimeout(CONNECTION_TIMEOUT_IN_MS);
        this.connectionListeners = Collections.synchronizedList(new LinkedList<ConnectionListener>());
        this.connectionShutdownListener = new ConnectionShutDownListener();
    }

    /**
     * <p>
     * Gets a new connection from the factory. As this factory only provides one
     * connection for every process, the connection is established on the first
     * call of this method. Every subsequent call will return the same instance
     * of the first established connection.
     * </p>
     *
     * <p>
     * In case a connection is lost, the factory will try to reestablish a new
     * connection.
     * </p>
     */
    @Override
    public Connection newConnection() throws IOException, TimeoutException {
        // Throw an exception if there is an attempt to retrieve a connection
        // from a closed factory
        if (this.state == State.CLOSED) {
            throw new IOException("Attempt to retrieve a connection from a closed connection factory");
        }
        // Try to establish a connection if there was no connection attempt so
        // far
        if (this.state == State.NEVER_CONNECTED) {
            establishConnection();
        }
        // Retrieve the connection if it is established
        if (this.connection != null && this.connection.isOpen()) {
            return this.connection;
        }
        // Throw an exception if no established connection could not be
        // retrieved
        LOGGER.error("Unable to retrieve connection");
        throw new IOException("Unable to retrieve connection");
    }

    /**
     *
     * <p>
     * Closes the connection factory and interrupts all threads associated to
     * it.
     * </p>
     *
     * <p>
     * Note: Make sure to close the connection factory when not used any more as
     * otherwise the connection may remain established and ghost threads may
     * reside.
     * </p>
     *
     * @throws TimeoutException
     */
    @PreDestroy
    public void close() throws TimeoutException {
        synchronized (this.operationOnConnectionMonitor) {
            if (this.state == State.CLOSED) {
                LOGGER.warn("Attempt to close connection factory which is already closed");
                return;
            }
            LOGGER.info("Closing connection factory");
            if (this.connection != null) {
                try {
                    this.connection.close();
                    this.connection = null;
                }
                catch (IOException e) {
                    if (!this.connection.isOpen()) {
                        LOGGER.warn("Attempt to close an already closed connection");
                    }
                    else {
                        LOGGER.error("Unable to close current connection", e);
                    }
                }
            }
            changeState(State.CLOSED);
            LOGGER.info("Closed connection factory");
        }
    }

    /**
     * Registers a connection listener at the factory which is notified about
     * changes of connection states.
     *
     * @param connectionListener
     *            The connection listener
     */
    public void registerListener(final ConnectionListener connectionListener) {
        this.connectionListeners.add(connectionListener);
    }

    /**
     * Removes a connection listener from the factory.
     *
     * @param connectionListener
     *            The connection listener
     */
    public void removeConnectionListener(final ConnectionListener connectionListener) {
        this.connectionListeners.remove(connectionListener);
    }

    /**
     * Sets an {@code ExecutorService} to be used for this connection. If none
     * is set a default one will be used (currently 5 threads). Consuming of
     * messages happens using this {@code ExecutorService}.
     * <p/>
     * Because we don't create a new connection to RabbitMQ every time
     * {@link #newConnection()} is called changing the {@code ExecutorService}
     * would only take effect when the underlying connection is closed.
     * <p/>
     * That is why the {@code ExecutorService} can only be set once. Every
     * further invocation will result in an {@link IllegalStateException}.
     *
     * @param executorService
     *            to use for consuming messages
     */
    public void setExecutorService(final ExecutorService executorService) {
        if (this.executorService != null) {
            throw new IllegalStateException("ExecutorService already set, trying to change it");
        }
        this.executorService = executorService;
    }

    /**
     * @return the {@code ExecutorService} to use
     */
    public ExecutorService getExecutorService() {
        return this.executorService;
    }

    /**
     * Changes the factory state and notifies all connection listeners.
     *
     * @param newState
     *            The new connection factory state
     * @throws TimeoutException
     */
    void changeState(final State newState) throws TimeoutException {
        this.state = newState;
        notifyListenersOnStateChange();
    }

    /**
     * Notifies all connection listener about a state change.
     *
     * @throws TimeoutException
     */
    void notifyListenersOnStateChange() throws TimeoutException {
        LOGGER.debug("Notifying connection listeners about state change to {}", this.state);

        for (ConnectionListener listener : this.connectionListeners) {
            switch (this.state) {
                case CONNECTED:
                    listener.onConnectionEstablished(this.connection);
                    break;
                case CONNECTING:
                    listener.onConnectionLost(this.connection);
                    break;
                case CLOSED:
                    listener.onConnectionClosed(this.connection);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Establishes a new connection.
     *
     * @throws IOException
     *             if establishing a new connection fails
     * @throws TimeoutException
     */
    void establishConnection() throws IOException, TimeoutException {
        synchronized (this.operationOnConnectionMonitor) {
            if (this.state == State.CLOSED) {
                throw new IOException("Attempt to establish a connection with a closed connection factory");
            }
            else if (this.state == State.CONNECTED) {
                LOGGER.warn("Establishing new connection although a connection is already established");
            }

            LOGGER.info("Trying to establish connection to {}:{}", getHost(), getPort());
            this.connection = super.newConnection(this.executorService);
            this.connection.addShutdownListener(this.connectionShutdownListener);
            LOGGER.info("Established connection to {}:{}", getHost(), getPort());
            changeState(State.CONNECTED);
        }
    }

    /**
     * A listener to register on the parent factory to be notified about
     * connection shutdowns.
     */
    private class ConnectionShutDownListener implements ShutdownListener {

        @Override
        public void shutdownCompleted(final ShutdownSignalException cause) {
            // Only hard error means loss of connection
            if (!cause.isHardError()) {
                return;
            }

            synchronized (SingleConnectionFactory.this.operationOnConnectionMonitor) {
                // No action to be taken if factory is already closed
                // or already connecting
                if (SingleConnectionFactory.this.state == State.CLOSED || SingleConnectionFactory.this.state == State.CONNECTING) {
                    return;
                }
                try {
                    changeState(State.CONNECTING);
                }
                catch (TimeoutException e) {
                    LOGGER.error("changeState to State.Connectiong error", e);
                }
            }
            LOGGER.error("Connection to {}:{} lost", getHost(), getPort());
            while (SingleConnectionFactory.this.state == State.CONNECTING) {
                try {
                    establishConnection();
                    return;
                }
                catch (IOException e) {
                    LOGGER.info("Next reconnect attempt in {} ms", CONNECTION_ESTABLISH_INTERVAL_IN_MS);
                    try {
                        Thread.sleep(CONNECTION_ESTABLISH_INTERVAL_IN_MS);
                    }
                    catch (InterruptedException ie) {
                        return;
                    }
                }
                catch (TimeoutException e) {
                    LOGGER.error("establishConnection timeout", e);
                }
            }
        }
    }
}
