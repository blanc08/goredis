package goredis

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

type server struct {
	listener net.Listener
	logger   *slog.Logger

	started      atomic.Bool
	clients      map[int64]net.Conn
	lastClientId int64
	clientsLock  sync.Mutex
	shuttingDown bool
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	return &server{
		listener: listener,
		logger:   logger,

		started:      atomic.Bool{},
		clients:      make(map[int64]net.Conn),
		lastClientId: 0,
		clientsLock:  sync.Mutex{},
		shuttingDown: false,
	}
}

func (server *server) Start() error {

	if !server.started.CompareAndSwap(false, true) {
		return fmt.Errorf("server already started")
	}

	server.logger.Info("server started")

	for {
		conn, err := server.listener.Accept()
		if err != nil {
			server.clientsLock.Lock()
			isShuttingDown := server.shuttingDown
			server.clientsLock.Unlock()

			if !isShuttingDown {
				return err
			}

			return nil
		}

		server.clientsLock.Lock()
		server.lastClientId += 1
		clientId := server.lastClientId
		server.clients[clientId] = conn
		server.clientsLock.Unlock()
		go server.handleConn(clientId, conn)
	}

}

func (server *server) Stop() error {
	server.clientsLock.Lock()
	defer server.clientsLock.Unlock()

	if server.shuttingDown {
		return fmt.Errorf("already shutting down")
	}

	for clientId, conn := range server.clients {
		server.logger.Info(
			"closing client",
			slog.Int64("clientId", clientId),
		)

		if err := conn.Close(); err != nil {
			server.logger.Error(
				"cannot close connection",
				slog.Int64("clientId", clientId),
				slog.String("error", err.Error()),
			)
		}
	}
	clear(server.clients)

	if err := server.listener.Close(); err != nil {
		server.logger.Error(
			"cannot stop listener",
			slog.String("error", err.Error()),
		)
	}

	return nil
}

func (server *server) handleConn(clientId int64, conn net.Conn) {
	slog.Info(
		"client connected",
		slog.Int64("clientId", clientId),
		slog.String("host", conn.RemoteAddr().String()),
	)

	for {
		buff := make([]byte, 4096)
		n, err := conn.Read(buff)
		if err != nil {
			// TODO : handle the error
			break
		}

		if n == 0 {
			// TODO: this means the client is closing the connection
			break
		}

		if _, err := conn.Write(buff[:n]); err != nil {
			// TODO: handle the errror
			break
		}
	}

	server.clientsLock.Lock()
	if _, ok := server.clients[clientId]; !ok {
		server.clientsLock.Unlock()
		return
	}

	if err := conn.Close(); err != nil {
		server.logger.Error(
			"cannot close client",
			slog.Int64("clientId", clientId),
			slog.String("error", err.Error()),
		)
	}

}
