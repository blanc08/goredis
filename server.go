package goredis

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
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
		request, err := readArray(conn, true)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				server.logger.Error(
					"error reading from client",
					slog.Int64("clientId", clientId),
					slog.String("err", err.Error()),
				)
			}
			break
		}

		server.logger.Debug(
			"request received",
			slog.Any("request", request),
			slog.Int64("clientId", clientId),
		)

		if len(request) == 0 {
			server.logger.Error("missing command in the request", slog.Int64("clientId", clientId))
		}

		commandName, ok := request[0].(string)
		if !ok {
			server.logger.Error("invalid command name", slog.Int64("clientId", clientId))
			break
		}

		switch strings.ToUpper(commandName) {
		case "GET":
			server.logger.Debug("GET command received", slog.Int64("clientId", clientId))
		case "SET":
			server.logger.Debug("SET command received", slog.Int64("clientId", clientId))
		default:
			server.logger.Error("unknown command", slog.String("command", commandName), slog.Int64("clientId", clientId))
			break
		}

		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			server.logger.Error(
				"error writing to client",
				slog.Int64("clientId", clientId),
				slog.String("err", err.Error()),
			)
			break
		}
	}

	server.clientsLock.Lock()
	if _, ok := server.clients[clientId]; !ok {
		server.clientsLock.Unlock()
		return
	}
	delete(server.clients, clientId)
	server.clientsLock.Unlock()

	server.logger.Info("client disconnecting", slog.Int64("clientId", clientId))
	if err := conn.Close(); err != nil {
		server.logger.Error(
			"cannot close client",
			slog.Int64("clientId", clientId),
			slog.String("error", err.Error()),
		)
	}

}
