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
			err = server.handleGetCommand(clientId, conn, request)
		case "SET":
			err = server.handleSetMethod(clientId, conn, request)
		default:
			server.logger.Error("unknown command", slog.String("command", commandName), slog.Int64("clientId", clientId))
			break
		}

		if err != nil {
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

func (server *server) handleGetCommand(clientId int64, conn net.Conn, command []any) error {
	if len(command) < 2 {
		_, err := conn.Write([]byte("--ERR missing key\r\n"))
		return err
	}

	key, ok := command[1].(string)
	if !ok {
		_, err := conn.Write([]byte("--ERR invalid key\r\n"))
		return err
	}

	server.logger.Debug(
		"GET command",
		slog.String("key", key),
		slog.Int64("clientId", clientId),
	)

	// TODO : Get the key here

	_, err := conn.Write([]byte("_\r\n"))
	return err
}

func (server *server) handleSetMethod(clientId int64, conn net.Conn, command []any) error {

	if len(command) < 3 {
		_, err := conn.Write([]byte("-ERR misisng key and value\r\n"))
		return err
	}

	key, ok := command[1].(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR invalid key\r\n"))
		return err
	}

	value, ok := command[2].(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR invalid value\r\n"))
		return err
	}

	server.logger.Debug(
		"SET key into value",
		slog.String("key", key),
		slog.String("value", value),
		slog.Int64("clientId", clientId),
	)

	// TODO: SET the key here

	_, err := conn.Write([]byte("+OK\r\n"))
	return err

}
