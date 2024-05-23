package goredis

import (
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"
)

// N Shards value
const nShards = 1000

type server struct {
	listener net.Listener
	logger   *slog.Logger

	started      atomic.Bool
	clients      map[int64]net.Conn
	lastClientId int64
	clientsLock  sync.Mutex
	shuttingDown bool

	dbLock   [nShards]sync.RWMutex
	database [nShards]map[string]string
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	server := &server{
		listener: listener,
		logger:   logger,

		started:      atomic.Bool{},
		clients:      make(map[int64]net.Conn),
		lastClientId: 0,
		clientsLock:  sync.Mutex{},
		shuttingDown: false,

		dbLock:   [nShards]sync.RWMutex{},
		database: [nShards]map[string]string{},
	}

	for i := 0; i < nShards; i++ {
		server.database[i] = make(map[string]string)
	}

	return server
}

func (server *server) Start() error {

	if !server.started.CompareAndSwap(false, true) {
		return fmt.Errorf("ser	ver already started")
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

	if !server.started.Load() {
		return fmt.Errorf("server not started yet")
	}

	if server.shuttingDown {
		return fmt.Errorf("already shutting down")
	}
	server.shuttingDown = true

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

func unsafeToUpper(s string) {
	bytes := unsafe.Slice(unsafe.StringData(s), len(s))

	for i := 0; i < len(bytes); i++ {
		b := bytes[i]
		if b >= 'a' && b <= 'z' {
			b = b + 'A' - 'a'
			bytes[i] = b
		}
	}
}

func (server *server) handleConn(clientId int64, conn net.Conn) {
	slog.Info(
		"client connected",
		slog.Int64("clientId", clientId),
		slog.String("host", conn.RemoteAddr().String()),
	)

	reader := newMessageReader(conn)
	writer := newBufferWriter(conn)
	go func() {
		// TODO: handle shutdown for the buffered writer.
		if err := writer.Start(); err != nil {
			server.logger.Error(
				"buffered writer error",
				slog.Int64("id", clientId),
				slog.String("err", err.Error()),
			)
		}
	}()

	for {
		length, err := reader.ReadArrayLength()
		if err != nil {
			break
		}

		if length < 1 {
			break
		}

		commandName, err := reader.ReadString()
		if err != nil {
			break
		}

		unsafeToUpper(commandName)

		switch commandName {
		case "GET":
			err = server.handleGetCommand(reader, writer)
		case "SET":
			err = server.handleSetMethod(reader, writer)
		default:
			server.logger.Error("unknown command", slog.String("command", commandName), slog.Int64("clientId", clientId))
			_, err = writer.Write([]byte("-ERR unknown command\r\n"))

			for i := 1; i < length; i++ {
				if _, err = reader.ReadString(); err != nil {
					break
				}
			}
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

func (server *server) handleGetCommand(reader *messageReader, conn io.Writer) error {
	key, err := reader.ReadString()
	if err != nil {
		return err
	}

	shard := calculateShard(key)
	server.dbLock[shard].RLock()
	value, ok := server.database[shard][key]
	server.dbLock[shard].RUnlock()

	if ok {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		_, err = conn.Write([]byte(resp))
	} else {
		_, err = conn.Write([]byte("_\r\n"))
	}

	return err
}

func calculateShard(s string) int {
	hasher := fnv.New64()
	_, _ = hasher.Write([]byte(s))
	hash := hasher.Sum64()
	return int(hash % uint64(nShards))
}

func (server *server) handleSetMethod(reader *messageReader, conn io.Writer) error {
	key, err := reader.ReadString()
	if err != nil {
		return err
	}

	value, err := reader.ReadString()
	if err != nil {
		return err
	}

	shard := calculateShard(key)
	server.dbLock[shard].Lock()
	server.database[shard][key] = value
	server.dbLock[shard].Unlock()

	_, err = conn.Write([]byte("+OK\r\n"))
	return err

}
