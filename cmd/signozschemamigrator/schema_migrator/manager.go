package schemamigrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
)

var (
	ErrFailedToGetConn                  = errors.New("failed to get conn")
	ErrFailedToGetHostAddrs             = errors.New("failed to get host addrs")
	ErrFailedToRunOperation             = errors.New("failed to run operation")
	ErrFailedToWaitForMutations         = errors.New("failed to wait for mutations")
	ErrFailedToWaitForDDLQueue          = errors.New("failed to wait for DDL queue")
	ErrFailedToWaitForDistributionQueue = errors.New("failed to wait for distribution queue")
	ErrDistributionQueueError           = errors.New("distribution_queue has entries with error_count != 0 or is_blocked = 1")

	legacyMigrationsTable = "schema_migrations"
	SignozLogsDB          = "signoz_logs"
	SignozMetricsDB       = "signoz_metrics"
	SignozTracesDB        = "signoz_traces"
	SignozMetadataDB      = "signoz_metadata"
	SignozAnalyticsDB     = "signoz_analytics"
	SignozMeterDB         = "signoz_meter"
	Databases             = []string{SignozTracesDB, SignozMetricsDB, SignozLogsDB, SignozMetadataDB, SignozAnalyticsDB, SignozMeterDB}

	InProgressStatus = "in-progress"
	FinishedStatus   = "finished"
	FailedStatus     = "failed"
)

type Mutation struct {
	Database         string    `ch:"database"`
	Table            string    `ch:"table"`
	MutationID       string    `ch:"mutation_id"`
	Command          string    `ch:"command"`
	CreateTime       time.Time `ch:"create_time"`
	PartsToDo        int64     `ch:"parts_to_do"`
	LatestFailReason string    `ch:"latest_fail_reason"`
}

type DistributedDDLQueue struct {
	Entry           string    `ch:"entry"`
	Cluster         string    `ch:"cluster"`
	Query           string    `ch:"query"`
	QueryCreateTime time.Time `ch:"query_create_time"`
	Host            string    `ch:"host"`
	Port            uint16    `ch:"port"`
	Status          string    `ch:"status"`
	ExceptionCode   string    `ch:"exception_code"`
}

type SchemaMigrationRecord struct {
	MigrationID uint64
	UpItems     []Operation
	DownItems   []Operation
}

// MigrationManager is the manager for the schema migrations.
type MigrationManager struct {
	// addrs is the list of addresses of the hosts in the cluster.
	addrs    []string
	addrsMux sync.Mutex
	conn     clickhouse.Conn
	connOpts clickhouse.Options
	conns    map[string]clickhouse.Conn

	clusterName        string
	replicationEnabled bool
	logger             *zap.Logger
	backoff            *backoff.ExponentialBackOff
	development        bool
}

type Option func(*MigrationManager)

// NewMigrationManager creates a new migration manager.
func NewMigrationManager(opts ...Option) (*MigrationManager, error) {
	mgr := &MigrationManager{
		logger: zap.NewNop(),
		// the default backoff is good enough for our use case
		// no mutation should be running for more than 15 minutes, if it is, we should fail fast
		backoff:            backoff.NewExponentialBackOff(),
		replicationEnabled: false,
		conns:              make(map[string]clickhouse.Conn),
	}
	for _, opt := range opts {
		opt(mgr)
	}
	if mgr.conn == nil {
		return nil, errors.New("conn is required")
	}
	return mgr, nil
}

func WithClusterName(clusterName string) Option {
	return func(mgr *MigrationManager) {
		mgr.clusterName = clusterName
	}
}

func WithDevelopment(development bool) Option {
	return func(mgr *MigrationManager) {
		mgr.development = development
	}
}

func WithReplicationEnabled(replicationEnabled bool) Option {
	return func(mgr *MigrationManager) {
		mgr.replicationEnabled = replicationEnabled
	}
}

func WithConn(conn clickhouse.Conn) Option {
	return func(mgr *MigrationManager) {
		mgr.conn = conn
	}
}

func WithConnOptions(opts clickhouse.Options) Option {
	return func(mgr *MigrationManager) {
		mgr.connOpts = opts
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(mgr *MigrationManager) {
		mgr.logger = logger
	}
}

func WithBackoff(backoff *backoff.ExponentialBackOff) Option {
	return func(mgr *MigrationManager) {
		mgr.backoff = backoff
	}
}

// Returns true if legacy migrations table does not exist
func (m *MigrationManager) ShouldRunSquashedV2(ctx context.Context, db string) (bool, error) {
	var count uint64
	err := m.conn.QueryRow(ctx, "SELECT count(*) FROM clusterAllReplicas($1, system.tables) WHERE database = $2 AND name = $3", m.clusterName, db, legacyMigrationsTable).Scan(&count)
	if err != nil {
		return false, err
	}

	return count == 0, nil
}

// HostAddrs returns the addresses of the all hosts in the cluster.
func (m *MigrationManager) HostAddrs() ([]string, error) {
	if m.development {
		return nil, nil
	}
	m.addrsMux.Lock()
	defer m.addrsMux.Unlock()
	if len(m.addrs) != 0 {
		return m.addrs, nil
	}

	hostAddrs := make(map[string]struct{})
	query := "SELECT DISTINCT host_address, port FROM system.clusters WHERE host_address NOT IN ['localhost', '127.0.0.1', '::1'] AND cluster = $1"
	rows, err := m.conn.Query(context.Background(), query, m.clusterName)
	if err != nil {
		return nil, errors.Join(ErrFailedToGetHostAddrs, err)
	}
	defer rows.Close()
	for rows.Next() {
		var hostAddr string
		var port uint16
		if err := rows.Scan(&hostAddr, &port); err != nil {
			return nil, errors.Join(ErrFailedToGetHostAddrs, err)
		}

		addr, err := netip.ParseAddr(hostAddr)
		if err != nil {
			return nil, errors.Join(ErrFailedToGetHostAddrs, err)
		}

		addrPort := netip.AddrPortFrom(addr, port)
		hostAddrs[addrPort.String()] = struct{}{}
	}

	if len(hostAddrs) != 0 {
		// connect to other host and do the same thing
		for hostAddr := range hostAddrs {
			m.logger.Info("Connecting to new host", zap.String("host", hostAddr))
			opts := m.connOpts
			opts.Addr = []string{hostAddr}
			conn, err := clickhouse.Open(&opts)
			if err != nil {
				return nil, errors.Join(ErrFailedToGetConn, err)
			}
			rows, err := conn.Query(context.Background(), query, m.clusterName)
			if err != nil {
				return nil, errors.Join(ErrFailedToGetConn, err)
			}
			defer rows.Close()
			for rows.Next() {
				var hostAddr string
				var port uint16
				if err := rows.Scan(&hostAddr, &port); err != nil {
					return nil, errors.Join(ErrFailedToGetHostAddrs, err)
				}

				addr, err := netip.ParseAddr(hostAddr)
				if err != nil {
					return nil, errors.Join(ErrFailedToGetHostAddrs, err)
				}

				addrPort := netip.AddrPortFrom(addr, port)
				hostAddrs[addrPort.String()] = struct{}{}
			}
			break
		}
	}

	addrs := make([]string, 0, len(hostAddrs))
	for addr := range hostAddrs {
		addrs = append(addrs, addr)
	}
	m.addrs = addrs
	return addrs, nil
}

func (m *MigrationManager) getConn(hostAddr string) (clickhouse.Conn, error) {
	m.addrsMux.Lock()
	defer m.addrsMux.Unlock()
	if conn, ok := m.conns[hostAddr]; ok {
		return conn, nil
	}
	opts := m.connOpts
	opts.Addr = []string{hostAddr}
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		return nil, err
	}
	m.conns[hostAddr] = conn
	return conn, nil
}

func (m *MigrationManager) waitForMutationsOnHost(ctx context.Context, hostAddr string) error {
	// reset backoff
	m.backoff.Reset()

	m.logger.Info("Fetching mutations on host", zap.String("host", hostAddr))
	conn, err := m.getConn(hostAddr)
	if err != nil {
		return err
	}
	for {
		if m.backoff.NextBackOff() == backoff.Stop {
			return errors.New("backoff stopped")
		}
		var mutations []Mutation
		if err := conn.Select(ctx, &mutations, "SELECT database, table, command, mutation_id, latest_fail_reason FROM system.mutations WHERE is_done = 0"); err != nil {
			return err
		}
		if len(mutations) != 0 {
			m.logger.Info("Waiting for mutations to be completed", zap.Int("count", len(mutations)), zap.String("host", hostAddr))
			for _, mutation := range mutations {
				m.logger.Info("Mutation details",
					zap.String("database", mutation.Database),
					zap.String("table", mutation.Table),
					zap.String("command", mutation.Command),
					zap.String("mutation_id", mutation.MutationID),
					zap.String("latest_fail_reason", mutation.LatestFailReason),
				)
			}
			time.Sleep(m.backoff.NextBackOff())
			continue
		}
		m.logger.Info("No mutations found on host", zap.String("host", hostAddr))
		break
	}
	return nil
}

// WaitForRunningMutations waits for all the mutations to be completed on all the hosts in the cluster.
func (m *MigrationManager) WaitForRunningMutations(ctx context.Context) error {
	addrs, err := m.HostAddrs()
	if err != nil {
		return err
	}
	for _, hostAddr := range addrs {
		m.logger.Info("Waiting for mutations on host", zap.String("host", hostAddr))
		if err := m.waitForMutationsOnHost(ctx, hostAddr); err != nil {
			return errors.Join(ErrFailedToWaitForMutations, err)
		}
	}
	return nil
}

// WaitDistributedDDLQueue waits for all the DDLs to be completed on all the hosts in the cluster.
func (m *MigrationManager) WaitDistributedDDLQueue(ctx context.Context) error {
	// reset backoff
	m.backoff.Reset()
	m.logger.Info("Fetching non-finished DDLs from distributed DDL queue")
	for {
		if m.backoff.NextBackOff() == backoff.Stop {
			return errors.New("backoff stopped")
		}

		ddlQueue, err := m.getDistributedDDLQueue(ctx)
		if err != nil {
			return err
		}

		if len(ddlQueue) != 0 {
			m.logger.Info("Waiting for distributed DDL queue to be completed", zap.Int("count", len(ddlQueue)))
			for _, ddl := range ddlQueue {
				m.logger.Info("DDL details",
					zap.String("query", ddl.Query),
					zap.String("status", ddl.Status),
					zap.String("host", ddl.Host),
					zap.String("exception_code", ddl.ExceptionCode),
				)
			}
			time.Sleep(m.backoff.NextBackOff())
			continue
		}
		m.logger.Info("No pending DDLs found in distributed DDL queue")
		break
	}
	return nil
}

func (m *MigrationManager) getDistributedDDLQueue(ctx context.Context) ([]DistributedDDLQueue, error) {
	var ddlQueue []DistributedDDLQueue
	query := "SELECT entry, cluster, query, host, port, status, exception_code FROM system.distributed_ddl_queue WHERE status != 'Finished'"

	// 10 attempts is an arbitrary number. If we don't get the DDL queue after 10 attempts, we give up.
	for i := 0; i < 10; i++ {
		if err := m.conn.Select(ctx, &ddlQueue, query); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				if exception.Code == 999 {
					// ClickHouse DDLWorker is cleaning up entries in the distributed_ddl_queue before we can query it. This leads to the exception:
					// code: 999, message: Coordination error: No node, path /clickhouse/signoz-clickhouse/task_queue/ddl/query-000000<some 4 digit number>/finished

					// It looks like this exception is safe to retry on.
					if strings.Contains(exception.Error(), "No node") {
						m.logger.Error("A retryable exception was received while fetching distributed DDL queue", zap.Error(err), zap.Int("attempt", i+1))
						continue
					}
				}
			}

			m.logger.Error("Failed to fetch distributed DDL queue", zap.Error(err), zap.Int("attempt", i+1))
			return nil, err
		}

		// If no exception was thrown, break the loop
		break
	}

	return ddlQueue, nil
}

func (m *MigrationManager) waitForDistributionQueueOnHost(ctx context.Context, conn clickhouse.Conn, db, table string) error {
	errCountQuery := "SELECT count(*) FROM system.distribution_queue WHERE database = $1 AND table = $2 AND (error_count != 0 OR is_blocked = 1)"

	var errCount uint64
	if err := conn.QueryRow(ctx, errCountQuery, db, table).Scan(&errCount); err != nil {
		return errors.Join(ErrFailedToWaitForDistributionQueue, err)
	}

	if errCount != 0 {
		return ErrDistributionQueueError
	}

	query := "SELECT count(*) FROM system.distribution_queue WHERE database = $1 AND table = $2 AND data_files > 0"
	// Should this be configurable and/or higher?
	t := time.NewTimer(2 * time.Minute)
	defer t.Stop()
	minimumInsertsCompletedChan := make(chan struct{})
	errChan := make(chan error)

	// count for the number of inserts in the queue with non-zero data_files
	go func() {
		insertsInQueue := 0
		for {
			var errCount uint64
			if err := conn.QueryRow(ctx, errCountQuery, db, table).Scan(&errCount); err != nil {
				errChan <- errors.Join(ErrFailedToWaitForDistributionQueue, err)
				return
			}
			if errCount != 0 {
				errChan <- ErrDistributionQueueError
				return
			}

			var count uint64
			// if the count of inserts in the queue with non-zero data_files is greater than 0, then it counts towards
			// one insert, while technically it is more than one insert, we are mainly interested in number of such actions
			if err := conn.QueryRow(ctx, query, db, table).Scan(&count); err != nil {
				m.logger.Error("Failed to fetch inserts in queue, will retry", zap.Error(err))
				continue
			}
			if count > 0 {
				insertsInQueue++
			}
			if insertsInQueue >= 16 {
				minimumInsertsCompletedChan <- struct{}{}
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// we waited for graceful period to complete, it might happen that no inserts occur after the migration
			// so we can't wait forever
			return nil
		case err := <-errChan:
			return err
		case <-minimumInsertsCompletedChan:
			return nil
		}
	}
}

// When dropping a column, we need to make sure that there are no pending items in `distribution_queue`
// for the table. This is because if we drop a column on local table, but there is a pending insert on remote
// table, it will fail.
// There is no deterministic way to find out if there are pending items in `distribution_queue` for a table with
// old schema, so we try to wait for 2 minutes or at least 16 inserts with non-zero `data_files` in the queue
// for the table.
// We need to do this for all hosts in the cluster.
func (m *MigrationManager) WaitForDistributionQueue(ctx context.Context, db, table string) error {
	addrs, err := m.HostAddrs()
	if err != nil {
		return errors.Join(ErrFailedToWaitForDistributionQueue, err)
	}
	for _, hostAddr := range addrs {
		conn, err := m.getConn(hostAddr)
		if err != nil {
			return errors.Join(ErrFailedToWaitForDistributionQueue, err)
		}
		if err := m.waitForDistributionQueueOnHost(ctx, conn, db, table); err != nil {
			return errors.Join(ErrFailedToWaitForDistributionQueue, err)
		}
	}
	return nil
}

func (m *MigrationManager) shouldRunMigration(db string, migrationID uint64, versions []uint64) bool {
	m.logger.Info("Checking if migration should run", zap.String("db", db), zap.Uint64("migration_id", migrationID), zap.Any("versions", versions))
	// if versions are provided, we only run the migrations that are in the versions slice
	if len(versions) != 0 {
		var doesExist bool
		for _, version := range versions {
			if migrationID == version {
				doesExist = true
				break
			}
		}
		if !doesExist {
			m.logger.Info("Migration should not run as it is not in the provided versions", zap.Uint64("migration_id", migrationID), zap.Any("versions", versions))
			return false
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s.schema_migrations_v2 WHERE migration_id = %d SETTINGS final = 1;", db, migrationID)
	m.logger.Info("Fetching migration status", zap.String("query", query))
	var migrationSchemaMigrationRecord MigrationSchemaMigrationRecord
	if err := m.conn.QueryRow(context.Background(), query).ScanStruct(&migrationSchemaMigrationRecord); err != nil {
		if err == sql.ErrNoRows {
			m.logger.Info("Migration not run", zap.Uint64("migration_id", migrationID))
			return true
		}
		// this should not happen
		m.logger.Error("Failed to fetch migration status", zap.Error(err))
		panic(err)
	}
	m.logger.Info("Migration status", zap.Uint64("migration_id", migrationID), zap.String("status", migrationSchemaMigrationRecord.Status))
	if migrationSchemaMigrationRecord.Status != InProgressStatus && migrationSchemaMigrationRecord.Status != FinishedStatus {
		m.logger.Info("Migration not run", zap.Uint64("migration_id", migrationID), zap.String("status", migrationSchemaMigrationRecord.Status))
		return true
	}
	return false
}

func (m *MigrationManager) IsSync(migration SchemaMigrationRecord) bool {
	for _, item := range migration.UpItems {
		// if any of the operations is a sync operation, return true
		if ok := m.IsSyncOperation(item); ok {
			return true
		}
	}

	return false
}

func (m *MigrationManager) IsSyncOperation(item Operation) bool {
	return item.ForceMigrate() || (!item.IsMutation() && item.IsIdempotent() && item.IsLightweight())
}

func (m *MigrationManager) IsAsync(migration SchemaMigrationRecord) bool {
	for _, item := range migration.UpItems {
		// if any of the operations is an async operation, return true
		if ok := m.IsAsyncOperation(item); ok {
			return true
		}
	}

	return false
}

func (m *MigrationManager) IsAsyncOperation(item Operation) bool {
	// If it is a force migrate operation, return false
	if item.ForceMigrate() {
		return false
	}

	// If it is a sync operation, return false
	if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
		return false
	}

	return true
}

func (m *MigrationManager) insertMigrationEntry(ctx context.Context, db string, migrationID uint64, status string) error {
	query := fmt.Sprintf("INSERT INTO %s.distributed_schema_migrations_v2 (migration_id, status, created_at) VALUES (%d, '%s', '%s')", db, migrationID, status, time.Now().UTC().Format("2006-01-02 15:04:05"))
	m.logger.Info("Inserting migration entry", zap.String("query", query))
	return m.conn.Exec(ctx, query)
}

func (m *MigrationManager) updateMigrationEntry(ctx context.Context, db string, migrationID uint64, status string, err string) error {
	query := fmt.Sprintf("ALTER TABLE %s.schema_migrations_v2 ON CLUSTER %s UPDATE status = $1, error = $2, updated_at = $3 WHERE migration_id = $4", db, m.clusterName)
	m.logger.Info("Updating migration entry", zap.String("query", query), zap.String("status", status), zap.String("error", err), zap.Uint64("migration_id", migrationID))
	return m.conn.Exec(ctx, query, status, err, time.Now().UTC().Format("2006-01-02 15:04:05"), migrationID)
}

func (m *MigrationManager) RunOperation(ctx context.Context, operation Operation, migrationID uint64, database string, skipStatusUpdate bool) error {
	m.logger.Info("Running operation", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Bool("skip_status_update", skipStatusUpdate))
	start := time.Now()
	var sql string
	if m.clusterName != "" {
		operation = operation.OnCluster(m.clusterName)
	}
	if m.replicationEnabled {
		operation = operation.WithReplication()
	}

	m.logger.Info("Waiting for running mutations before running the operation")

	if err := m.WaitForRunningMutations(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}

	if shouldWaitForDistributionQueue, database, table := operation.ShouldWaitForDistributionQueue(); shouldWaitForDistributionQueue {
		m.logger.Info("Waiting for distribution queue", zap.String("database", database), zap.String("table", table))
		if err := m.WaitForDistributionQueue(ctx, database, table); err != nil {
			updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
			if updateErr != nil {
				return errors.Join(err, updateErr)
			}
			return err
		}
	}

	if !skipStatusUpdate {
		insertErr := m.insertMigrationEntry(ctx, database, migrationID, InProgressStatus)
		if insertErr != nil {
			return insertErr
		}
	}

	sql = operation.ToSQL()
	m.logger.Info("Running operation", zap.String("sql", sql))
	err := m.conn.Exec(ctx, sql)
	if err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}

	m.logger.Info("Waiting for running mutations after running the operation")

	if err := m.WaitForRunningMutations(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if !skipStatusUpdate {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FinishedStatus, "")
		if updateErr != nil {
			return updateErr
		}
	}
	duration := time.Since(start)
	m.logger.Info("Operation completed", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Duration("duration", duration))

	return nil
}

func (m *MigrationManager) RunOperationWithoutUpdate(ctx context.Context, operation Operation, migrationID uint64, database string) error {
	m.logger.Info("Running operation", zap.Uint64("migration_id", migrationID), zap.String("database", database))
	start := time.Now()

	var sql string
	if m.clusterName != "" {
		operation = operation.OnCluster(m.clusterName)
	}

	if m.replicationEnabled {
		operation = operation.WithReplication()
	}

	m.logger.Info("Waiting for running mutations before running the operation")
	if err := m.WaitForRunningMutations(ctx); err != nil {
		return err
	}

	m.logger.Info("Waiting for distributed DDL queue before running the operation")
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		return err
	}

	if shouldWaitForDistributionQueue, database, table := operation.ShouldWaitForDistributionQueue(); shouldWaitForDistributionQueue {
		m.logger.Info("Waiting for distribution queue", zap.String("database", database), zap.String("table", table))
		if err := m.WaitForDistributionQueue(ctx, database, table); err != nil {
			return err
		}
	}

	sql = operation.ToSQL()
	m.logger.Info("Running operation", zap.String("sql", sql))
	err := m.conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	duration := time.Since(start)
	m.logger.Info("Operation completed", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Duration("duration", duration))

	return nil
}

func (m *MigrationManager) InsertMigrationEntry(ctx context.Context, db string, migrationID uint64, status string) error {
	query := fmt.Sprintf("INSERT INTO %s.distributed_schema_migrations_v2 (migration_id, status, created_at) VALUES (%d, '%s', '%s')", db, migrationID, status, time.Now().UTC().Format("2006-01-02 15:04:05"))
	m.logger.Info("Inserting migration entry", zap.String("query", query))
	return m.conn.Exec(ctx, query)
}

func (m *MigrationManager) CheckMigrationStatus(ctx context.Context, db string, migrationID uint64, status string) (bool, error) {
	query := fmt.Sprintf("SELECT * FROM %s.distributed_schema_migrations_v2 WHERE migration_id = %d SETTINGS final = 1;", db, migrationID)
	m.logger.Info("Checking migration status", zap.String("query", query))

	var migrationSchemaMigrationRecord MigrationSchemaMigrationRecord
	if err := m.conn.QueryRow(ctx, query).ScanStruct(&migrationSchemaMigrationRecord); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, err
	}

	return migrationSchemaMigrationRecord.Status == status, nil
}

func (m *MigrationManager) Close() error {
	return m.conn.Close()
}
