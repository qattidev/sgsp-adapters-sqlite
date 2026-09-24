// Package postgres provides the durable AssignmentStore adapter. Applications
// own driver setup and invoke ApplyMigrations explicitly; New never mutates a
// database.
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"qattidev/sgsp"
	"qattidev/sgsp-sqlite/internal/dbgen"
	"qattidev/sgsp/placement"
)

type Store struct{ db *sql.DB }

var _ placement.AssignmentStore = (*Store)(nil)

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, sgsp.ErrInvalidArgument
	}
	return &Store{db: db}, nil
}
func validGroup(group placement.GroupID) bool {
	return group.App.ID != "" && group.App.Version != "" && group.Key != ""
}
func validOwner(owner sgsp.Owner) bool {
	return owner.ID != "" && owner.Endpoint.Address != "" && owner.Endpoint.ServerName != ""
}

// Assign acquires the SQLite write lock by inserting before reading the winner.
// Configure a busy timeout on every connection to wait for competing writers.
func (s *Store) Assign(ctx context.Context, group placement.GroupID, candidate sgsp.Owner) (placement.Assignment, error) {
	if s == nil || !validGroup(group) || !validOwner(candidate) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return placement.Assignment{}, err
	}
	defer tx.Rollback()
	q := dbgen.New(tx)
	err = q.InsertAssignment(ctx, dbgen.InsertAssignmentParams{AppID: group.App.ID, GroupKey: group.Key, AppVersion: group.App.Version, OwnerID: candidate.ID, Incarnation: candidate.Incarnation[:], EndpointAddress: candidate.Endpoint.Address, EndpointServerName: candidate.Endpoint.ServerName})
	if err != nil {
		return placement.Assignment{}, err
	}
	assignment, err := read(ctx, q, group)
	if err != nil {
		return placement.Assignment{}, err
	}
	if assignment.Group.App.Version != group.App.Version {
		return placement.Assignment{}, sgsp.ErrUnsupportedVersion
	}
	if assignment.Closed {
		return placement.Assignment{}, sgsp.ErrGroupClosed
	}
	if err := tx.Commit(); err != nil {
		return placement.Assignment{}, err
	}
	return assignment, nil
}
func (s *Store) Get(ctx context.Context, group placement.GroupID) (placement.Assignment, error) {
	if s == nil || !validGroup(group) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	assignment, err := read(ctx, dbgen.New(s.db), group)
	if err != nil {
		return placement.Assignment{}, err
	}
	if assignment.Group.App.Version != group.App.Version {
		return placement.Assignment{}, sgsp.ErrUnsupportedVersion
	}
	return assignment, nil
}
func (s *Store) Close(ctx context.Context, group placement.GroupID, expected sgsp.Owner) error {
	if s == nil || !validGroup(group) || !validOwner(expected) {
		return sgsp.ErrInvalidArgument
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := dbgen.New(tx).AcquireWriteLock(ctx, dbgen.AcquireWriteLockParams{AppID: group.App.ID, GroupKey: group.Key}); err != nil {
		return err
	}
	assignment, err := readLocked(ctx, dbgen.New(tx), group)
	if err != nil {
		return err
	}
	if assignment.Group.App.Version != group.App.Version {
		return sgsp.ErrUnsupportedVersion
	}
	if assignment.Owner.ID != expected.ID || assignment.Owner.Incarnation != expected.Incarnation {
		return sgsp.ErrForbidden
	}
	if !assignment.Closed {
		if err := dbgen.New(tx).CloseAssignment(ctx, dbgen.CloseAssignmentParams{AppID: group.App.ID, GroupKey: group.Key}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func read(ctx context.Context, q *dbgen.Queries, group placement.GroupID) (placement.Assignment, error) {
	row, err := q.GetAssignment(ctx, dbgen.GetAssignmentParams{AppID: group.App.ID, GroupKey: group.Key})
	return assignment(group, row.AppVersion, row.OwnerID, row.Incarnation, row.EndpointAddress, row.EndpointServerName, row.Closed, err)
}
func readLocked(ctx context.Context, q *dbgen.Queries, group placement.GroupID) (placement.Assignment, error) {
	row, err := q.LockAssignment(ctx, dbgen.LockAssignmentParams{AppID: group.App.ID, GroupKey: group.Key})
	return assignment(group, row.AppVersion, row.OwnerID, row.Incarnation, row.EndpointAddress, row.EndpointServerName, row.Closed, err)
}
func assignment(group placement.GroupID, version, ownerID string, incarnation []byte, address, serverName string, closed bool, err error) (placement.Assignment, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return placement.Assignment{}, placement.ErrAssignmentNotFound
	}
	if err != nil {
		return placement.Assignment{}, err
	}
	if len(incarnation) != len(sgsp.Incarnation{}) {
		return placement.Assignment{}, sgsp.ErrProtocolViolation
	}
	var id sgsp.Incarnation
	copy(id[:], incarnation)
	return placement.Assignment{Group: placement.GroupID{App: sgsp.AppIdentity{ID: group.App.ID, Version: version}, Key: group.Key}, Owner: sgsp.Owner{ID: ownerID, Incarnation: id, Endpoint: sgsp.Endpoint{Address: address, ServerName: serverName}}, Closed: closed}, nil
}
