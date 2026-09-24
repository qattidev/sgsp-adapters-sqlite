-- name: InsertAssignment :exec
INSERT INTO sgsp_group_assignments (app_id, group_key, app_version, owner_id, incarnation, endpoint_address, endpoint_server_name)
VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT (app_id, group_key) DO NOTHING;

-- name: GetAssignment :one
SELECT app_version, owner_id, incarnation, endpoint_address, endpoint_server_name, closed FROM sgsp_group_assignments WHERE app_id=?1 AND group_key=?2;

-- name: LockAssignment :one
SELECT app_version, owner_id, incarnation, endpoint_address, endpoint_server_name, closed FROM sgsp_group_assignments WHERE app_id=?1 AND group_key=?2;

-- name: CloseAssignment :exec
UPDATE sgsp_group_assignments SET closed=true, closed_at=CURRENT_TIMESTAMP WHERE app_id=?1 AND group_key=?2;

-- name: AcquireWriteLock :exec
UPDATE sgsp_group_assignments SET closed=closed WHERE app_id=?1 AND group_key=?2;
